"""Local CSI dashboard. Serial/UDP/TCP ingestion never blocks HTTP rendering."""
import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import csv
import importlib
import io
import ipaddress
import json
import math
import os
from pathlib import Path
import random
import socket
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parent


def _validate_webhook_url(raw):
    url = (raw or '').strip()
    if not url:
        return ''
    if len(url) > 512:
        raise ValueError('Webhook URL ยาวเกินไป (สูงสุด 512 ตัวอักษร)')
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        raise ValueError('Webhook URL ต้องขึ้นต้นด้วย http:// หรือ https://')
    return url


def _webhook_preview(url):
    if not url:
        return ''
    parsed = urllib.parse.urlparse(url)
    return f'{parsed.scheme}://{parsed.netloc}/...'


def _resample_series(samples, count=128):
    """Resample irregular (t, value) pairs onto an evenly spaced grid."""
    t_end = samples[-1][0]
    t_start = max(samples[0][0], t_end - 10.0)
    window = [item for item in samples if item[0] >= t_start]
    if len(window) < 48:
        return [], 0.0, 0.0
    span = window[-1][0] - window[0][0]
    if span < 6.0:
        return [], 0.0, 0.0
    max_gap = max(window[i + 1][0] - window[i][0] for i in range(len(window) - 1))
    if max_gap > 1.5:
        return [], 0.0, 0.0
    sample_hz = (len(window) - 1) / span
    grid = []
    idx = 0
    last_idx = len(window) - 1
    step = span / (count - 1)
    for k in range(count):
        tk = window[0][0] + k * step
        while idx + 1 < last_idx and window[idx + 1][0] < tk:
            idx += 1
        t0, y0 = window[idx]
        t1, y1 = window[min(idx + 1, last_idx)]
        if t1 <= t0:
            grid.append(y0)
        else:
            grid.append(y0 + ((tk - t0) / (t1 - t0)) * (y1 - y0))
    return grid, sample_hz, step


def _spectral_profile(tapered, dt, f_min, f_max, step_hz=0.025):
    """Evaluate DFT power spectrum across [f_min, f_max]."""
    freqs = []
    powers = []
    freq = f_min
    two_pi_dt = 2.0 * math.pi * dt
    while freq <= f_max + 1e-9:
        w = two_pi_dt * freq
        re = 0.0
        im = 0.0
        for i, val in enumerate(tapered):
            ang = w * i
            re += val * math.cos(ang)
            im -= val * math.sin(ang)
        freqs.append(freq)
        powers.append(re * re + im * im)
        freq += step_hz
    return freqs, powers


def _select_band_bpm(freqs, powers, f_min, f_max, noise_floor, min_snr=4.0):
    """Pick peak frequency inside [f_min, f_max] if above global spectral floor."""
    indices = [i for i, f in enumerate(freqs) if f_min <= f <= f_max]
    if not indices:
        return None
    peak_idx = max(indices, key=lambda i: powers[i])
    peak = powers[peak_idx]
    if peak <= 0 or (noise_floor > 0 and peak / noise_floor < min_snr):
        return None
    return round(freqs[peak_idx] * 60.0)


def _compute_vitals(samples):
    """Estimate respiration and heart-rate periodicities from quiet CSI amplitude means."""
    grid, sample_hz, dt = _resample_series(samples)
    if not grid:
        return None, None, 0.0, 'COLLECTING'
    mean_val = sum(grid) / len(grid)
    n_minus_1 = len(grid) - 1
    tapered = [
        (val - mean_val) * (0.5 * (1.0 - math.cos(2.0 * math.pi * i / n_minus_1)))
        for i, val in enumerate(grid)
    ]
    max_freq = min(2.20, sample_hz * 0.45)
    freqs, powers = _spectral_profile(tapered, dt, 0.15, max_freq, 0.025)
    if len(powers) < 5:
        return None, None, round(sample_hz, 1), 'WEAK_SIGNAL'
    noise_floor = statistics.median(powers)
    rr_bpm = _select_band_bpm(freqs, powers, 0.15, 0.55, noise_floor) if sample_hz >= 4.0 else None
    if sample_hz < 8.0:
        return None, rr_bpm, round(sample_hz, 1), 'LOW_RATE'
    hr_bpm = _select_band_bpm(freqs, powers, 0.85, 2.15, noise_floor)
    status = 'ACTIVE' if (hr_bpm is not None or rr_bpm is not None) else 'WEAK_SIGNAL'
    return hr_bpm, rr_bpm, round(sample_hz, 1), status


def _dispatch_cloud_alert(engine, url, payload):
    """Deliver a fall-alert payload to a user-configured HTTP/HTTPS webhook."""
    parsed = urllib.parse.urlparse(url)
    target = parsed.netloc
    try:
        if parsed.netloc.lower() == 'ntfy.sh' and len(parsed.path.strip('/')) > 0:
            data = str(payload['message']).encode('utf-8')
            headers = {'Title': 'CSI Fall Alert', 'Priority': 'high', 'Content-Type': 'text/plain; charset=utf-8'}
        else:
            body = dict(payload)
            body.setdefault('content', payload['message'])
            body.setdefault('text', payload['message'])
            data = json.dumps(body, ensure_ascii=False).encode('utf-8')
            headers = {'Content-Type': 'application/json; charset=utf-8'}
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=5) as resp:
            status_text = f'OK ({resp.status})'
    except Exception as exc:
        status_text = f'FAILED ({exc})'
    engine.record_alert_log(payload['event'], target, status_text)


def parse_frame(line):
    fields = line.strip().split(',')
    if len(fields) < 5 or fields[0] != 'CSI':
        raise ValueError('Not a CSI frame')
    timestamp, rssi, count = map(int, fields[1:4])
    if timestamp < 0 or not -127 <= rssi <= 0 or not 10 <= count <= 114 or len(fields) != count + 4:
        raise ValueError('Invalid CSI header/length')
    amps = [float(x) for x in fields[4:]]
    if any(not math.isfinite(x) or x < 0 or x > 20000 for x in amps):
        raise ValueError('Invalid amplitudes')
    return timestamp, rssi, amps


class Engine:
    def __init__(self, mode, target=''):
        self.lock = threading.RLock()
        self.mode = mode
        self.target = target
        self.frames = deque(maxlen=30)
        self.history = deque(maxlen=300)
        self.records = deque(maxlen=18000)
        self.events = deque(maxlen=30)
        self.last = 0.0
        self.count = self.invalid = 0
        self.arrivals = deque(maxlen=1000)
        self.energy = 0.0
        self.threshold = 0.06
        self.baseline = []
        self.cal_until = 0.0
        self.calibrated = False
        self.motion = False
        self.error = ''
        self.rssi = None
        self.amps = []
        self.predictor = None
        self.activity = 'DISABLED'
        self.model_error = ''
        self.last_predict = 0.0
        self.probs = {}
        self.source = ''
        self.prev_timestamp = None
        self.link_up = False
        self.vital_samples = deque(maxlen=256)
        self.last_vitals = 0.0
        self.hr_bpm = None
        self.rr_bpm = None
        self.sample_hz = 0.0
        self.vitals_status = 'COLLECTING'
        self.fall_alert = False
        self.fall_reason = ''
        self.fall_at = ''
        self.last_fall_at = -100.0
        self.fall_threshold = 0.40
        self.fall_auto = True
        self.webhook_url = ''
        self.auto_cloud = True
        self.alert_logs = deque(maxlen=20)

    def _clear_analysis_state(self):
        self.frames.clear()
        self.energy = 0.0
        self.motion = False
        self.activity = 'WAITING' if self.predictor else 'DISABLED'
        self.probs = {}
        self.vital_samples.clear()
        self.last_vitals = 0.0
        self.hr_bpm = None
        self.rr_bpm = None
        self.sample_hz = 0.0
        self.vitals_status = 'COLLECTING'

    def reset_mode(self, mode, target=''):
        with self.lock:
            self.mode = mode
            self.target = target
            self.history.clear()
            self.records.clear()
            self.events.clear()
            self.last = 0.0
            self.count = self.invalid = 0
            self.arrivals.clear()
            self.threshold = 0.06
            self.baseline.clear()
            self.cal_until = 0.0
            self.calibrated = False
            self.error = ''
            self.rssi = None
            self.amps = []
            self.source = ''
            self.prev_timestamp = None
            self.link_up = False
            self.fall_alert = False
            self.fall_reason = ''
            self.fall_at = ''
            self._clear_analysis_state()

    def set_link_state(self, up, error=None):
        with self.lock:
            self.link_up = bool(up)
            if error is not None:
                self.error = error

    def configure_alerts(self, webhook_url=None, auto_cloud=None, fall_threshold=None, fall_auto=None):
        with self.lock:
            if webhook_url is not None:
                self.webhook_url = _validate_webhook_url(webhook_url)
            if auto_cloud is not None:
                self.auto_cloud = bool(auto_cloud)
            if fall_auto is not None:
                self.fall_auto = bool(fall_auto)
            if fall_threshold is not None:
                val = float(fall_threshold)
                if not math.isfinite(val) or not 0.1 <= val <= 5.0:
                    raise ValueError('Fall threshold ต้องอยู่ระหว่าง 0.10 – 5.00')
                self.fall_threshold = val

    def record_alert_log(self, event, target, status_text):
        with self.lock:
            self.alert_logs.appendleft({'time': time.strftime('%H:%M:%S'), 'event': event, 'target': target, 'status': status_text})

    def _build_fall_payload_locked(self, reason):
        return {
            'event': 'FALL_DETECTED',
            'message': f'[CSI Smart Care] แจ้งเตือนเหตุสงสัยการล้ม: {reason} (source={self.source or self.mode}, energy={self.energy:.4f}, rssi={self.rssi})',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'reason': reason,
            'source': self.source or self.mode,
            'energy': round(self.energy, 4),
            'rssi': self.rssi,
            'hr_bpm': self.hr_bpm,
            'rr_bpm': self.rr_bpm,
        }

    def trigger_fall(self, reason, force_cloud=False):
        dispatch = None
        with self.lock:
            now = time.monotonic()
            self.fall_alert = True
            self.fall_reason = reason
            self.fall_at = time.strftime('%H:%M:%S')
            self.last_fall_at = now
            self.events.appendleft({'time': self.fall_at, 'event': f'FALL ALERT ({reason})'})
            if self.webhook_url and (force_cloud or self.auto_cloud):
                dispatch = (self.webhook_url, self._build_fall_payload_locked(reason))
        if dispatch is not None:
            threading.Thread(target=_dispatch_cloud_alert, args=(self, dispatch[0], dispatch[1]), daemon=True).start()
            return True
        return False

    def acknowledge_fall(self):
        with self.lock:
            if self.fall_alert:
                self.events.appendleft({'time': time.strftime('%H:%M:%S'), 'event': 'FALL ACK'})
            self.fall_alert = False
            self.fall_reason = ''
            self.fall_at = ''

    def calibrate(self):
        with self.lock:
            self.baseline.clear()
            self.cal_until = time.monotonic() + 10
            self.calibrated = False
            self.motion = False
            self.frames.clear()
            self.energy = 0.0

    def reset_calibration(self):
        with self.lock:
            self.baseline.clear()
            self.cal_until = 0.0
            self.calibrated = False
            self.motion = False
            self.threshold = 0.06
            self.frames.clear()
            self.energy = 0.0
            self.events.appendleft({'time': time.strftime('%H:%M:%S'), 'event': 'CALIBRATION RESET'})

    def ingest(self, line, source):
        try:
            ts, rssi, amps = parse_frame(line)
        except (ValueError, OverflowError):
            with self.lock:
                self.invalid += 1
            return False
        now = time.monotonic()
        cloud_dispatch = None
        first_frame = False
        with self.lock:
            reset = (now-self.last > 2 or source != self.source or
                     (self.prev_timestamp is not None and ts <= self.prev_timestamp) or
                     (self.amps and len(amps) != len(self.amps)))
            if reset:
                self._clear_analysis_state()
            first_frame = self.count == 0
            self.source = source
            self.prev_timestamp = ts
            self.last, self.rssi, self.amps = now, rssi, amps
            self.count += 1
            self.arrivals.append(now)
            self.error = ''
            if self.frames:
                previous = self.frames[-1]
                # Scale-independent mean frame-to-frame change; heuristic, not probability.
                raw = sum(abs(a-b) for a,b in zip(amps, previous)) / max(sum(previous), 1)
                self.energy = .2 * raw + .8 * self.energy
                if self.cal_until:
                    self.baseline.append(self.energy)
            self.frames.append(amps)
            if self.cal_until and now >= self.cal_until:
                self.cal_until = 0.0
                if len(self.baseline) >= 20:
                    self.threshold = max(.015, statistics.mean(self.baseline) + 4*statistics.pstdev(self.baseline))
                    self.calibrated = True
                else:
                    self.error = 'Calibration failed: fewer than 20 samples. Generate Wi-Fi traffic and retry.'
            old = self.motion
            self.motion = self.calibrated and self.energy > self.threshold * (.65 if self.motion else 1)
            if self.motion != old:
                self.events.appendleft({'time': time.strftime('%H:%M:%S'), 'event': 'MOTION' if self.motion else 'QUIET'})
            if self.predictor and len(self.frames) == 30 and now - self.last_predict > .25:
                self.last_predict = now
                try:
                    self.activity, self.probs = self.predictor(list(self.frames))
                except Exception as exc:
                    self.activity, self.probs = 'MODEL ERROR', {}
                    self.model_error = str(exc)
            if self.motion:
                self.vital_samples.clear()
                self.hr_bpm = None
                self.rr_bpm = None
                self.vitals_status = 'MOTION_INTERFERENCE'
            else:
                self.vital_samples.append((now, sum(amps) / len(amps)))
                if now - self.last_vitals >= 0.5:
                    self.last_vitals = now
                    self.hr_bpm, self.rr_bpm, self.sample_hz, self.vitals_status = _compute_vitals(self.vital_samples)
            fall_hit_reason = ''
            if self.activity == 'FALL':
                fall_hit_reason = 'ML Model ตรวจจับท่าล้ม'
            elif (self.fall_auto and self.calibrated and not self.fall_alert and
                  now - self.last_fall_at >= 15.0 and
                  self.energy >= max(self.fall_threshold, self.threshold * 4.0)):
                fall_hit_reason = f'แรงเปลี่ยนแปลงสัญญาณฉับพลัน ({self.energy:.3f})'
            if fall_hit_reason and not self.fall_alert and now - self.last_fall_at >= 15.0:
                self.fall_alert = True
                self.fall_reason = fall_hit_reason
                self.fall_at = time.strftime('%H:%M:%S')
                self.last_fall_at = now
                self.events.appendleft({'time': self.fall_at, 'event': f'FALL ALERT ({fall_hit_reason})'})
                if self.webhook_url and self.auto_cloud:
                    cloud_dispatch = (self.webhook_url, self._build_fall_payload_locked(fall_hit_reason))
            point = {'time': time.time(), 'energy': round(self.energy, 5), 'rssi': rssi}
            self.history.append(point)
            self.records.append((time.time(), ts, rssi, self.energy, self.threshold, self.motion, source, *amps))
        if first_frame:
            print(f'[CSI] streaming <- {source} (rssi={rssi}, subcarriers={len(amps)})', flush=True)
        if cloud_dispatch is not None:
            threading.Thread(target=_dispatch_cloud_alert, args=(self, cloud_dispatch[0], cloud_dispatch[1]), daemon=True).start()
        return True

    def _derive_posture_locked(self, stale):
        if stale:
            return 'OFFLINE', 'none', 'UNKNOWN', '—'
        if self.fall_alert:
            src = 'ml_model' if self.activity == 'FALL' else 'heuristic'
            return 'FALL', src, 'PRESENT', '1+'
        if self.cal_until:
            return 'CALIBRATING', 'none', 'UNKNOWN', '—'
        if self.predictor and self.activity not in ('DISABLED', 'WAITING', 'UNKNOWN', 'MODEL ERROR'):
            occ = 'EMPTY' if self.activity == 'EMPTY' else 'PRESENT'
            ppl = '0' if occ == 'EMPTY' else '1+'
            return self.activity, 'ml_model', occ, ppl
        if not self.calibrated:
            return 'UNCALIBRATED', 'none', ('PRESENT' if self.energy > self.threshold else 'UNKNOWN'), '—'
        if self.motion:
            pose = 'WALK' if self.energy > self.threshold * 2.0 else 'MOVING'
            return pose, 'heuristic', 'PRESENT', '1+'
        return 'QUIET', 'heuristic', 'EMPTY', '0'

    def snapshot(self):
        with self.lock:
            now = time.monotonic()
            stale = not self.last or now-self.last > 2
            status = 'DISCONNECTED' if stale else ('CALIBRATING' if self.cal_until else ('UNCALIBRATED' if not self.calibrated else ('MOTION' if self.motion else 'QUIET')))
            posture, posture_source, occupancy, people_label = self._derive_posture_locked(stale)
            return dict(mode=self.mode, target=self.target, status=status, count=self.count, invalid=self.invalid,
                        rate=sum(t > now-1 for t in self.arrivals), rssi=None if stale else self.rssi,
                        energy=0 if stale else self.energy, threshold=self.threshold,
                        age=None if not self.last else round(now-self.last, 2),
                        amplitudes=[] if stale else self.amps, history=list(self.history),
                        events=list(self.events), error=self.error, model_error=self.model_error,
                        activity='UNKNOWN' if stale else self.activity, probabilities={} if stale else self.probs,
                        calibration_left=max(0, round(self.cal_until-now)), source=self.source,
                        link_up=self.link_up, posture=posture, posture_source=posture_source,
                        occupancy=occupancy, people_label=people_label,
                        hr_bpm=None if stale else self.hr_bpm, rr_bpm=None if stale else self.rr_bpm,
                        sample_hz=0.0 if stale else self.sample_hz,
                        vitals_status='OFFLINE' if stale else self.vitals_status,
                        fall_alert=self.fall_alert, fall_reason=self.fall_reason, fall_at=self.fall_at,
                        fall_threshold=self.fall_threshold, fall_auto=self.fall_auto,
                        auto_cloud=self.auto_cloud, webhook_configured=bool(self.webhook_url),
                        webhook_url_preview=_webhook_preview(self.webhook_url),
                        alert_logs=list(self.alert_logs))


def load_predictor(model_path):
    # Only an explicitly selected, trusted local model is loaded.
    import sys
    import warnings
    import numpy as np
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    joblib = importlib.import_module('joblib')
    extract_features = importlib.import_module('ml.features').extract_features
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        model = joblib.load(model_path)
    if any('version' in str(w.message).lower() for w in caught):
        raise ValueError('Model/library version mismatch. Use the training environment or retrain; model disabled.')
    names = {0:'EMPTY', 1:'WALK', 2:'STAND', 3:'SIT', 4:'WAVE'}
    def predict(frames):
        if any(len(x) != 52 for x in frames):
            raise ValueError('Legacy model requires exactly 52 amplitudes per frame')
        x = extract_features(frames).reshape(1, -1)
        if x.shape[1] != model.n_features_in_:
            raise ValueError('Feature shape differs from trained model')
        p = model.predict_proba(x)[0]
        labels = [names.get(c, str(c)) for c in model.classes_]
        return labels[int(np.argmax(p))], dict(zip(labels, map(float, p)))
    return predict


def _wait_stop(stop_event, seconds):
    if stop_event is None:
        time.sleep(seconds)
        return False
    return stop_event.wait(seconds)


def serial_worker(engine, port, baud, stop_event=None):
    serial = importlib.import_module('serial')
    while not (stop_event and stop_event.is_set()):
        try:
            with serial.Serial() as connection:
                connection.port, connection.baudrate, connection.timeout = port, baud, .2
                connection.dtr = connection.rts = False
                connection.open()
                pending = b''
                while not (stop_event and stop_event.is_set()):
                    pending += connection.read(min(max(connection.in_waiting, 1), 8192))
                    while b'\n' in pending:
                        line, pending = pending.split(b'\n', 1)
                        if line.startswith(b'CSI,'):
                            engine.ingest(line.decode('ascii', errors='replace'), port)
                    if len(pending) > 8192:
                        pending = b''
        except Exception as exc:
            if stop_event and stop_event.is_set():
                break
            with engine.lock:
                engine.error = str(exc)
            if _wait_stop(stop_event, 2):
                break


def udp_worker(engine, address, port, stop_event=None):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((address, port))
        sock.settimeout(1)
        selected = None
        while not (stop_event and stop_event.is_set()):
            try:
                payload, sender = sock.recvfrom(4096)
            except socket.timeout:
                continue
            # Single-node baseline: never mix different transmitters into one window.
            if selected is not None and sender[0] != selected:
                continue
            if engine.ingest(payload.decode('ascii', errors='replace'), sender[0]):
                selected = sender[0]


def tcp_worker(engine, host, port, stop_event=None, sock_holder=None):
    target = f'{host}:{port}'
    was_connected = False
    while not (stop_event and stop_event.is_set()):
        sock = None
        try:
            sock = socket.create_connection((host, port), timeout=3)
            sock.settimeout(0.25)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except OSError:
                pass
            engine.set_link_state(True, '')
            if not was_connected:
                print(f'[TCP] connected -> {target}', flush=True)
                was_connected = True
            if sock_holder is not None:
                sock_holder['sock'] = sock
            pending = b''
            last_probe = 0.0
            while not (stop_event and stop_event.is_set()):
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    now = time.monotonic()
                    with engine.lock:
                        last_csi = engine.last
                    if now - last_csi > 0.35 and now - last_probe >= 0.12:
                        try:
                            sock.sendall(b'\n')
                            last_probe = now
                        except OSError as exc:
                            raise ConnectionError(f'keepalive write failed: {exc}') from exc
                    continue
                if not chunk:
                    raise ConnectionError(f'{target} closed connection')
                pending += chunk
                while b'\n' in pending:
                    line, pending = pending.split(b'\n', 1)
                    line = line.rstrip(b'\r')
                    if line.startswith(b'CSI,'):
                        engine.ingest(line.decode('ascii', errors='replace'), target)
                if len(pending) > 8192:
                    pending = b''
        except Exception as exc:
            if stop_event and stop_event.is_set():
                break
            if was_connected:
                print(f'[TCP] disconnected <- {target}: {exc}', flush=True)
                was_connected = False
            engine.set_link_state(False, f'เชื่อมต่อ {target} ไม่สำเร็จ: {exc}')
        finally:
            engine.set_link_state(False)
            if sock_holder is not None and sock_holder.get('sock') is sock:
                sock_holder['sock'] = None
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        if _wait_stop(stop_event, 2):
            break


_PRIVATE_NETS = (
    ipaddress.IPv4Network('10.0.0.0/8'),
    ipaddress.IPv4Network('172.16.0.0/12'),
    ipaddress.IPv4Network('192.168.0.0/16'),
)


def _local_private_ipv4s():
    found = []
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(('8.8.8.8', 80))
            found.append(probe.getsockname()[0])
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            found.append(info[4][0])
    except OSError:
        pass
    result = []
    seen = set()
    for raw in found:
        if raw in seen:
            continue
        seen.add(raw)
        try:
            addr = ipaddress.IPv4Address(raw)
        except ValueError:
            continue
        if any(addr in net for net in _PRIVATE_NETS):
            result.append(addr)
    return result


def _probe_tcp(ip, port):
    try:
        with socket.create_connection((ip, port), timeout=0.35):
            return ip
    except OSError:
        return None


def scan_lan_esp32(port=9000, active_target=''):
    results = []
    seen_ips = set()
    active_ip = ''
    if active_target:
        active_host = active_target.split(':', 1)[0]
        try:
            active_ip = socket.gethostbyname(active_host)
            results.append({'host': active_host, 'port': port, 'label': f'{active_host} (กำลังเชื่อมต่อ)'})
            seen_ips.add(active_ip)
        except OSError:
            pass
    try:
        mdns_ip = socket.gethostbyname('csi-radar.local')
        if mdns_ip not in seen_ips and _probe_tcp(mdns_ip, port):
            results.append({'host': mdns_ip, 'port': port, 'label': f'csi-radar.local ({mdns_ip})'})
            seen_ips.add(mdns_ip)
    except OSError:
        pass
    local_ips = _local_private_ipv4s()
    self_set = {str(ip) for ip in local_ips} | seen_ips
    candidates = []
    seen_nets = set()
    for addr in local_ips[:3]:
        net = ipaddress.IPv4Network(f'{addr}/24', strict=False)
        if net in seen_nets:
            continue
        seen_nets.add(net)
        for host in net.hosts():
            ip_str = str(host)
            if ip_str not in self_set:
                candidates.append(ip_str)
    if candidates:
        with ThreadPoolExecutor(max_workers=64) as pool:
            for ip in pool.map(lambda target_ip: _probe_tcp(target_ip, port), candidates):
                if ip and ip not in seen_ips:
                    seen_ips.add(ip)
                    results.append({'host': ip, 'port': port, 'label': ip})
    return results


class SourceManager:
    def __init__(self, engine):
        self.engine = engine
        self._lock = threading.Lock()
        self._stop = None
        self._thread = None
        self._sock_holder = {}
        self.user_disconnected = False
        self.auto_managed = False
        self._last_link_ok = time.monotonic()

    def stop(self):
        with self._lock:
            if self._stop is not None:
                self._stop.set()
            sock = self._sock_holder.get('sock')
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            thread = self._thread
            self._stop = None
            self._thread = None
            self._sock_holder = {}
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)

    def _launch(self, mode, target, runner):
        self.stop()
        self.engine.reset_mode(mode, target)
        stop_event = threading.Event()
        sock_holder = {}
        def guarded():
            try:
                runner(stop_event, sock_holder)
            except Exception as exc:
                if not stop_event.is_set():
                    with self.engine.lock:
                        self.engine.error = f'Receiver stopped: {exc}'
        thread = threading.Thread(target=guarded, daemon=True)
        with self._lock:
            self._stop = stop_event
            self._thread = thread
            self._sock_holder = sock_holder
        thread.start()

    def start_serial(self, port, baud):
        self.user_disconnected = False
        self.auto_managed = False
        self._launch('serial', port, lambda ev, _sh: serial_worker(self.engine, port, baud, ev))

    def start_udp(self, bind, port):
        self.user_disconnected = False
        self.auto_managed = False
        self._launch('udp', f'{bind}:{port}', lambda ev, _sh: udp_worker(self.engine, bind, port, ev))

    def start_tcp(self, host, port, auto=False):
        self.user_disconnected = False
        self.auto_managed = bool(auto)
        self._last_link_ok = time.monotonic()
        self._launch('tcp', f'{host}:{port}', lambda ev, sh: tcp_worker(self.engine, host, port, ev, sh))

    def disconnect(self):
        self.user_disconnected = True
        self.auto_managed = False
        self.stop()
        self.engine.reset_mode('idle', '')

    def auto_connect(self, port=9000, only_if_idle=False):
        snap = self.engine.snapshot()
        if only_if_idle and (snap['mode'] != 'idle' or self.user_disconnected):
            return {'connected': False, 'target': snap['target'], 'hosts': []}
        if snap['mode'] == 'tcp' and snap['link_up']:
            active_host = snap['target'].split(':', 1)[0]
            return {'connected': True, 'target': snap['target'], 'hosts': [{'host': active_host, 'port': port, 'label': f"{snap['target']} (เชื่อมต่ออยู่แล้ว)"}]}
        hosts = scan_lan_esp32(port, '')
        if not hosts:
            return {'connected': False, 'target': '', 'hosts': []}
        if only_if_idle and (self.engine.snapshot()['mode'] != 'idle' or self.user_disconnected):
            return {'connected': False, 'target': self.engine.snapshot()['target'], 'hosts': hosts}
        chosen = hosts[0]
        self.start_tcp(chosen['host'], chosen['port'], auto=True)
        print(f"[AutoConnect] connecting -> {chosen['host']}:{chosen['port']}", flush=True)
        return {'connected': True, 'target': f"{chosen['host']}:{chosen['port']}", 'hosts': hosts}

    def maybe_recover(self, port=9000):
        if self.user_disconnected:
            return False
        snap = self.engine.snapshot()
        if snap['mode'] == 'idle':
            return bool(self.auto_connect(port, only_if_idle=True)['connected'])
        if snap['mode'] == 'tcp':
            if snap['link_up']:
                self._last_link_ok = time.monotonic()
                return False
            if self.auto_managed and time.monotonic() - self._last_link_ok >= 15.0:
                self._last_link_ok = time.monotonic()
                return bool(self.auto_connect(port, only_if_idle=False)['connected'])
        return False


def _read_json_body(handler):
    try:
        length = int(handler.headers.get('Content-Length', '0'))
    except ValueError:
        return None
    if length < 0 or length > 4096:
        return None
    raw = handler.rfile.read(length) if length else b'{}'
    try:
        data = json.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def make_handler(engine, manager=None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:
            pass

        def reply(self, status, content, mime):
            self.send_response(status)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(content)

        def reply_json(self, status, payload):
            self.reply(status, json.dumps(payload, allow_nan=False).encode(), 'application/json')

        def do_GET(self):
            if self.path == '/api/state':
                self.reply_json(200, engine.snapshot())
            elif self.path == '/api/export':
                buffer = io.StringIO(newline='')
                writer = csv.writer(buffer)
                with engine.lock:
                    rows = list(engine.records)
                width = max((len(row)-7 for row in rows), default=52)
                writer.writerow(['host_time','device_us','rssi','motion_energy','threshold','motion','source']+[f'amp{i}' for i in range(width)])
                writer.writerows(rows)
                self.reply(200, buffer.getvalue().encode(), 'text/csv; charset=utf-8')
            elif self.path in ('/', '/index.html'):
                self.reply(200, (ROOT/'web'/'index.html').read_bytes(), 'text/html; charset=utf-8')
            else:
                self.reply(404, b'Not found', 'text/plain')

        def do_POST(self):
            if self.path == '/api/calibrate':
                if self.headers.get('X-CSI-Action') != 'calibrate':
                    return self.reply(403, b'Forbidden', 'text/plain')
                engine.calibrate()
                return self.reply(200, b'{"ok":true}', 'application/json')
            if self.path == '/api/calibrate/reset':
                if self.headers.get('X-CSI-Action') != 'calibrate-reset':
                    return self.reply(403, b'Forbidden', 'text/plain')
                engine.reset_calibration()
                return self.reply(200, b'{"ok":true}', 'application/json')
            if self.path == '/api/connect':
                if self.headers.get('X-CSI-Action') != 'connect':
                    return self.reply(403, b'Forbidden', 'text/plain')
                if manager is None:
                    return self.reply_json(503, {'ok': False, 'error': 'Connection switching unavailable'})
                body = _read_json_body(self)
                if body is None:
                    return self.reply_json(400, {'ok': False, 'error': 'Invalid JSON payload'})
                mode = body.get('mode', 'tcp')
                if mode == 'disconnect':
                    manager.disconnect()
                    return self.reply_json(200, {'ok': True, 'mode': 'idle'})
                if mode != 'tcp':
                    return self.reply_json(400, {'ok': False, 'error': 'Unsupported mode'})
                host = str(body.get('host', '')).strip()
                if not host or len(host) > 253 or any(ch.isspace() for ch in host):
                    return self.reply_json(400, {'ok': False, 'error': 'กรุณากรอก IP หรือ Hostname ของ ESP32 ให้ถูกต้อง'})
                try:
                    port = int(body.get('port', 9000))
                except (TypeError, ValueError):
                    return self.reply_json(400, {'ok': False, 'error': 'พอร์ตไม่ถูกต้อง'})
                if not 1 <= port <= 65535:
                    return self.reply_json(400, {'ok': False, 'error': 'พอร์ตต้องอยู่ระหว่าง 1–65535'})
                manager.start_tcp(host, port)
                return self.reply_json(200, {'ok': True, 'mode': 'tcp', 'target': f'{host}:{port}'})
            if self.path == '/api/autoconnect':
                if self.headers.get('X-CSI-Action') != 'autoconnect':
                    return self.reply(403, b'Forbidden', 'text/plain')
                if manager is None:
                    return self.reply_json(503, {'ok': False, 'error': 'Connection switching unavailable'})
                body = _read_json_body(self) or {}
                try:
                    port = int(body.get('port', 9000))
                except (TypeError, ValueError):
                    port = 9000
                if not 1 <= port <= 65535:
                    port = 9000
                res = manager.auto_connect(port)
                return self.reply_json(200, {'ok': True, **res})
            if self.path == '/api/scan':
                if self.headers.get('X-CSI-Action') != 'scan':
                    return self.reply(403, b'Forbidden', 'text/plain')
                body = _read_json_body(self) or {}
                try:
                    port = int(body.get('port', 9000))
                except (TypeError, ValueError):
                    port = 9000
                if not 1 <= port <= 65535:
                    port = 9000
                snap = engine.snapshot()
                active = snap['target'] if snap['mode'] == 'tcp' and snap['link_up'] else ''
                hosts = scan_lan_esp32(port, active)
                return self.reply_json(200, {'ok': True, 'hosts': hosts})
            if self.path == '/api/alert/config':
                if self.headers.get('X-CSI-Action') != 'alert-config':
                    return self.reply(403, b'Forbidden', 'text/plain')
                body = _read_json_body(self)
                if body is None:
                    return self.reply_json(400, {'ok': False, 'error': 'Invalid JSON payload'})
                try:
                    engine.configure_alerts(
                        webhook_url=body.get('webhook_url') if 'webhook_url' in body else None,
                        auto_cloud=body.get('auto_cloud') if 'auto_cloud' in body else None,
                        fall_threshold=body.get('fall_threshold') if 'fall_threshold' in body else None,
                        fall_auto=body.get('fall_auto') if 'fall_auto' in body else None,
                    )
                except (TypeError, ValueError) as exc:
                    return self.reply_json(400, {'ok': False, 'error': str(exc)})
                return self.reply_json(200, {'ok': True, 'webhook_configured': bool(engine.webhook_url)})
            if self.path == '/api/alert/test':
                if self.headers.get('X-CSI-Action') != 'alert-test':
                    return self.reply(403, b'Forbidden', 'text/plain')
                body = _read_json_body(self) or {}
                sent_cloud = engine.trigger_fall('ทดสอบแจ้งเตือนล้มจากผู้ใช้', force_cloud=bool(body.get('send_cloud', True)))
                return self.reply_json(200, {'ok': True, 'cloud_dispatched': sent_cloud})
            if self.path == '/api/alert/ack':
                if self.headers.get('X-CSI-Action') != 'alert-ack':
                    return self.reply(403, b'Forbidden', 'text/plain')
                engine.acknowledge_fall()
                return self.reply_json(200, {'ok': True})
            return self.reply(404, b'Not found', 'text/plain')
    return Handler


class DashboardHTTPServer(ThreadingHTTPServer):
    # Windows SO_REUSEADDR lets multiple servers bind the same port silently.
    allow_reuse_address = os.name != 'nt'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['serial', 'udp', 'tcp'], default='tcp')
    parser.add_argument('--port', default='COM3')
    parser.add_argument('--baud', type=int, default=115200)
    parser.add_argument('--host', default='')
    parser.add_argument('--tcp-port', type=int, default=9000)
    parser.add_argument('--http-port', type=int, default=8765)
    parser.add_argument('--udp-port', type=int, default=9000)
    parser.add_argument('--udp-bind', default='0.0.0.0')
    parser.add_argument('--model', type=Path)
    args = parser.parse_args()
    engine = Engine(args.mode if args.mode != 'tcp' or args.host else 'idle')
    if args.model:
        try:
            engine.predictor = load_predictor(args.model)
            engine.activity = 'WAITING'
        except Exception as exc:
            engine.model_error = str(exc)
    manager = SourceManager(engine)
    if args.mode == 'serial':
        manager.start_serial(args.port, args.baud)
    elif args.mode == 'udp':
        manager.start_udp(args.udp_bind, args.udp_port)
    elif args.mode == 'tcp' and args.host:
        manager.start_tcp(args.host, args.tcp_port)
    stop_watchdog = threading.Event()
    if args.mode == 'tcp' and not args.host:
        def _startup_autodiscover():
            manager.auto_connect(args.tcp_port, only_if_idle=True)
            while not stop_watchdog.wait(5):
                manager.maybe_recover(args.tcp_port)
        threading.Thread(target=_startup_autodiscover, daemon=True).start()
    try:
        server = DashboardHTTPServer(('127.0.0.1', args.http_port), make_handler(engine, manager))
    except OSError as exc:
        stop_watchdog.set()
        manager.stop()
        print(f'Cannot start CSI dashboard on 127.0.0.1:{args.http_port}: {exc}', file=sys.stderr)
        sys.exit(1)
    print(f'CSI dashboard [{engine.mode}]: http://127.0.0.1:{args.http_port}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_watchdog.set()
        manager.stop()
        server.server_close()


if __name__ == '__main__':
    main()
