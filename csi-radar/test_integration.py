from collections import Counter
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import shutil
import socket
import subprocess
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import urlopen, Request
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from server import Engine, SourceManager, make_handler, udp_worker


class Integration(unittest.TestCase):
    def test_http_calibration_export_and_udp(self):
        engine=Engine('udp')
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(engine))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        base='http://127.0.0.1:'+str(server.server_address[1])
        # Reserve a local UDP port to choose an unused port for this test.
        probe=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        probe.bind(('127.0.0.1',0)); port=probe.getsockname()[1]; probe.close()
        threading.Thread(target=udp_worker,args=(engine,'127.0.0.1',port),daemon=True).start()
        try:
            with urlopen(base+'/',timeout=3) as response:
                self.assertIn(b'CSI Motion Observatory',response.read())
            with urlopen(Request(base+'/api/calibrate',method='POST',headers={'X-CSI-Action':'calibrate'}),timeout=3) as response:
                self.assertEqual(response.status,200)
            with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sock:
                for n in range(10):
                    sock.sendto(('CSI,'+str(n+1)+',-50,52,'+','.join(['4000']*52)).encode(),('127.0.0.1',port))
                    time.sleep(.02)
            with urlopen(base+'/api/state',timeout=3) as response:
                result=json.load(response)
                self.assertGreater(result['count'],0)
                self.assertEqual(result['mode'],'udp')
                self.assertEqual(result['source'],'127.0.0.1')
            with urlopen(base+'/api/export',timeout=3) as response:
                self.assertIn(b'amp51',response.read())
        finally:
            server.shutdown();server.server_close()

    def test_http_tcp_connect_and_mode_switch(self):
        engine = Engine('idle')
        manager = SourceManager(engine)
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(engine, manager))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = 'http://127.0.0.1:' + str(server.server_address[1])

        esp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        esp_sock.bind(('127.0.0.1', 0))
        esp_sock.listen(1)
        esp_port = esp_sock.getsockname()[1]

        def mock_esp():
            conn, _ = esp_sock.accept()
            with conn:
                conn.sendall(b'# Boot comment should be ignored\n')
                for n in range(1, 6):
                    frame = f'CSI,{n*50000},-45,52,' + ','.join(['4000'] * 52) + '\n'
                    conn.sendall(frame.encode())
                    time.sleep(0.02)
                time.sleep(0.3)

        threading.Thread(target=mock_esp, daemon=True).start()
        try:
            req = Request(
                base + '/api/connect',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'connect'},
                data=json.dumps({'mode': 'tcp', 'host': '127.0.0.1', 'port': esp_port}).encode(),
            )
            with urlopen(req, timeout=3) as response:
                self.assertEqual(response.status, 200)

            deadline = time.monotonic() + 3
            result = {}
            while time.monotonic() < deadline:
                with urlopen(base + '/api/state', timeout=3) as response:
                    result = json.load(response)
                if result['count'] >= 5:
                    break
                time.sleep(0.05)
            self.assertEqual(result['mode'], 'tcp')
            self.assertEqual(result['target'], f'127.0.0.1:{esp_port}')
            self.assertTrue(result['link_up'])
            self.assertGreaterEqual(result['count'], 5)
            self.assertEqual(result['invalid'], 0)

            req = Request(
                base + '/api/connect',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'connect'},
                data=json.dumps({'mode': 'disconnect'}).encode(),
            )
            with urlopen(req, timeout=3) as response:
                self.assertEqual(response.status, 200)
            with urlopen(base + '/api/state', timeout=3) as response:
                disconnected = json.load(response)
                self.assertEqual(disconnected['mode'], 'idle')
                self.assertFalse(disconnected['link_up'])

            auto_req = Request(
                base + '/api/autoconnect',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'autoconnect'},
                data=json.dumps({'port': esp_port}).encode(),
            )
            with patch('server.scan_lan_esp32', return_value=[{'host': '127.0.0.1', 'port': esp_port, 'label': 'mock'}]):
                self.assertFalse(manager.maybe_recover(esp_port))
                with urlopen(auto_req, timeout=3) as response:
                    auto_body = json.load(response)
                    self.assertTrue(auto_body['connected'])
                    self.assertEqual(auto_body['target'], f'127.0.0.1:{esp_port}')
                self.assertFalse(manager.user_disconnected)
                self.assertTrue(manager.auto_managed)
        finally:
            manager.stop()
            esp_sock.close()
            server.shutdown()
            server.server_close()

    def test_http_fall_alert_and_cloud_webhook(self):
        received = []

        class HookHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:
                pass

            def do_POST(self):
                length = int(self.headers.get('Content-Length', '0'))
                received.append(json.loads(self.rfile.read(length).decode('utf-8')))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'ok')

        hook_srv = HTTPServer(('127.0.0.1', 0), HookHandler)
        threading.Thread(target=hook_srv.serve_forever, daemon=True).start()
        hook_url = f'http://127.0.0.1:{hook_srv.server_port}/hook'

        engine = Engine('tcp')
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(engine))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f'http://127.0.0.1:{server.server_port}'
        try:
            cfg_req = Request(
                base + '/api/alert/config',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'alert-config'},
                data=json.dumps({'webhook_url': hook_url, 'auto_cloud': True}).encode(),
            )
            with urlopen(cfg_req, timeout=3) as resp:
                self.assertEqual(resp.status, 200)

            test_req = Request(
                base + '/api/alert/test',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'alert-test'},
                data=json.dumps({'send_cloud': True}).encode(),
            )
            with urlopen(test_req, timeout=3) as resp:
                self.assertTrue(json.load(resp)['cloud_dispatched'])

            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and not received:
                time.sleep(0.05)
            self.assertEqual(len(received), 1)
            self.assertEqual(received[0]['event'], 'FALL_DETECTED')

            with urlopen(base + '/api/state', timeout=3) as resp:
                state = json.load(resp)
            self.assertTrue(state['fall_alert'])
            self.assertTrue(state['alert_logs'][0]['status'].startswith('OK (200)'))

            ack_req = Request(
                base + '/api/alert/ack',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'alert-ack'},
                data=b'{}',
            )
            with urlopen(ack_req, timeout=3) as resp:
                self.assertEqual(resp.status, 200)
            with urlopen(base + '/api/state', timeout=3) as resp:
                self.assertFalse(json.load(resp)['fall_alert'])

            reset_req = Request(
                base + '/api/calibrate/reset',
                method='POST',
                headers={'Content-Type': 'application/json', 'X-CSI-Action': 'calibrate-reset'},
                data=b'{}',
            )
            with urlopen(reset_req, timeout=3) as resp:
                self.assertEqual(resp.status, 200)
            self.assertEqual(engine.events[0]['event'], 'CALIBRATION RESET')
        finally:
            server.shutdown()
            server.server_close()
            hook_srv.shutdown()
            hook_srv.server_close()

    def test_dashboard_html_ids_and_bindings(self):
        html = (Path(__file__).resolve().parent / 'web' / 'index.html').read_text(encoding='utf-8')
        ids = []

        class IdCollector(HTMLParser):
            def handle_starttag(self, tag, attrs):
                for key, val in attrs:
                    if key == 'id' and val:
                        ids.append(val)

        IdCollector().feed(html)
        duplicates = {dom_id: count for dom_id, count in Counter(ids).items() if count > 1}
        self.assertEqual(duplicates, {})

        script_match = re.search(r'<script>(.*?)</script>', html, re.DOTALL)
        self.assertIsNotNone(script_match)
        script = script_match.group(1) if script_match else ''
        referenced = set(re.findall(r"\bel\('([^']+)'\)", script))
        self.assertTrue(referenced)
        missing = sorted(referenced - set(ids))
        self.assertEqual(missing, [])
        node = shutil.which('node')
        if node:
            res = subprocess.run([node, '--check'], input=script, text=True, capture_output=True)
            self.assertEqual(res.returncode, 0, res.stderr)

if __name__=='__main__':unittest.main()
