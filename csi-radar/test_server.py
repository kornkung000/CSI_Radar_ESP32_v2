import math
import unittest
from unittest.mock import patch
from server import Engine, parse_frame

def frame(t=1, value=4000, count=52):
    return f'CSI,{t},-50,{count},'+','.join([str(value)]*count)

class Tests(unittest.TestCase):
    def test_parser_rejects_corrupt_and_nonfinite(self):
        for bad in [frame()+',2', frame().replace('4000','nan'), frame().replace('4000','-1'), 'CSI,1,-50,52,1', 'LOG hello']:
            with self.assertRaises(ValueError): parse_frame(bad)
        self.assertEqual(len(parse_frame(frame())[2]),52)

    def test_calibration_motion_and_stale(self):
        e=Engine('serial')
        with patch('server.time.monotonic',return_value=100): e.calibrate()
        for i in range(201):
            with patch('server.time.monotonic', return_value=100+i*.05): e.ingest(frame(i+1), 'COM3')
        self.assertTrue(e.calibrated)
        with patch('server.time.monotonic',return_value=110.1):
            e.ingest(frame(203,8000),'COM3')
            self.assertEqual(e.snapshot()['status'],'MOTION')
        with patch('server.time.monotonic',return_value=114):
            self.assertEqual(e.snapshot()['status'],'DISCONNECTED')
            self.assertEqual(e.snapshot()['activity'],'UNKNOWN')
            self.assertEqual(e.snapshot()['amplitudes'],[])

    def test_sparse_calibration_fails(self):
        e=Engine('serial')
        with patch('server.time.monotonic', return_value=100): e.calibrate(); e.ingest(frame(), 'COM3')
        with patch('server.time.monotonic', return_value=111): e.ingest(frame(2), 'COM3')
        self.assertFalse(e.calibrated)
        self.assertIn('failed',e.error)

    def test_reset_does_not_count_reboot_as_motion(self):
        e=Engine('serial');e.calibrated=True
        e.ingest(frame(100,4000),'COM3')
        e.ingest(frame(1,8000),'COM3')
        self.assertEqual(e.energy,0)
        self.assertFalse(e.motion)

    def test_vitals_estimation_and_fall_ack(self):
        e=Engine('tcp'); e.calibrated=True; e.threshold=0.03
        for i in range(165):
            t = 200.0 + i * 0.05
            val = int(4000 + 18 * math.sin(2 * math.pi * 0.30 * (i * 0.05)) + 14 * math.sin(2 * math.pi * 1.20 * (i * 0.05)))
            with patch('server.time.monotonic', return_value=t):
                e.ingest(frame(i + 1, val), '192.168.1.50:9000')
        with patch('server.time.monotonic', return_value=208.3):
            snap = e.snapshot()
        self.assertEqual(snap['vitals_status'], 'ACTIVE')
        self.assertIsNotNone(snap['hr_bpm'])
        self.assertTrue(66 <= snap['hr_bpm'] <= 78, snap['hr_bpm'])
        self.assertIsNotNone(snap['rr_bpm'])
        self.assertTrue(15 <= snap['rr_bpm'] <= 21, snap['rr_bpm'])
        self.assertEqual(snap['people_label'], '0')
        with patch('server.time.monotonic', return_value=208.4):
            for step in range(4):
                e.ingest(frame(170 + step, 12000 if step % 2 == 0 else 4000), '192.168.1.50:9000')
            alert_snap = e.snapshot()
        self.assertTrue(alert_snap['fall_alert'])
        self.assertEqual(alert_snap['posture'], 'FALL')
        e.acknowledge_fall()
        with patch('server.time.monotonic', return_value=208.5):
            self.assertFalse(e.snapshot()['fall_alert'])
        e.reset_calibration()
        with patch('server.time.monotonic', return_value=208.6):
            reset_snap = e.snapshot()
        self.assertFalse(e.calibrated)
        self.assertEqual(reset_snap['status'], 'UNCALIBRATED')
        self.assertEqual(reset_snap['threshold'], 0.06)

if __name__=='__main__': unittest.main()
