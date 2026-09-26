import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class HoldFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db"); self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self): self.tmp.cleanup()

    def request(self, mb=10000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def hold_body(self, entries, station="GS1", antenna="ANT1", window=None):
        window = window or self.window
        return {"station_id": station, "antenna_id": antenna, "expires_at": iso(self.now + timedelta(hours=3)),
                "items": [{"request_id": req["id"], "window_id": window["id"], "starts_at": iso(start), "ends_at": iso(end), "rate_mbps": rate} for req, start, end, rate in entries]}

    def create_hold(self, req, start=None, end=None, rate=50):
        start = start or self.now; end = end or self.now + timedelta(minutes=30)
        return self.svc.create_hold_batch("lead", "operator", self.hold_body([(req, start, end, rate)]))["holds"][0]

    def active_holds(self):
        return self.svc.list_holds("lead", "operator", "", {"status": ["active"]})["holds"]

    def test_create_query_cancel_hold(self):
        req = self.request(); hold = self.create_hold(req)
        self.assertEqual(hold["status"], "active")
        self.assertEqual(self.svc.get_hold(hold["id"], "lead", "operator", "")["batch_id"], hold["batch_id"])
        self.assertEqual(len(self.active_holds()), 1)
        self.assertEqual(len(self.svc.state("lead", "operator", "")["holds"]), 1)
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold_batch("requester-t1", "requester", self.hold_body([(self.request(), self.now, self.now + timedelta(minutes=30), 50)]))
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.cancel_hold(hold["id"], "req-t2", "requester", "T2", {"reason": "越权取消"})
        self.assertEqual(ctx.exception.code, "tenant_forbidden")
        canceled = self.svc.cancel_hold(hold["id"], "lead", "operator", "", {"reason": "不再需要"})
        self.assertEqual(canceled["status"], "canceled")
        req2 = self.request()
        schedule = self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        self.assertEqual(schedule["status"], "scheduled")

    def test_hold_blocks_scheduling_and_held_request(self):
        req = self.request(); self.create_hold(req)
        other = self.request()
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(other["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.code, "antenna_hold_conflict")
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.code, "request_held")

    def test_batch_conflict_rolls_back_whole_batch(self):
        blocker = self.request()
        self.svc.schedule_request(blocker["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        good, bad = self.request(), self.request()
        body = self.hold_body([(good, self.now + timedelta(minutes=30), self.now + timedelta(minutes=60), 50),
                               (bad, self.now + timedelta(minutes=10), self.now + timedelta(minutes=40), 50)])
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold_batch("lead", "operator", body)
        self.assertEqual(ctx.exception.code, "hold_batch_conflict")
        self.assertIn("antenna_conflict", [c["code"] for c in ctx.exception.details["conflicts"]])
        self.assertEqual(self.active_holds(), [])

    def test_batch_internal_overlap_rejected(self):
        req1, req2 = self.request(), self.request()
        body = self.hold_body([(req1, self.now, self.now + timedelta(minutes=40), 50),
                               (req2, self.now + timedelta(minutes=20), self.now + timedelta(minutes=60), 50)])
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold_batch("lead", "operator", body)
        self.assertIn("batch_item_overlap", [c["code"] for c in ctx.exception.details["conflicts"]])
        self.assertEqual(self.active_holds(), [])

    def test_expired_hold_releases_slot(self):
        req = self.request(); hold = self.create_hold(req)
        self.svc.repo.conn.execute("UPDATE holds SET expires_at=? WHERE id=?", (iso(utcnow() - timedelta(seconds=1)), hold["id"]))
        holds = self.svc.list_holds("lead", "operator", "", {})["holds"]
        self.assertEqual(holds[0]["status"], "expired")
        other = self.request()
        schedule = self.svc.schedule_request(other["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        self.assertEqual(schedule["status"], "scheduled")
        with self.assertRaises(ApiError) as ctx:
            self.svc.convert_hold(hold["id"], "lead", "operator")
        self.assertEqual(ctx.exception.code, "hold_not_active")

    def test_convert_hold_success(self):
        req = self.request(); hold = self.create_hold(req)
        result = self.svc.convert_hold(hold["id"], "lead", "operator")
        self.assertEqual(result["hold"]["status"], "converted")
        self.assertEqual(result["hold"]["converted_schedule_id"], result["schedule"]["id"])
        self.assertEqual(result["schedule"]["status"], "scheduled")
        self.assertEqual(self.svc.state("lead", "operator", "")["requests"][0]["status"], "scheduled")

    def test_convert_revalidates_and_keeps_hold_on_failure(self):
        req = self.request()
        hold = self.create_hold(req, self.now + timedelta(minutes=30), self.now + timedelta(minutes=60))
        self.svc.create_maintenance("op", "operator", {"station_id": "GS1", "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=45)), "ends_at": iso(self.now + timedelta(minutes=75)), "reason": "例行巡检"})
        with self.assertRaises(ApiError) as ctx:
            self.svc.convert_hold(hold["id"], "lead", "operator")
        self.assertEqual(ctx.exception.code, "hold_conversion_conflict")
        self.assertIn("maintenance_conflict", [c["code"] for c in ctx.exception.details["conflicts"]])
        self.assertEqual(self.svc.get_hold(hold["id"], "lead", "operator", "")["status"], "active")

    def test_holds_count_toward_tenant_quota(self):
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 3600})
        self.create_hold(self.request(), self.now, self.now + timedelta(minutes=30))
        self.create_hold(self.request(), self.now + timedelta(minutes=30), self.now + timedelta(minutes=60))
        with self.assertRaises(ApiError) as ctx:
            self.create_hold(self.request(), self.now + timedelta(minutes=60), self.now + timedelta(minutes=90))
        self.assertIn("tenant_quota_exceeded", [c["code"] for c in ctx.exception.details["conflicts"]])

    def test_same_satellite_hold_blocks_other_station(self):
        self.svc.create_station("op", "operator", {"id": "GS2", "name": "西安站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT2", "station_id": "GS2", "max_rate_mbps": 80})
        window2 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS2", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.create_hold(self.request(), self.now, self.now + timedelta(minutes=30))
        req2 = self.request()
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": window2["id"], "antenna_id": "ANT2", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.code, "satellite_hold_conflict")
        body = self.hold_body([(req2, self.now + timedelta(minutes=10), self.now + timedelta(minutes=40), 50)], station="GS2", antenna="ANT2", window=window2)
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold_batch("lead", "operator", body)
        self.assertIn("satellite_hold_conflict", [c["code"] for c in ctx.exception.details["conflicts"]])


if __name__ == "__main__": unittest.main()
