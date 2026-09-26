import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class HoldTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db")
        self.t0 = utcnow().replace(microsecond=0) + timedelta(hours=1)
        # 两个站、两条天线、两个同站窗口
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_satellite("op", "operator", {"id": "SAT2", "name": "遥感二号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_station("op", "operator", {"id": "GS2", "name": "喀什站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "A1", "station_id": "GS1", "max_rate_mbps": 90})
        self.svc.create_antenna("op", "operator", {"id": "A2", "station_id": "GS1", "max_rate_mbps": 90})
        self.svc.create_antenna("op", "operator", {"id": "B1", "station_id": "GS2", "max_rate_mbps": 90})
        self.win1 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(hours=3)), "max_rate_mbps": 80})
        self.win1b = self.svc.create_window("op", "operator", {"satellite_id": "SAT2", "station_id": "GS1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(hours=3)), "max_rate_mbps": 80})
        self.win2 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS2", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(hours=3)), "max_rate_mbps": 80})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 3 * 3600})
        self.expires = iso(self.t0 + timedelta(minutes=30))

    def tearDown(self): self.tmp.cleanup()

    def request(self, sat="SAT1", mb=3000, deadline_hours=24):
        return self.svc.create_request("rq", "requester", "T1",
                                       {"satellite_id": sat, "data_mb": mb, "priority": 7,
                                        "deadline": iso(self.t0 + timedelta(hours=deadline_hours))})

    def item(self, req, win=None, antenna="A1", start_min=0, dur_min=30, rate=60):
        win = win or (self.win1["id"] if req["satellite_id"] == "SAT1" else self.win1b["id"])
        return {"request_id": req["id"], "window_id": win, "antenna_id": antenna,
                "starts_at": iso(self.t0 + timedelta(minutes=start_min)),
                "ends_at": iso(self.t0 + timedelta(minutes=start_min + dur_min)), "rate_mbps": rate}

    def create_hold(self, items, **kw):
        body = {"station_id": kw.get("station", "GS1"), "expires_at": kw.get("expires_at", self.expires), "items": items}
        if "note" in kw: body["note"] = kw["note"]
        return self.svc.create_hold("op", "operator", body)

    # ---------- 基本流程 ----------

    def test_create_query_and_hold_blocks_schedule(self):
        req = self.request()
        hold = self.create_hold([self.item(req)])
        self.assertEqual(hold["batch"]["status"], "active")
        self.assertEqual(hold["items"][0]["status"], "held")
        # 请求变为 held
        got = self.svc.state("operator", "")
        self.assertTrue(any(r["id"] == req["id"] and r["status"] == "held" for r in got["requests"]))
        # 正式排程撞上占位：同一天线
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req["id"], "op2", "operator",
                                      {"window_id": self.win1["id"], "antenna_id": "A1",
                                       "starts_at": iso(self.t0 + timedelta(minutes=5)),
                                       "ends_at": iso(self.t0 + timedelta(minutes=20)), "rate_mbps": 60})
        self.assertEqual(ctx.exception.code, "request_held")
        # 另一请求不能用被占天线时段
        other = self.request()
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(other["id"], "op2", "operator",
                                      {"window_id": self.win1["id"], "antenna_id": "A1",
                                       "starts_at": iso(self.t0 + timedelta(minutes=5)),
                                       "ends_at": iso(self.t0 + timedelta(minutes=20)), "rate_mbps": 60})
        self.assertEqual(ctx.exception.code, "antenna_hold_conflict")
        # 非重叠时段可以排
        ok = self.svc.schedule_request(other["id"], "op2", "operator",
                                       {"window_id": self.win1["id"], "antenna_id": "A1",
                                        "starts_at": iso(self.t0 + timedelta(minutes=40)),
                                        "ends_at": iso(self.t0 + timedelta(minutes=55)), "rate_mbps": 60})
        self.assertEqual(ok["status"], "scheduled")
        # 查询接口
        listed = self.svc.list_holds("operator", "", None, None)
        self.assertEqual(len(listed["holds"]), 1)
        detail = self.svc.get_hold(hold["batch"]["id"], "operator", "")
        self.assertEqual(detail["batch"]["id"], hold["batch"]["id"])

    def test_convert_hold_revalidates_and_creates_schedules(self):
        req1, req2 = self.request(), self.request(sat="SAT2", mb=2000)
        hold = self.create_hold([self.item(req1, start_min=0, dur_min=30),
                                 self.item(req2, win=self.win1b["id"], antenna="A2", start_min=0, dur_min=20)])
        # 转换前插入一条维护，使重新核验失败 -> 占位保留
        self.svc.create_maintenance("op", "operator",
                                    {"station_id": "GS1", "antenna_id": "A1",
                                     "starts_at": iso(self.t0 - timedelta(minutes=5)),
                                     "ends_at": iso(self.t0 + timedelta(minutes=10)), "reason": "临时检修"})
        with self.assertRaises(ApiError) as ctx:
            self.svc.convert_hold(hold["batch"]["id"], "op", "operator", {})
        self.assertEqual(ctx.exception.code, "hold_convert_conflict")
        self.assertEqual(ctx.exception.details[0]["request_id"], req1["id"])
        self.assertTrue(any(r["code"] == "maintenance_conflict" for r in ctx.exception.details[0]["reasons"]))
        still = self.svc.get_hold(hold["batch"]["id"], "operator", "")
        self.assertEqual(still["batch"]["status"], "active")
        # 维护改到不冲突后转换成功
        with self.svc.repo.tx() as conn:
            conn.execute("UPDATE maintenance SET ends_at=? WHERE station_id='GS1' AND antenna_id='A1'",
                         (iso(self.t0 - timedelta(minutes=1)),))
        out = self.svc.convert_hold(hold["batch"]["id"], "op", "operator", {})
        self.assertEqual(out["batch"]["status"], "converted")
        self.assertEqual({s["request_id"] for s in out["schedules"]}, {req1["id"], req2["id"]})
        self.assertTrue(all(s["status"] == "scheduled" for s in out["schedules"]))
        got = self.svc.get_schedule(out["schedules"][0]["id"])
        self.assertEqual(got["status"], "scheduled")
        # 已转换批次不能重复转换
        with self.assertRaises(ApiError) as ctx:
            self.svc.convert_hold(hold["batch"]["id"], "op", "operator", {})
        self.assertEqual(ctx.exception.code, "hold_not_active")

    def test_cancel_hold_releases_requests(self):
        req = self.request()
        hold = self.create_hold([self.item(req)])
        out = self.svc.cancel_hold(hold["batch"]["id"], "op", "operator", "", {"reason": "组长取消"})
        self.assertEqual(out["batch"]["status"], "canceled")
        with self.svc.repo.conn:
            status = self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()[0]
        self.assertEqual(status, "pending")
        with self.assertRaises(ApiError) as ctx:
            self.svc.cancel_hold(hold["batch"]["id"], "op", "operator", "", {"reason": "x"})
        self.assertEqual(ctx.exception.code, "hold_not_active")

    def test_expired_hold_is_released_on_touch(self):
        req = self.request()
        hold = self.create_hold([self.item(req)], expires_at=iso(self.t0 - timedelta(minutes=1)))
        # 创建时过期时间必须在未来 -> 直接建一个远期再改库模拟到期
        with self.svc.repo.tx() as conn:
            conn.execute("UPDATE hold_batches SET expires_at=? WHERE id=?",
                         (iso(utcnow() - timedelta(seconds=1)), hold["batch"]["id"]))
        sweep = self.svc.sweep_holds("op", "operator")
        self.assertIn(hold["batch"]["id"], sweep["released_batch_ids"])
        detail = self.svc.get_hold(hold["batch"]["id"], "operator", "")
        self.assertEqual(detail["batch"]["status"], "released")
        with self.svc.repo.conn:
            status = self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()[0]
        self.assertEqual(status, "pending")
        # 过期批次转换报 hold_not_active（active 过滤后为 released）
        with self.assertRaises(ApiError) as ctx:
            self.svc.convert_hold(hold["batch"]["id"], "op", "operator", {})
        self.assertEqual(ctx.exception.code, "hold_not_active")

    def test_lazy_sweep_on_create_and_list(self):
        req = self.request()
        hold = self.create_hold([self.item(req)])
        with self.svc.repo.tx() as conn:
            conn.execute("UPDATE hold_batches SET expires_at=? WHERE id=?",
                         (iso(utcnow() - timedelta(seconds=1)), hold["batch"]["id"]))
        req2 = self.request()
        # 新占位动作触发惰性清扫，旧批次释放，新批次可占同一时段
        hold2 = self.create_hold([self.item(req2)])
        self.assertEqual(self.svc.get_hold(hold["batch"]["id"], "operator", "")["batch"]["status"], "released")
        self.assertEqual(hold2["batch"]["status"], "active")

    # ---------- 整批原子 + 冲突原因 ----------

    def test_batch_atomic_on_conflict_and_reasons(self):
        good = self.request()
        bad = self.request()
        before = self.svc.state("operator", "")
        items = [self.item(good, antenna="A2", start_min=0, dur_min=20),
                 # 超出可见窗口
                 self.item(bad, start_min=170, dur_min=20)]
        with self.assertRaises(ApiError) as ctx:
            self.create_hold(items)
        self.assertEqual(ctx.exception.code, "hold_batch_conflict")
        reasons = {(x["request_id"]): [r["code"] for r in x["reasons"]] for x in ctx.exception.details}
        self.assertNotIn(good["id"], reasons)
        self.assertIn("outside_visibility", reasons[bad["id"]])
        # 整批未入库：good 请求仍是 pending，也没有任何批次
        with self.svc.repo.conn:
            self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) FROM hold_batches").fetchone()[0], 0)
            self.assertEqual(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (good["id"],)).fetchone()[0], "pending")

    def test_antenna_maintenance_and_quota_conflicts(self):
        self.svc.create_maintenance("op", "operator",
                                    {"station_id": "GS1", "antenna_id": "A1",
                                     "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(hours=1)),
                                     "reason": "保养"})
        req = self.request()
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(req)])
        codes = [r["code"] for r in ctx.exception.details[0]["reasons"]]
        self.assertIn("maintenance_conflict", codes)

        req2 = self.request(mb=100000)
        with self.assertRaises(ApiError) as ctx:
            # 配额 3h：占 3.5 小时必超
            self.create_hold([self.item(req2, antenna="A2", start_min=0, dur_min=210, rate=80)])
        self.assertIn("tenant_quota_exceeded", [r["code"] for r in ctx.exception.details[0]["reasons"]])

    def test_satellite_conflict_at_other_station(self):
        # GS2 上已存在 SAT1 的正式排程，则 GS1 占位 SAT1 同时段应被同星规则拦截
        req_at_gs2 = self.request()
        sched = self.svc.schedule_request(req_at_gs2["id"], "op", "operator",
                                          {"window_id": self.win2["id"], "antenna_id": "B1",
                                           "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)),
                                           "rate_mbps": 60})
        self.assertEqual(sched["status"], "scheduled")
        req = self.request()
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(req)])
        self.assertIn("satellite_conflict", [r["code"] for r in ctx.exception.details[0]["reasons"]])

    def test_hold_vs_hold_antenna_and_satellite(self):
        r1, r2, r3 = self.request(), self.request(), self.request(sat="SAT2")
        self.create_hold([self.item(r1, antenna="A1")])
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(r2, antenna="A1")])
        self.assertIn("antenna_hold_conflict", [r["code"] for r in ctx.exception.details[0]["reasons"]])
        # SAT2 在另一站 GS2 占 SAT1 不会冲突，但 GS1 用 A2 占 SAT2 与已有占位同星？不——已有占位是 SAT1。
        # 同星：再占一个 SAT1 即便换天线也不行
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(r2, antenna="A2")])
        self.assertIn("satellite_hold_conflict", [r["code"] for r in ctx.exception.details[0]["reasons"]])
        # 不同星、不同天线、时段相同：允许
        ok = self.create_hold([self.item(r3, win=self.win1b["id"], antenna="A2")])
        self.assertEqual(ok["batch"]["status"], "active")

    def test_internal_batch_conflicts(self):
        r1, r2 = self.request(), self.request()
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(r1, antenna="A1"), self.item(r2, antenna="A1")])
        codes = [reason["code"] for item in ctx.exception.details for reason in item["reasons"]]
        self.assertIn("batch_antenna_conflict", codes)
        # 同星两条即使不同天线也拦截
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(r1, antenna="A1"), self.item(r2, antenna="A2")])
        codes = [reason["code"] for item in ctx.exception.details for reason in item["reasons"]]
        self.assertIn("batch_satellite_conflict", codes)

    def test_quota_counts_held_seconds(self):
        r1 = self.request(mb=1000)
        self.create_hold([self.item(r1, antenna="A1", start_min=0, dur_min=120, rate=60)])  # 2h
        r2 = self.request(mb=1000)
        with self.assertRaises(ApiError) as ctx:
            self.create_hold([self.item(r2, antenna="A2", start_min=30, dur_min=120, rate=60)])  # 再 2h -> 4h > 3h
        self.assertIn("tenant_quota_exceeded", [r["code"] for r in ctx.exception.details[0]["reasons"]])

    # ---------- 权限与参数 ----------

    def test_permissions(self):
        req = self.request()
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold("v", "viewer", {"station_id": "GS1", "expires_at": self.expires, "items": [self.item(req)]})
        self.assertEqual(ctx.exception.status, 403)
        hold = self.create_hold([self.item(req)])
        # requester 可查本租户、不可查他租户（这里只有 T1，故能看）
        detail = self.svc.get_hold(hold["batch"]["id"], "requester", "T1")
        self.assertEqual(detail["batch"]["id"], hold["batch"]["id"])
        self.svc.create_satellite("op", "operator", {"id": "SATX", "name": "x", "data_rate_mbps": 10, "priority": 1, "storage_capacity_mb": 100, "tenant": "T2"})
        # viewer 不允许访问 holds 列表
        with self.assertRaises(ApiError) as ctx:
            self.svc.list_holds("viewer", "", None, None)
        self.assertEqual(ctx.exception.status, 403)
        # requester 不能建/转
        with self.assertRaises(ApiError) as ctx:
            self.svc.convert_hold(hold["batch"]["id"], "rq", "requester", {})
        self.assertEqual(ctx.exception.status, 403)

    def test_invalid_inputs(self):
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold("op", "operator", {"station_id": "GS1", "expires_at": self.expires, "items": []})
        self.assertEqual(ctx.exception.code, "items_required")
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold("op", "operator", {"station_id": "GS1", "expires_at": iso(utcnow() - timedelta(minutes=1)),
                                                    "items": [{"request_id": 1, "window_id": 1, "antenna_id": "A1",
                                                               "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=10)),
                                                               "rate_mbps": 10}]})
        self.assertEqual(ctx.exception.code, "hold_expiry_past")
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_hold("op", "operator", {"station_id": "NOPE", "expires_at": self.expires,
                                                    "items": [{"request_id": 1, "window_id": 1, "antenna_id": "A1",
                                                               "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=10)),
                                                               "rate_mbps": 10}]})
        self.assertEqual(ctx.exception.code, "station_not_found")

    def test_get_unknown_hold(self):
        with self.assertRaises(ApiError) as ctx:
            self.svc.get_hold(999, "operator", "")
        self.assertEqual(ctx.exception.code, "hold_not_found")


if __name__ == "__main__":
    unittest.main()
