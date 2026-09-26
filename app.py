#!/usr/bin/env python3
"""Multi-station satellite scheduling service using only Python's standard library."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from holds import ledger, validation

PORT = 8204
ROLES = {"viewer", "requester", "operator", "commander", "auditor"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message); self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool: return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, path: str | Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row; self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS satellites(id TEXT PRIMARY KEY, name TEXT NOT NULL, data_rate_mbps REAL NOT NULL, priority INTEGER NOT NULL, storage_capacity_mb REAL NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS stations(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', weather TEXT NOT NULL DEFAULT 'clear');
        CREATE TABLE IF NOT EXISTS antennas(id TEXT PRIMARY KEY, station_id TEXT NOT NULL REFERENCES stations(id), max_rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS maintenance(id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT REFERENCES antennas(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), station_id TEXT NOT NULL REFERENCES stations(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, max_rate_mbps REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), tenant TEXT NOT NULL, priority INTEGER NOT NULL, data_mb REAL NOT NULL, deadline TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_by TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotas(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, station_id TEXT NOT NULL REFERENCES stations(id), daily_seconds INTEGER NOT NULL, UNIQUE(tenant,station_id));
        CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL UNIQUE REFERENCES requests(id), window_id INTEGER NOT NULL REFERENCES visibility_windows(id), station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT NOT NULL REFERENCES antennas(id), satellite_id TEXT NOT NULL REFERENCES satellites(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'scheduled', revision INTEGER NOT NULL DEFAULT 1, disposition_reason TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, schedule_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        """)

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try: yield self.conn; self.conn.execute("COMMIT")
        except Exception: self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, request_id: int | None, schedule_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(request_id,schedule_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (request_id, schedule_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class SatelliteSchedulingService:
    def __init__(self, path: str | Path):
        self.repo = Repository(path)
        ledger.ensure_schema(self.repo.conn)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, tenant = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Tenant", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "requester" and not tenant: raise ApiError(401, "tenant_required", "requester 必须提供 X-Tenant")
        return actor, role, tenant

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

    def create_satellite(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护卫星")
        sid, name, tenant = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("tenant", "")).strip()
        rate, priority, capacity = body.get("data_rate_mbps"), body.get("priority"), body.get("storage_capacity_mb")
        if not sid or not name or not tenant or not isinstance(rate, (int, float)) or float(rate) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10 or not isinstance(capacity, (int, float)) or float(capacity) <= 0:
            raise ApiError(400, "invalid_satellite", "卫星参数不完整")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO satellites(id,name,data_rate_mbps,priority,storage_capacity_mb,tenant,status) VALUES(?,?,?,?,?,?,?)", (sid, name, float(rate), priority, float(capacity), tenant, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM satellites WHERE id=?", (sid,)).fetchone())

    def create_station(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护地面站")
        sid, name = str(body.get("id", "")).strip(), str(body.get("name", "")).strip()
        weather = str(body.get("weather", "clear")).lower()
        if not sid or not name or weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_station", "地面站名称或天气状态无效")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO stations(id,name,status,weather) VALUES(?,?,?,?)", (sid, name, body.get("status", "active"), weather))
            return dict(conn.execute("SELECT * FROM stations WHERE id=?", (sid,)).fetchone())

    def create_antenna(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护天线")
        aid, station, rate = str(body.get("id", "")).strip(), str(body.get("station_id", "")).strip(), body.get("max_rate_mbps")
        if not aid or not station or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_antenna", "天线参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT OR REPLACE INTO antennas(id,station_id,max_rate_mbps,status) VALUES(?,?,?,?)", (aid, station, float(rate), body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM antennas WHERE id=?", (aid,)).fetchone())

    def create_maintenance(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "maintenance_forbidden", "当前角色不能登记维护")
        station, start, end, reason = str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), str(body.get("reason", "")).strip()
        antenna = body.get("antenna_id")
        if not station or end <= start or not reason: raise ApiError(400, "invalid_maintenance", "维护参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            if antenna and not conn.execute("SELECT 1 FROM antennas WHERE id=? AND station_id=?", (antenna, station)).fetchone(): raise ApiError(400, "antenna_station_mismatch", "天线不属于该站")
            cur = conn.execute("INSERT INTO maintenance(station_id,antenna_id,starts_at,ends_at,reason) VALUES(?,?,?,?,?)", (station, antenna, iso(start), iso(end), reason))
            return dict(conn.execute("SELECT * FROM maintenance WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_window(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能维护可见窗口")
        satellite, station, start, end, rate = str(body.get("satellite_id", "")).strip(), str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("max_rate_mbps")
        if not satellite or not station or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_window", "可见窗口参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM satellites WHERE id=?", (satellite,)).fetchone(): raise ApiError(404, "satellite_not_found", "卫星不存在")
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            cur = conn.execute("INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES(?,?,?,?,?)", (satellite, station, iso(start), iso(end), float(rate)))
            return dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_quota(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "quota_forbidden", "当前角色不能设置租户配额")
        tenant, station, seconds = str(body.get("tenant", "")).strip(), str(body.get("station_id", "")).strip(), body.get("daily_seconds")
        if not tenant or not station or not isinstance(seconds, int) or seconds <= 0: raise ApiError(400, "invalid_quota", "配额参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT INTO quotas(tenant,station_id,daily_seconds) VALUES(?,?,?) ON CONFLICT(tenant,station_id) DO UPDATE SET daily_seconds=excluded.daily_seconds", (tenant, station, seconds))
            return dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone())

    def create_request(self, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"requester", "operator", "commander"}: raise ApiError(403, "request_forbidden", "当前角色不能创建数据请求")
        satellite = str(body.get("satellite_id", "")).strip(); data_mb = body.get("data_mb"); priority = body.get("priority"); deadline = parse_time(body.get("deadline"))
        owner = tenant if role == "requester" else str(body.get("tenant", "")).strip()
        if not satellite or not owner or not isinstance(data_mb, (int, float)) or float(data_mb) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10:
            raise ApiError(400, "invalid_request", "请求参数无效")
        with self.repo.tx() as conn:
            satellite_row = conn.execute("SELECT * FROM satellites WHERE id=?", (satellite,)).fetchone()
            if not satellite_row: raise ApiError(404, "satellite_not_found", "卫星不存在")
            if owner != satellite_row["tenant"]: raise ApiError(403, "tenant_satellite_forbidden", "租户不能申请不属于自己的卫星")
            if deadline <= utcnow(): raise ApiError(409, "deadline_expired", "请求截止时间已经过去")
            cur = conn.execute("INSERT INTO requests(satellite_id,tenant,priority,data_mb,deadline,created_by,created_at) VALUES(?,?,?,?,?,?,?)", (satellite, owner, priority, float(data_mb), iso(deadline), actor, iso()))
            request_id = cur.lastrowid; Repository.audit(conn, request_id, None, actor, role, "request_created", {"data_mb": float(data_mb), "deadline": iso(deadline)})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def _used_quota(self, conn: sqlite3.Connection, tenant: str, station: str, day: str, exclude_schedule: int | None = None) -> int:
        sql = """SELECT COALESCE(SUM((julianday(s.ends_at)-julianday(s.starts_at))*86400),0) FROM schedules s JOIN requests r ON r.id=s.request_id
                 WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=? AND s.status IN ('scheduled','receiving','received')"""
        args: list[Any] = [tenant, station, day]
        if exclude_schedule is not None: sql += " AND s.id!=?"; args.append(exclude_schedule)
        return int(conn.execute(sql, args).fetchone()[0])

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_schedule", "排程参数无效")
        with self.repo.tx() as conn:
            self._sweep_holds(conn, actor, role)
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not request or not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "请求、窗口或天线不存在")
            if request["status"] not in {"pending", "preempted"}: raise ApiError(409, "request_closed", "请求当前不能排程")
            if ledger.active_for_request(conn, request_id): raise ApiError(409, "request_held", "请求已有生效占位，请转换占位或先取消")
            if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]: raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
            station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
            satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
            if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active": raise ApiError(409, "resource_inactive", "卫星、地面站或天线不可用")
            if station["weather"] != "clear": raise ApiError(409, "weather_blocked", "天气条件不允许接收")
            w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
            if start < w_start or end > w_end: raise ApiError(409, "outside_visibility", "排程超出可见窗口")
            if end > parse_time(request["deadline"]): raise ApiError(409, "deadline_missed", "预计结束时间超过请求截止时间")
            max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
            if float(rate) > max_rate: raise ApiError(409, "rate_exceeded", "请求速率超过可用上限", {"max_rate_mbps": max_rate})
            transferred = (end - start).total_seconds() * float(rate) / 8
            if transferred < float(request["data_mb"]): raise ApiError(409, "insufficient_capacity", "窗口内可接收数据量不足", {"capacity_mb": transferred, "required_mb": request["data_mb"]})
            maintenance = conn.execute("""SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
            if maintenance: raise ApiError(409, "maintenance_conflict", "天线或地面站处于维护期", dict(maintenance))
            equipment = conn.execute("SELECT id,status FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
            if equipment: raise ApiError(409, "antenna_conflict", "天线时段已被占用", {"schedule_id": equipment["id"]})
            satellite_conflict = conn.execute("SELECT id,status FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (request["satellite_id"], iso(end), iso(start))).fetchone()
            if satellite_conflict: raise ApiError(409, "satellite_conflict", "同一卫星时段已被其他站接收", {"schedule_id": satellite_conflict["id"]})
            antenna_hold = ledger.overlapping(conn, "antenna_id", antenna_id, iso(end), iso(start))
            if antenna_hold: raise ApiError(409, "antenna_hold_conflict", "天线时段已被占位保留", {"hold_id": antenna_hold["id"]})
            satellite_hold = ledger.overlapping(conn, "satellite_id", request["satellite_id"], iso(end), iso(start))
            if satellite_hold: raise ApiError(409, "satellite_hold_conflict", "同一卫星时段已被占位保留", {"hold_id": satellite_hold["id"]})
            quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
            used = self._used_quota(conn, request["tenant"], station["id"], start.date().isoformat()) + ledger.hold_seconds(conn, request["tenant"], station["id"], start.date().isoformat())
            duration = int((end - start).total_seconds())
            if quota and used + duration > quota["daily_seconds"]: raise ApiError(409, "tenant_quota_exceeded", "租户当日地面站配额不足", {"used_seconds": used, "requested_seconds": duration, "limit": quota["daily_seconds"]})
            schedule_id = self._insert_schedule(conn, request=request, window=window, antenna_id=antenna_id, start=start, end=end, rate=rate, actor=actor)
            Repository.audit(conn, request_id, schedule_id, actor, role, "schedule_created", {"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": transferred})
            return self.get_schedule(schedule_id)

    def _insert_schedule(self, conn: sqlite3.Connection, *, request: sqlite3.Row, window: sqlite3.Row, antenna_id: str, start: datetime, end: datetime, rate: float, actor: str) -> int:
        cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,created_by,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                           (request["id"], window["id"], window["station_id"], antenna_id, request["satellite_id"], iso(start), iso(end), float(rate), actor, iso(), iso()))
        conn.execute("UPDATE requests SET status='scheduled' WHERE id=?", (request["id"],))
        return cur.lastrowid

    def get_schedule(self, schedule_id: int) -> dict[str, Any]:
        row = self.repo.conn.execute("""SELECT s.*,r.tenant,r.data_mb,r.priority request_priority,r.deadline FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        return dict(row)

    def transition(self, schedule_id: int, actor: str, role: str, tenant: str, target: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if target == "receiving":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能开始接收")
                if row["status"] != "scheduled": raise ApiError(409, "invalid_transition", "只有已排程任务可以开始接收")
                conn.execute("UPDATE schedules SET status='receiving',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
            elif target == "received":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能完成接收")
                if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以完成")
                conn.execute("UPDATE schedules SET status='received',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
                conn.execute("UPDATE requests SET status='received' WHERE id=?", (row["request_id"],))
            else: raise ApiError(400, "invalid_transition", "未知状态")
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, f"receive_{target}", {})
            return self.get_schedule(schedule_id)

    def cancel_schedule(self, schedule_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role == "requester":
                if row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户排程")
                if row["status"] != "scheduled": raise ApiError(409, "cancel_not_allowed", "接收开始后租户不能取消")
            elif role not in {"operator", "commander"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消排程")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据不能取消或删除")
            if row["status"] == "canceled": return self.get_schedule(schedule_id)
            conn.execute("UPDATE schedules SET status='canceled',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), schedule_id))
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "schedule_canceled", {"reason": reason})
            return self.get_schedule(schedule_id)

    def emergency_preempt(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以执行紧急抢占")
        order_id, reason = str(body.get("order_id", "")).strip(), str(body.get("reason", "")).strip()
        if not order_id or not reason: raise ApiError(400, "emergency_details_required", "order_id 和 reason 必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.priority,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据的排程不能被抢占")
            if row["status"] not in {"scheduled", "receiving"}: raise ApiError(409, "invalid_transition", "当前排程不可抢占")
            conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (f"{order_id}: {reason}", iso(), schedule_id))
            conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "emergency_preemption", {"order_id": order_id, "reason": reason, "displaced_priority": row["priority"]})
            return {"schedule": self.get_schedule(schedule_id), "reschedule_required": True, "order_id": order_id}

    def change_window(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能变更可见窗口")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_window", "窗口结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            if not window: raise ApiError(404, "window_not_found", "可见窗口不存在")
            rows = conn.execute("SELECT * FROM schedules WHERE window_id=? AND status IN ('scheduled','receiving','received')", (window_id,)).fetchall()
            impacts = []
            for row in rows:
                sched_start, sched_end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
                invalid = sched_start < start or sched_end > end
                if row["status"] == "received":
                    impacts.append({"schedule_id": row["id"], "action": "preserve_received_data", "reason": "已接收数据不可回滚", "invalid": invalid})
                    continue
                if invalid:
                    conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", ("visibility_window_changed", iso(), row["id"]))
                    conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
                    impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preempted", "reason": "新窗口无法覆盖原排程", "old_start": row["starts_at"], "old_end": row["ends_at"]})
                else:
                    impacts.append({"schedule_id": row["id"], "action": "unchanged", "reason": "新窗口仍覆盖排程"})
            conn.execute("UPDATE visibility_windows SET starts_at=?,ends_at=?,revision=revision+1 WHERE id=?", (iso(start), iso(end), window_id))
            Repository.audit(conn, None, None, actor, role, "visibility_window_changed", {"window_id": window_id, "impacts": impacts})
            return {"window": dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()), "impacts": impacts}

    def reschedule(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] != "preempted": raise ApiError(409, "reschedule_not_needed", "只有被抢占请求需要重排")
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, None, actor, role, "reschedule_requested", {"reason": body.get("reason", "")})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def _sweep_holds(self, conn: sqlite3.Connection, actor: str, role: str) -> None:
        for released in ledger.release_expired(conn, iso()):
            Repository.audit(conn, released["request_id"], None, actor, role, "hold_expired", {"hold_id": released["id"], "batch_id": released["batch_id"]})

    def create_hold_batch(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "hold_forbidden", "当前角色不能创建占位")
        station_id, antenna_id = str(body.get("station_id", "")).strip(), str(body.get("antenna_id", "")).strip()
        expires, raw_items = parse_time(body.get("expires_at")), body.get("items")
        if not station_id or not antenna_id or not isinstance(raw_items, list) or not raw_items or len(raw_items) > 100:
            raise ApiError(400, "invalid_hold_batch", "占位批次需要 station_id、antenna_id、expires_at 和 1-100 条 items")
        if expires <= utcnow(): raise ApiError(400, "invalid_hold_expiry", "占位过期时间必须晚于当前时间")
        items = []
        for i, raw in enumerate(raw_items):
            if not isinstance(raw, dict): raise ApiError(400, "invalid_hold_item", f"第 {i + 1} 项不是对象")
            request_id, window_id, rate = raw.get("request_id"), raw.get("window_id"), raw.get("rate_mbps")
            start, end = parse_time(raw.get("starts_at")), parse_time(raw.get("ends_at"))
            if not isinstance(request_id, int) or not isinstance(window_id, int) or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0:
                raise ApiError(400, "invalid_hold_item", f"第 {i + 1} 项占位参数无效")
            items.append({"index": i, "request_id": request_id, "window_id": window_id, "start": start, "end": end, "start_iso": iso(start), "end_iso": iso(end), "rate_mbps": float(rate)})
        with self.repo.tx() as conn:
            self._sweep_holds(conn, actor, role)
            conflicts = validation.validate_batch(conn, station_id=station_id, antenna_id=antenna_id, items=items)
            if conflicts: raise ApiError(409, "hold_batch_conflict", "占位批次存在冲突，整批未入库", {"conflicts": conflicts})
            batch_id, now_iso = f"H{uuid.uuid4().hex[:12]}", iso()
            for item in items:
                request = conn.execute("SELECT satellite_id,tenant FROM requests WHERE id=?", (item["request_id"],)).fetchone()
                item["satellite_id"], item["tenant"] = request["satellite_id"], request["tenant"]
            hold_ids = ledger.insert_batch(conn, batch_id=batch_id, station_id=station_id, antenna_id=antenna_id, expires_iso=iso(expires), actor=actor, now_iso=now_iso, items=items)
            Repository.audit(conn, None, None, actor, role, "hold_batch_created", {"batch_id": batch_id, "hold_ids": hold_ids, "request_ids": [item["request_id"] for item in items], "station_id": station_id, "antenna_id": antenna_id, "expires_at": iso(expires)})
            return {"batch_id": batch_id, "holds": [ledger.get(conn, hold_id) for hold_id in hold_ids]}

    def list_holds(self, actor: str, role: str, tenant: str, query: dict[str, list[str]]) -> dict[str, Any]:
        status, station_id = (query.get("status") or [None])[0], (query.get("station_id") or [None])[0]
        with self.repo.tx() as conn:
            self._sweep_holds(conn, actor, role)
            return {"holds": ledger.list_holds(conn, status=status, tenant=tenant if role == "requester" else None, station_id=station_id), "server_time": iso()}

    def get_hold(self, hold_id: int, actor: str, role: str, tenant: str) -> dict[str, Any]:
        with self.repo.tx() as conn:
            self._sweep_holds(conn, actor, role)
            hold = ledger.get(conn, hold_id)
            if not hold: raise ApiError(404, "hold_not_found", "占位不存在")
            if role == "requester" and hold["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能查看其他租户占位")
            return hold

    def cancel_hold(self, hold_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            self._sweep_holds(conn, actor, role)
            hold = ledger.get(conn, hold_id)
            if not hold: raise ApiError(404, "hold_not_found", "占位不存在")
            if role == "requester":
                if hold["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户占位")
            elif role not in {"operator", "commander"}: raise ApiError(403, "hold_forbidden", "当前角色不能取消占位")
            if hold["status"] == "canceled": return hold
            if hold["status"] != "active": raise ApiError(409, "hold_not_active", "占位已过期或已转换，不能取消")
            ledger.set_status(conn, hold_id, "canceled", iso(), reason=reason)
            Repository.audit(conn, hold["request_id"], None, actor, role, "hold_canceled", {"hold_id": hold_id, "batch_id": hold["batch_id"], "reason": reason})
            return ledger.get(conn, hold_id)

    def convert_hold(self, hold_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "hold_forbidden", "当前角色不能转换占位")
        with self.repo.tx() as conn:
            self._sweep_holds(conn, actor, role)
            hold = ledger.get(conn, hold_id)
            if not hold: raise ApiError(404, "hold_not_found", "占位不存在")
            if hold["status"] != "active": raise ApiError(409, "hold_not_active", "占位已过期或已转换，不能转换")
            request = conn.execute("SELECT * FROM requests WHERE id=?", (hold["request_id"],)).fetchone()
            conflicts = validation.validate_conversion(conn, hold=hold, request=request)
            if conflicts: raise ApiError(409, "hold_conversion_conflict", "占位转换核验失败，占位仍保留", {"conflicts": conflicts})
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (hold["window_id"],)).fetchone()
            schedule_id = self._insert_schedule(conn, request=request, window=window, antenna_id=hold["antenna_id"], start=parse_time(hold["starts_at"]), end=parse_time(hold["ends_at"]), rate=hold["rate_mbps"], actor=actor)
            ledger.set_status(conn, hold_id, "converted", iso(), schedule_id=schedule_id)
            Repository.audit(conn, hold["request_id"], schedule_id, actor, role, "hold_converted", {"hold_id": hold_id, "batch_id": hold["batch_id"], "schedule_id": schedule_id})
            return {"hold": ledger.get(conn, hold_id), "schedule": self.get_schedule(schedule_id)}

    def state(self, actor: str, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        with self.repo.tx() as sweep_conn: self._sweep_holds(sweep_conn, actor, role)
        if role == "requester":
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute("SELECT s.* FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.id DESC", (tenant,))]
            stations, holds = [], ledger.list_holds(conn, tenant=tenant)
        elif role == "viewer":
            requests, stations = [], []
            schedules = [dict(r) for r in conn.execute("SELECT id,status,starts_at,ends_at FROM schedules ORDER BY id DESC")]
            holds = [dict(r) for r in conn.execute("SELECT id,status,starts_at,ends_at FROM holds ORDER BY id DESC")]
        else:
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY id DESC")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
            holds = ledger.list_holds(conn)
        return {"requests": requests, "schedules": schedules, "stations": stations, "holds": holds, "server_time": iso()}


def send_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode(); handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: SatelliteSchedulingService; web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size: return {}
        try: value = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str, query: dict[str, list[str]]) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "satellite-scheduling"}
        actor, role, tenant = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state(actor, role, tenant)
        parts = [p for p in path.split("/") if p]
        if parts == ["api", "holds"]: return 200, self.service.list_holds(actor, role, tenant, query)
        if len(parts) == 3 and parts[:2] == ["api", "holds"] and parts[2].isdigit(): return 200, self.service.get_hold(int(parts[2]), actor, role, tenant)
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit(): return 200, self.service.get_schedule(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role, tenant = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        actions = {
            "/api/satellites": lambda: (201, self.service.create_satellite(actor, role, body)),
            "/api/stations": lambda: (201, self.service.create_station(actor, role, body)),
            "/api/antennas": lambda: (201, self.service.create_antenna(actor, role, body)),
            "/api/maintenance": lambda: (201, self.service.create_maintenance(actor, role, body)),
            "/api/visibility-windows": lambda: (201, self.service.create_window(actor, role, body)),
            "/api/quotas": lambda: (200, self.service.set_quota(actor, role, body)),
            "/api/requests": lambda: (201, self.service.create_request(actor, role, tenant, body)),
            "/api/holds": lambda: (201, self.service.create_hold_batch(actor, role, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "holds"] and parts[2].isdigit():
            hid, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_hold(hid, actor, role, tenant, body)
            if action == "convert": return 200, self.service.convert_hold(hid, actor, role)
        if len(parts) == 4 and parts[:2] == ["api", "requests"] and parts[2].isdigit():
            rid, action = int(parts[2]), parts[3]
            if action == "schedule": return 201, self.service.schedule_request(rid, actor, role, body)
            if action == "reschedule": return 200, self.service.reschedule(rid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            sid, action = int(parts[2]), parts[3]
            if action in {"start", "complete"}: return 200, self.service.transition(sid, actor, role, tenant, "receiving" if action == "start" else "received", body)
            if action == "cancel": return 200, self.service.cancel_schedule(sid, actor, role, tenant, body)
            if action == "preempt": return 200, self.service.emergency_preempt(sid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "visibility-windows"] and parts[2].isdigit() and parts[3] == "change": return 200, self.service.change_window(int(parts[2]), actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path, parse_qs(parsed.query)) if method == "GET" else self.post_api(parsed.path); send_json(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            send_json(self, exc.status, payload)
        except Exception as exc: print(f"unhandled error: {exc!r}"); send_json(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = SatelliteSchedulingService(db_path); handler = type("SatelliteHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"}); return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("SAT_DB", "satellite_scheduling.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"satellite-scheduling listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
