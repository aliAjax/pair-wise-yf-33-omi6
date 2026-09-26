#!/usr/bin/env python3
"""Multi-station satellite scheduling service using only Python's standard library."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from hold_ledger import HoldLedger
from hold_policy import HoldPolicy

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
        self.holds = HoldLedger(self.repo)
        self.policy = HoldPolicy(self.holds)

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

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_schedule", "排程参数无效")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not request or not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "请求、窗口或天线不存在")
            if request["status"] not in {"pending", "preempted"}:
                if request["status"] == "held": raise ApiError(409, "request_held", "请求处于临时占位中，需先转换或释放占位")
                raise ApiError(409, "request_closed", "请求当前不能排程")
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
            antenna_hold = self.holds.antenna_hold(conn, antenna_id, iso(end), iso(start))
            if antenna_hold: raise ApiError(409, "antenna_hold_conflict", "天线时段已被临时占位保留", dict(antenna_hold))
            satellite_conflict = conn.execute("SELECT id,status FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (request["satellite_id"], iso(end), iso(start))).fetchone()
            if satellite_conflict: raise ApiError(409, "satellite_conflict", "同一卫星时段已被其他站接收", {"schedule_id": satellite_conflict["id"]})
            satellite_hold = self.holds.satellite_hold(conn, request["satellite_id"], iso(end), iso(start))
            if satellite_hold: raise ApiError(409, "satellite_hold_conflict", "同一卫星时段已被临时占位保留", dict(satellite_hold))
            quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
            duration = int((end - start).total_seconds())
            if quota:
                used_sched, used_hold = self.holds.used_seconds(conn, request["tenant"], station["id"], start.date().isoformat())
                if used_sched + used_hold + duration > quota["daily_seconds"]:
                    raise ApiError(409, "tenant_quota_exceeded", "租户当日地面站配额不足",
                                   {"used_seconds": used_sched, "held_seconds": used_hold, "requested_seconds": duration, "limit": quota["daily_seconds"]})
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                               (request_id, window_id, station["id"], antenna_id, request["satellite_id"], iso(start), iso(end), float(rate), actor, iso(), iso()))
            schedule_id = cur.lastrowid
            conn.execute("UPDATE requests SET status='scheduled' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, schedule_id, actor, role, "schedule_created", {"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": transferred})
            return self.get_schedule(schedule_id)

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

    # ---------------- 临时占位 ----------------

    @staticmethod
    def _parse_hold_items(body: dict[str, Any]) -> list[dict[str, Any]]:
        raw = body.get("items")
        if not isinstance(raw, list) or not raw: raise ApiError(400, "items_required", "items 必须是非空数组")
        if len(raw) > 200: raise ApiError(400, "too_many_items", "单批占位不能超过 200 条")
        candidates: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict): raise ApiError(400, "invalid_item", "items 元素必须是对象")
            window_id, antenna_id = item.get("window_id"), str(item.get("antenna_id", "")).strip()
            request_id, rate = item.get("request_id"), item.get("rate_mbps")
            if not isinstance(request_id, int) or not isinstance(window_id, int) or not antenna_id:
                raise ApiError(400, "invalid_item", "每条占位需提供整数 request_id、window_id 和 antenna_id")
            if not isinstance(rate, (int, float)) or float(rate) <= 0:
                raise ApiError(400, "invalid_item", "rate_mbps 必须是正数")
            start, end = parse_time(item.get("starts_at")), parse_time(item.get("ends_at"))
            if end <= start: raise ApiError(400, "invalid_item", "占位结束时间必须晚于开始时间")
            candidates.append({"request_id": request_id, "window_id": window_id, "antenna_id": antenna_id,
                               "starts_at": start, "ends_at": end, "rate_mbps": float(rate),
                               "starts_at_iso": iso(start), "ends_at_iso": iso(end)})
        return candidates

    def _sweep(self, conn: sqlite3.Connection) -> list[int]:
        return self.holds.sweep_expired(conn, iso())

    def create_hold(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "hold_forbidden", "只有排程员可以登记临时占位")
        station_id, note = str(body.get("station_id", "")).strip(), str(body.get("note", "")).strip()
        expires_at = parse_time(body.get("expires_at"))
        candidates = self._parse_hold_items(body)
        if not station_id: raise ApiError(400, "station_required", "必须提供 station_id")
        if expires_at <= utcnow(): raise ApiError(400, "hold_expiry_past", "占位到期时间必须晚于当前时间")
        with self.repo.tx() as conn:
            self._sweep(conn)
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station_id,)).fetchone():
                raise ApiError(404, "station_not_found", "地面站不存在")
            verdict = self.policy.validate(conn, station_id, candidates)
            if not verdict["ok"]:
                Repository.audit(conn, None, None, actor, role, "hold_rejected",
                                 {"station_id": station_id, "verdict": verdict})
                if "global" in verdict:
                    raise ApiError(409, "hold_global_conflict", "占位整批未入库，存在全局错误", verdict["global"])
                raise ApiError(409, "hold_batch_conflict", "占位整批未入库，请处理冲突后重试", verdict["conflicts"])
            batch_id = self.holds.insert_batch(conn, station_id, iso(expires_at), note, verdict["normalized"], actor, iso())
            Repository.audit(conn, None, None, actor, role, "hold_created",
                             {"batch_id": batch_id, "station_id": station_id,
                              "request_ids": [x["request_id"] for x in verdict["normalized"]],
                              "expires_at": iso(expires_at)})
            return self.holds.detail(conn, batch_id)

    def list_holds(self, role: str, tenant: str, status: str | None, station_id: str | None) -> dict[str, Any]:
        if role not in {"operator", "commander", "auditor", "requester"}:
            raise ApiError(403, "hold_forbidden", "当前角色不能查询占位")
        with self.repo.tx() as conn:
            released = self._sweep(conn)
        conn = self.repo.conn
        batches = self.holds.list_batches(conn, status=status, station_id=station_id)
        result = []
        for b in batches:
            items = self.holds.list_items(conn, b["id"])
            if role == "requester":
                items = [it for it in items if it["tenant"] == tenant]
                if not items: continue
            result.append({"batch": b, "items": items, "item_count": len(items)})
        return {"holds": result, "released_batch_ids": released, "server_time": iso()}

    def get_hold(self, batch_id: int, role: str, tenant: str) -> dict[str, Any]:
        if role not in {"operator", "commander", "auditor", "requester"}:
            raise ApiError(403, "hold_forbidden", "当前角色不能查询占位")
        with self.repo.tx() as conn:
            self._sweep(conn)
            detail = self.holds.detail(conn, batch_id)
        if not detail: raise ApiError(404, "hold_not_found", "占位批次不存在")
        if role == "requester":
            visible = [it for it in detail["items"] if it["tenant"] == tenant]
            if not visible: raise ApiError(403, "tenant_forbidden", "不能查看其他租户的占位")
            detail = {"batch": {k: v for k, v in detail["batch"].items() if k in {"id", "station_id", "status", "expires_at"}},
                      "items": visible, "item_count": len(visible)}
        return detail

    def cancel_hold(self, batch_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander", "requester"}:
            raise ApiError(403, "hold_forbidden", "当前角色不能取消占位")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            batch = self.holds.get_batch(conn, batch_id)
            if not batch: raise ApiError(404, "hold_not_found", "占位批次不存在")
            if batch["status"] != "active":
                raise ApiError(409, "hold_not_active", f"占位批次已处于 {batch['status']} 状态，不能取消",
                               {"status": batch["status"]})
            if role == "requester":
                tenants = {r["tenant"] for r in conn.execute(
                    """SELECT DISTINCT r.tenant FROM hold_items hi JOIN requests r ON r.id=hi.request_id
                       WHERE hi.batch_id=? AND hi.status='held'""", (batch_id,)).fetchall()}
                if tenants != {tenant}: raise ApiError(403, "tenant_forbidden", "只能取消本租户且不含其他租户请求的占位")
            self.holds.mark_canceled(conn, batch_id, reason, iso())
            Repository.audit(conn, None, None, actor, role, "hold_canceled",
                             {"batch_id": batch_id, "reason": reason})
            return self.holds.detail(conn, batch_id)

    def convert_hold(self, batch_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "hold_forbidden", "只有排程员可以转换占位")
        with self.repo.tx() as conn:
            self._sweep(conn)
            batch = self.holds.get_batch(conn, batch_id)
            if not batch: raise ApiError(404, "hold_not_found", "占位批次不存在")
            if batch["status"] != "active":
                raise ApiError(409, "hold_not_active", f"占位批次已处于 {batch['status']} 状态，不能转换",
                               {"status": batch["status"]})
            items = self.holds.list_items(conn, batch_id)
            held = [it for it in items if it["status"] == "held"]
            candidates = [{"request_id": it["request_id"], "window_id": it["window_id"],
                           "antenna_id": it["antenna_id"],
                           "starts_at": parse_time(it["starts_at"]), "ends_at": parse_time(it["ends_at"]),
                           "rate_mbps": float(it["rate_mbps"]),
                           "starts_at_iso": it["starts_at"], "ends_at_iso": it["ends_at"]} for it in held]
            # 转换时重新核验（排除本批次自身占用）；核验失败保留占位。
            verdict = self.policy.validate(conn, batch["station_id"], candidates, exclude_batch=batch_id)
            if not verdict["ok"]:
                Repository.audit(conn, None, None, actor, role, "hold_convert_rejected",
                                 {"batch_id": batch_id, "verdict": verdict})
                if "global" in verdict:
                    raise ApiError(409, "hold_global_conflict", "转换失败，存在全局错误", verdict["global"])
                raise ApiError(409, "hold_convert_conflict", "转换重新核验失败，占位予以保留", verdict["conflicts"])
            schedule_ids: dict[int, int] = {}
            schedules: list[dict[str, Any]] = []
            for entry in verdict["normalized"]:
                entry["station_id"] = batch["station_id"]
                sid = self._insert_schedule(conn, entry, actor)
                schedule_ids[entry["request_id"]] = sid
                schedules.append(self.get_schedule(sid))
            self.holds.mark_converted(conn, batch_id, schedule_ids, iso())
            Repository.audit(conn, None, None, actor, role, "hold_converted",
                             {"batch_id": batch_id, "schedule_ids": schedule_ids})
            return {"batch": self.holds.get_batch(conn, batch_id), "items": self.holds.list_items(conn, batch_id),
                    "schedules": schedules}

    def _insert_schedule(self, conn: sqlite3.Connection, entry: dict[str, Any], actor: str) -> int:
        """核验通过后落正式排程；请求由 held 转 scheduled。"""
        cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,created_by,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                           (entry["request_id"], entry["window_id"], entry["station_id"],
                            entry["antenna_id"], entry["satellite_id"], entry["starts_at"], entry["ends_at"],
                            float(entry["rate_mbps"]), actor, iso(), iso()))
        schedule_id = cur.lastrowid
        conn.execute("UPDATE requests SET status='scheduled' WHERE id=?", (entry["request_id"],))
        Repository.audit(conn, entry["request_id"], schedule_id, actor, "operator", "schedule_from_hold",
                         {"batch_window_id": entry["window_id"], "antenna_id": entry["antenna_id"]})
        return schedule_id

    def sweep_holds(self, actor: str, role: str) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "hold_forbidden", "只有排程员可以手动清扫到期占位")
        with self.repo.tx() as conn:
            released = self._sweep(conn)
            if released:
                Repository.audit(conn, None, None, actor, role, "holds_swept", {"batch_ids": released})
        return {"released_batch_ids": released, "server_time": iso()}

    def state(self, role: str, tenant: str) -> dict[str, Any]:
        with self.repo.tx() as conn:
            self._sweep(conn)
        conn = self.repo.conn
        if role == "requester":
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute("SELECT s.* FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.id DESC", (tenant,))]
            stations = []
            holds = [{"batch": b, "items": [it for it in self.holds.list_items(conn, b["id"]) if it["tenant"] == tenant]}
                     for b in self.holds.list_batches(conn, status="active")]
            holds = [h for h in holds if h["items"]]
        elif role == "viewer":
            requests, schedules, stations = [], [dict(r) for r in conn.execute("SELECT id,status,starts_at,ends_at FROM schedules ORDER BY id DESC")], []
            holds = []
        else:
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY id DESC")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
            holds = [{"batch": b, "items": self.holds.list_items(conn, b["id"])}
                     for b in self.holds.list_batches(conn, status="active")]
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
        if path == "/api/state": return 200, self.service.state(role, tenant)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit(): return 200, self.service.get_schedule(int(parts[2]))
        if path == "/api/holds":
            status = query.get("status", [None])[0]
            station_id = query.get("station_id", [None])[0]
            return 200, self.service.list_holds(role, tenant, status, station_id)
        if len(parts) == 3 and parts[:2] == ["api", "holds"] and parts[2].isdigit():
            return 200, self.service.get_hold(int(parts[2]), role, tenant)
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
            "/api/holds": lambda: (201, self.service.create_hold(actor, role, body)),
            "/api/holds/sweep": lambda: (200, self.service.sweep_holds(actor, role)),
        }
        if path in actions: return actions[path]()
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
        if len(parts) == 4 and parts[:2] == ["api", "holds"] and parts[2].isdigit():
            hid, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_hold(hid, actor, role, tenant, body)
            if action == "convert": return 200, self.service.convert_hold(hid, actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path, query) if method == "GET" else self.post_api(parsed.path); send_json(self, status, payload)
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
