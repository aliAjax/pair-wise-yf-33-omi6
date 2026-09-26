"""占位判定：批量占位与占位转换的只读冲突检查。

所有函数只查询不写入，冲突以字典列表返回（空列表 = 通过），由调用方（app.py）决定如何报错。
判定项：可见窗口、维护时段、同星接收、已有排程、已有占位和租户配额。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from . import ledger


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _conflict(index: int | None, request_id: int | None, code: str, message: str, **extra: Any) -> dict[str, Any]:
    entry = {"index": index, "request_id": request_id, "code": code, "message": message}
    entry.update(extra)
    return entry


def _schedule_seconds(conn: sqlite3.Connection, tenant: str, station_id: str, day: str) -> int:
    return int(conn.execute("""SELECT COALESCE(SUM((julianday(s.ends_at)-julianday(s.starts_at))*86400),0)
                               FROM schedules s JOIN requests r ON r.id=s.request_id
                               WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=? AND s.status IN ('scheduled','receiving','received')""",
                            (tenant, station_id, day)).fetchone()[0])


def _check_slot(conn: sqlite3.Connection, *, index: int | None, request_id: int | None, station_id: str, antenna_id: str,
                satellite_id: str, start_iso: str, end_iso: str, exclude_hold_id: int | None = None) -> list[dict[str, Any]]:
    """时段级冲突：维护、已有排程（天线与同星）、已有占位（天线与同星）。"""
    conflicts = []
    maintenance = conn.execute("SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?",
                               (station_id, antenna_id, end_iso, start_iso)).fetchone()
    if maintenance: conflicts.append(_conflict(index, request_id, "maintenance_conflict", "天线或地面站处于维护期", maintenance_id=maintenance["id"]))
    equipment = conn.execute("SELECT id FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')",
                             (station_id, antenna_id, end_iso, start_iso)).fetchone()
    if equipment: conflicts.append(_conflict(index, request_id, "antenna_conflict", "天线时段已被排程占用", schedule_id=equipment["id"]))
    satellite = conn.execute("SELECT id FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')",
                             (satellite_id, end_iso, start_iso)).fetchone()
    if satellite: conflicts.append(_conflict(index, request_id, "satellite_conflict", "同一卫星时段已被其他站接收", schedule_id=satellite["id"]))
    antenna_hold = ledger.overlapping(conn, "antenna_id", antenna_id, end_iso, start_iso, exclude_hold_id)
    if antenna_hold: conflicts.append(_conflict(index, request_id, "antenna_hold_conflict", "天线时段已被其他占位保留", hold_id=antenna_hold["id"]))
    satellite_hold = ledger.overlapping(conn, "satellite_id", satellite_id, end_iso, start_iso, exclude_hold_id)
    if satellite_hold: conflicts.append(_conflict(index, request_id, "satellite_hold_conflict", "同一卫星时段已被其他占位保留", hold_id=satellite_hold["id"]))
    return conflicts


def validate_batch(conn: sqlite3.Connection, *, station_id: str, antenna_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """整批占位判定：返回全部冲突，空列表表示可以整批入库。items 元素含 index/request_id/window_id/start/end/start_iso/end_iso/rate_mbps。"""
    station = conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()
    if not station: return [_conflict(None, None, "station_not_found", "地面站不存在")]
    antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
    if not antenna: return [_conflict(None, None, "antenna_not_found", "天线不存在")]
    if antenna["station_id"] != station_id: return [_conflict(None, None, "antenna_station_mismatch", "天线不属于该站")]
    conflicts: list[dict[str, Any]] = []
    if station["status"] != "active" or antenna["status"] != "active":
        conflicts.append(_conflict(None, None, "resource_inactive", "地面站或天线不可用"))
    if station["weather"] != "clear":
        conflicts.append(_conflict(None, None, "weather_blocked", "天气条件不允许接收"))
    usage: dict[tuple[str, str], int] = {}
    seen_requests: set[int] = set()
    for pos, item in enumerate(items):
        index, request_id = item["index"], item["request_id"]
        request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
        if not request:
            conflicts.append(_conflict(index, request_id, "request_not_found", "请求不存在")); continue
        window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (item["window_id"],)).fetchone()
        if not window:
            conflicts.append(_conflict(index, request_id, "window_not_found", "可见窗口不存在")); continue
        satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
        if not satellite:
            conflicts.append(_conflict(index, request_id, "satellite_not_found", "卫星不存在")); continue
        if request_id in seen_requests:
            conflicts.append(_conflict(index, request_id, "request_already_held", "同一请求在批次中重复占位"))
        seen_requests.add(request_id)
        if request["status"] not in {"pending", "preempted"}:
            conflicts.append(_conflict(index, request_id, "request_closed", "请求当前不能占位"))
        if window["satellite_id"] != request["satellite_id"] or window["station_id"] != station_id:
            conflicts.append(_conflict(index, request_id, "window_mismatch", "卫星、窗口和地面站不匹配"))
        if satellite["status"] != "active":
            conflicts.append(_conflict(index, request_id, "resource_inactive", "卫星不可用"))
        if item["start"] < _parse(window["starts_at"]) or item["end"] > _parse(window["ends_at"]):
            conflicts.append(_conflict(index, request_id, "outside_visibility", "占位超出可见窗口"))
        if item["end"] > _parse(request["deadline"]):
            conflicts.append(_conflict(index, request_id, "deadline_missed", "占位结束时间超过请求截止时间"))
        max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
        if item["rate_mbps"] > max_rate:
            conflicts.append(_conflict(index, request_id, "rate_exceeded", "占位速率超过可用上限", max_rate_mbps=max_rate))
        duration = int((item["end"] - item["start"]).total_seconds())
        transferred = duration * item["rate_mbps"] / 8
        if transferred < float(request["data_mb"]):
            conflicts.append(_conflict(index, request_id, "insufficient_capacity", "占位时段可接收数据量不足", capacity_mb=transferred, required_mb=request["data_mb"]))
        conflicts.extend(_check_slot(conn, index=index, request_id=request_id, station_id=station_id, antenna_id=antenna_id,
                                     satellite_id=request["satellite_id"], start_iso=item["start_iso"], end_iso=item["end_iso"]))
        if ledger.active_for_request(conn, request_id):
            conflicts.append(_conflict(index, request_id, "request_already_held", "请求已有生效占位"))
        for prev in items[:pos]:
            if item["start"] < prev["end"] and prev["start"] < item["end"]:
                conflicts.append(_conflict(index, request_id, "batch_item_overlap", "批次内占位时段互相重叠", other_index=prev["index"], other_request_id=prev["request_id"]))
                break
        day = item["start"].date().isoformat()
        key = (request["tenant"], day)
        used = _schedule_seconds(conn, request["tenant"], station_id, day) + ledger.hold_seconds(conn, request["tenant"], station_id, day) + usage.get(key, 0)
        quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station_id)).fetchone()
        if quota and used + duration > quota["daily_seconds"]:
            conflicts.append(_conflict(index, request_id, "tenant_quota_exceeded", "租户当日地面站配额不足", used_seconds=used, requested_seconds=duration, limit=quota["daily_seconds"]))
        usage[key] = usage.get(key, 0) + duration
    return conflicts


def validate_conversion(conn: sqlite3.Connection, *, hold: dict[str, Any], request: sqlite3.Row | None) -> list[dict[str, Any]]:
    """占位转正式排程前的重新核验：返回全部冲突，空列表表示可转换；有冲突时调用方应保留占位。"""
    request_id = hold["request_id"]
    if not request:
        return [_conflict(None, request_id, "request_not_found", "请求不存在")]
    conflicts: list[dict[str, Any]] = []
    start, end = _parse(hold["starts_at"]), _parse(hold["ends_at"])
    if request["status"] not in {"pending", "preempted"}:
        conflicts.append(_conflict(None, request_id, "request_closed", "请求已排程或关闭，占位不能转换"))
    window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (hold["window_id"],)).fetchone()
    satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (hold["satellite_id"],)).fetchone()
    station = conn.execute("SELECT * FROM stations WHERE id=?", (hold["station_id"],)).fetchone()
    antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (hold["antenna_id"],)).fetchone()
    if not window:
        conflicts.append(_conflict(None, request_id, "window_not_found", "可见窗口不存在"))
    elif start < _parse(window["starts_at"]) or end > _parse(window["ends_at"]):
        conflicts.append(_conflict(None, request_id, "outside_visibility", "窗口变更后不再覆盖占位时段"))
    if not satellite or satellite["status"] != "active" or not station or station["status"] != "active" or not antenna or antenna["status"] != "active":
        conflicts.append(_conflict(None, request_id, "resource_inactive", "卫星、地面站或天线不可用"))
    if station and station["weather"] != "clear":
        conflicts.append(_conflict(None, request_id, "weather_blocked", "天气条件不允许接收"))
    if end > _parse(request["deadline"]):
        conflicts.append(_conflict(None, request_id, "deadline_missed", "占位结束时间超过请求截止时间"))
    if satellite and window and antenna:
        max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
        if float(hold["rate_mbps"]) > max_rate:
            conflicts.append(_conflict(None, request_id, "rate_exceeded", "占位速率超过当前可用上限", max_rate_mbps=max_rate))
    duration = int((end - start).total_seconds())
    transferred = duration * float(hold["rate_mbps"]) / 8
    if transferred < float(request["data_mb"]):
        conflicts.append(_conflict(None, request_id, "insufficient_capacity", "占位时段可接收数据量不足", capacity_mb=transferred, required_mb=request["data_mb"]))
    conflicts.extend(_check_slot(conn, index=None, request_id=request_id, station_id=hold["station_id"], antenna_id=hold["antenna_id"],
                                 satellite_id=hold["satellite_id"], start_iso=hold["starts_at"], end_iso=hold["ends_at"], exclude_hold_id=hold["id"]))
    day = start.date().isoformat()
    used = _schedule_seconds(conn, hold["tenant"], hold["station_id"], day) + ledger.hold_seconds(conn, hold["tenant"], hold["station_id"], day, exclude_hold_id=hold["id"])
    quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (hold["tenant"], hold["station_id"])).fetchone()
    if quota and used + duration > quota["daily_seconds"]:
        conflicts.append(_conflict(None, request_id, "tenant_quota_exceeded", "租户当日地面站配额不足", used_seconds=used, requested_seconds=duration, limit=quota["daily_seconds"]))
    return conflicts
