"""占位账本：holds 表结构与全部持久化操作。

所有函数只接收连接并在调用方事务内执行，不做冲突判定（判定见 validation.py）。
占位状态：active（生效）、converted（已转正式排程）、canceled（已取消）、expired（到期释放）。
"""
from __future__ import annotations

import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS holds(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    request_id INTEGER NOT NULL REFERENCES requests(id),
    window_id INTEGER NOT NULL REFERENCES visibility_windows(id),
    station_id TEXT NOT NULL REFERENCES stations(id),
    antenna_id TEXT NOT NULL REFERENCES antennas(id),
    satellite_id TEXT NOT NULL REFERENCES satellites(id),
    tenant TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    rate_mbps REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    expires_at TEXT NOT NULL,
    converted_schedule_id INTEGER REFERENCES schedules(id),
    disposition_reason TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_holds_antenna ON holds(antenna_id,status,starts_at,ends_at);
CREATE INDEX IF NOT EXISTS idx_holds_satellite ON holds(satellite_id,status,starts_at,ends_at);
CREATE INDEX IF NOT EXISTS idx_holds_request ON holds(request_id,status);
"""

ACTIVE, CONVERTED, CANCELED, EXPIRED = "active", "converted", "canceled", "expired"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def insert_batch(conn: sqlite3.Connection, *, batch_id: str, station_id: str, antenna_id: str, expires_iso: str, actor: str, now_iso: str, items: list[dict[str, Any]]) -> list[int]:
    """整批写入占位；调用方需先完成判定，任一冲突应整批不入库。"""
    ids = []
    for item in items:
        cur = conn.execute("""INSERT INTO holds(batch_id,request_id,window_id,station_id,antenna_id,satellite_id,tenant,starts_at,ends_at,rate_mbps,expires_at,created_by,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (batch_id, item["request_id"], item["window_id"], station_id, antenna_id, item["satellite_id"], item["tenant"],
                            item["start_iso"], item["end_iso"], item["rate_mbps"], expires_iso, actor, now_iso, now_iso))
        ids.append(cur.lastrowid)
    return ids


def get(conn: sqlite3.Connection, hold_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM holds WHERE id=?", (hold_id,)).fetchone()
    return dict(row) if row else None


def list_holds(conn: sqlite3.Connection, *, status: str | None = None, tenant: str | None = None, station_id: str | None = None) -> list[dict[str, Any]]:
    sql, args = "SELECT * FROM holds WHERE 1=1", []
    if status: sql += " AND status=?"; args.append(status)
    if tenant: sql += " AND tenant=?"; args.append(tenant)
    if station_id: sql += " AND station_id=?"; args.append(station_id)
    return [dict(r) for r in conn.execute(sql + " ORDER BY id DESC", args)]


def active_for_request(conn: sqlite3.Connection, request_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM holds WHERE request_id=? AND status='active' ORDER BY id DESC LIMIT 1", (request_id,)).fetchone()
    return dict(row) if row else None


def overlapping(conn: sqlite3.Connection, field: str, value: str, end_iso: str, start_iso: str, exclude_hold_id: int | None = None) -> dict[str, Any] | None:
    """返回与时段 [start_iso, end_iso) 重叠的生效占位，field 限 antenna_id / satellite_id。"""
    if field not in ("antenna_id", "satellite_id"): raise ValueError(f"不支持的占位重叠维度: {field}")
    sql = f"SELECT * FROM holds WHERE status='active' AND {field}=? AND starts_at<? AND ends_at>?"
    args: list[Any] = [value, end_iso, start_iso]
    if exclude_hold_id is not None: sql += " AND id!=?"; args.append(exclude_hold_id)
    row = conn.execute(sql + " ORDER BY id LIMIT 1", args).fetchone()
    return dict(row) if row else None


def hold_seconds(conn: sqlite3.Connection, tenant: str, station_id: str, day: str, exclude_hold_id: int | None = None) -> int:
    """生效占位在某租户-站-自然日内占用的秒数，用于配额核算。"""
    sql = "SELECT COALESCE(SUM((julianday(ends_at)-julianday(starts_at))*86400),0) FROM holds WHERE status='active' AND tenant=? AND station_id=? AND substr(starts_at,1,10)=?"
    args: list[Any] = [tenant, station_id, day]
    if exclude_hold_id is not None: sql += " AND id!=?"; args.append(exclude_hold_id)
    return int(conn.execute(sql, args).fetchone()[0])


def set_status(conn: sqlite3.Connection, hold_id: int, status: str, now_iso: str, *, reason: str | None = None, schedule_id: int | None = None) -> None:
    conn.execute("""UPDATE holds SET status=?, updated_at=?,
                    disposition_reason=COALESCE(?, disposition_reason),
                    converted_schedule_id=COALESCE(?, converted_schedule_id) WHERE id=?""",
                 (status, now_iso, reason, schedule_id, hold_id))


def release_expired(conn: sqlite3.Connection, now_iso: str) -> list[dict[str, Any]]:
    """释放所有到期未转换的生效占位，返回被释放的行供调用方写审计。"""
    rows = [dict(r) for r in conn.execute("SELECT * FROM holds WHERE status='active' AND expires_at<=?", (now_iso,))]
    for row in rows:
        conn.execute("UPDATE holds SET status='expired', disposition_reason='hold_expired', updated_at=? WHERE id=?", (now_iso, row["id"]))
    return rows
