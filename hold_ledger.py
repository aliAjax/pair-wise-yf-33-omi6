"""临时占位账本：持久化占位批次与条目，并维护占位对天线、同星和租户配额的占用。

本模块只做存储与占用核算（账本），不判断“能不能占位”——规则在 hold_policy.HoldPolicy。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

SCHEMA = """
CREATE TABLE IF NOT EXISTS hold_batches(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL REFERENCES stations(id),
    status TEXT NOT NULL DEFAULT 'active',
    expires_at TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    disposition_reason TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hold_items(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES hold_batches(id),
    request_id INTEGER NOT NULL REFERENCES requests(id),
    window_id INTEGER NOT NULL REFERENCES visibility_windows(id),
    antenna_id TEXT NOT NULL REFERENCES antennas(id),
    satellite_id TEXT NOT NULL REFERENCES satellites(id),
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    rate_mbps REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'held',
    schedule_id INTEGER REFERENCES schedules(id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_hold_items_one_active_per_request
    ON hold_items(request_id) WHERE status='held';
CREATE INDEX IF NOT EXISTS idx_hold_items_batch ON hold_items(batch_id);
CREATE INDEX IF NOT EXISTS idx_hold_batches_status ON hold_batches(status);
"""

BATCH_ACTIVE = "active"
BATCH_CONVERTED = "converted"
BATCH_RELEASED = "released"
BATCH_CANCELED = "canceled"
ITEM_HELD = "held"
ITEM_CONVERTED = "converted"
ITEM_RELEASED = "released"
ITEM_CANCELED = "canceled"
ACTIVE_SCHEDULE_STATUSES = ("scheduled", "receiving")


class HoldLedger:
    """占位账本：所有方法都接收连接，事务由服务层统一管理。"""

    def __init__(self, repo: Any):
        self.repo = repo
        repo.conn.executescript(SCHEMA)

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    # ---- 只读视图（供判定层与服务层使用）-----------------------------------

    def get_request(self, conn: sqlite3.Connection, request_id: int) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def get_window(self, conn: sqlite3.Connection, window_id: int) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone())

    def get_antenna(self, conn: sqlite3.Connection, antenna_id: str) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone())

    def get_station(self, conn: sqlite3.Connection, station_id: str) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone())

    def get_satellite(self, conn: sqlite3.Connection, satellite_id: str) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM satellites WHERE id=?", (satellite_id,)).fetchone())

    def get_quota(self, conn: sqlite3.Connection, tenant: str, station_id: str) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station_id)).fetchone())

    def find_maintenance(self, conn: sqlite3.Connection, station_id: str, antenna_id: str,
                         end_iso: str, start_iso: str) -> dict[str, Any] | None:
        return self._dict(conn.execute(
            "SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?",
            (station_id, antenna_id, end_iso, start_iso)).fetchone())

    def active_schedule_rows(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute("""SELECT s.id,s.station_id,s.antenna_id,s.satellite_id,s.starts_at,s.ends_at,r.tenant
                               FROM schedules s JOIN requests r ON r.id=s.request_id
                               WHERE s.status IN ('scheduled','receiving')""").fetchall()
        return [dict(r) for r in rows]

    def active_hold_rows(self, conn: sqlite3.Connection, exclude_batch: int | None = None) -> list[dict[str, Any]]:
        sql = """SELECT hi.id AS item_id,hi.batch_id,hi.request_id,hb.station_id,hi.antenna_id,
                        hi.satellite_id,hi.starts_at,hi.ends_at,r.tenant
                 FROM hold_items hi
                 JOIN hold_batches hb ON hb.id=hi.batch_id
                 JOIN requests r ON r.id=hi.request_id
                 WHERE hb.status='active' AND hi.status='held'"""
        args: list[Any] = []
        if exclude_batch is not None:
            sql += " AND hi.batch_id!=?"; args.append(exclude_batch)
        return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def antenna_hold(self, conn: sqlite3.Connection, antenna_id: str, end_iso: str, start_iso: str) -> dict[str, Any] | None:
        return self._dict(conn.execute("""SELECT hi.id AS hold_item_id,hi.batch_id,hi.request_id
                                          FROM hold_items hi JOIN hold_batches hb ON hb.id=hi.batch_id
                                          WHERE hb.status='active' AND hi.status='held'
                                            AND hi.antenna_id=? AND hi.starts_at<? AND hi.ends_at>?""",
                                       (antenna_id, end_iso, start_iso)).fetchone())

    def satellite_hold(self, conn: sqlite3.Connection, satellite_id: str, end_iso: str, start_iso: str) -> dict[str, Any] | None:
        return self._dict(conn.execute("""SELECT hi.id AS hold_item_id,hi.batch_id,hi.request_id,hi.antenna_id,hb.station_id
                                          FROM hold_items hi JOIN hold_batches hb ON hb.id=hi.batch_id
                                          WHERE hb.status='active' AND hi.status='held'
                                            AND hi.satellite_id=? AND hi.starts_at<? AND hi.ends_at>?""",
                                       (satellite_id, end_iso, start_iso)).fetchone())

    def used_seconds(self, conn: sqlite3.Connection, tenant: str, station_id: str, day: str) -> tuple[int, int]:
        """返回 (正式排程已用秒, 活动占位已用秒)。"""
        scheduled = conn.execute("""SELECT COALESCE(SUM((julianday(s.ends_at)-julianday(s.starts_at))*86400),0)
                                    FROM schedules s JOIN requests r ON r.id=s.request_id
                                    WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=?
                                      AND s.status IN ('scheduled','receiving','received')""",
                                 (tenant, station_id, day)).fetchone()[0]
        held = conn.execute("""SELECT COALESCE(SUM((julianday(hi.ends_at)-julianday(hi.starts_at))*86400),0)
                               FROM hold_items hi JOIN hold_batches hb ON hb.id=hi.batch_id
                               JOIN requests r ON r.id=hi.request_id
                               WHERE hb.status='active' AND hi.status='held'
                                 AND r.tenant=? AND hb.station_id=? AND substr(hi.starts_at,1,10)=?""",
                            (tenant, station_id, day)).fetchone()[0]
        return int(scheduled), int(held)

    # ---- 批次/条目查询 ------------------------------------------------------

    def get_batch(self, conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
        return self._dict(conn.execute("SELECT * FROM hold_batches WHERE id=?", (batch_id,)).fetchone())

    def list_batches(self, conn: sqlite3.Connection, status: str | None = None,
                     station_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM hold_batches"; args: list[Any] = []
        where = []
        if status: where.append("status=?"); args.append(status)
        if station_id: where.append("station_id=?"); args.append(station_id)
        if where: sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC"
        return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def list_items(self, conn: sqlite3.Connection, batch_id: int) -> list[dict[str, Any]]:
        rows = conn.execute("""SELECT hi.*,r.tenant,r.data_mb,r.status AS request_status,r.deadline
                               FROM hold_items hi JOIN requests r ON r.id=hi.request_id
                               WHERE hi.batch_id=? ORDER BY hi.id""", (batch_id,)).fetchall()
        return [dict(r) for r in rows]

    def detail(self, conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
        batch = self.get_batch(conn, batch_id)
        if not batch: return None
        return {"batch": batch, "items": self.list_items(conn, batch_id)}

    # ---- 写入 ---------------------------------------------------------------

    def insert_batch(self, conn: sqlite3.Connection, station_id: str, expires_iso: str, note: str,
                     items: list[dict[str, Any]], actor: str, now_iso: str) -> int:
        cur = conn.execute("""INSERT INTO hold_batches(station_id,status,expires_at,note,created_by,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?)""",
                           (station_id, BATCH_ACTIVE, expires_iso, note, actor, now_iso, now_iso))
        batch_id = cur.lastrowid
        for it in items:
            conn.execute("""INSERT INTO hold_items(batch_id,request_id,window_id,antenna_id,satellite_id,
                                                   starts_at,ends_at,rate_mbps,status,created_at,updated_at)
                            VALUES(?,?,?,?,?,?,?,?, 'held', ?,?)""",
                         (batch_id, it["request_id"], it["window_id"], it["antenna_id"], it["satellite_id"],
                          it["starts_at"], it["ends_at"], float(it["rate_mbps"]), now_iso, now_iso))
            conn.execute("UPDATE requests SET status='held' WHERE id=?", (it["request_id"],))
        return batch_id

    def mark_canceled(self, conn: sqlite3.Connection, batch_id: int, reason: str, now_iso: str) -> None:
        ids = [r["request_id"] for r in conn.execute(
            "SELECT request_id FROM hold_items WHERE batch_id=? AND status='held'", (batch_id,)).fetchall()]
        for rid in ids:
            conn.execute("UPDATE requests SET status='pending' WHERE id=? AND status='held'", (rid,))
        conn.execute("UPDATE hold_items SET status=?,updated_at=? WHERE batch_id=? AND status='held'",
                     (ITEM_CANCELED, now_iso, batch_id))
        conn.execute("UPDATE hold_batches SET status=?,disposition_reason=?,updated_at=? WHERE id=?",
                     (BATCH_CANCELED, reason, now_iso, batch_id))

    def mark_converted(self, conn: sqlite3.Connection, batch_id: int, schedule_ids: dict[int, int], now_iso: str) -> None:
        """schedule_ids: {request_id: schedule_id}。"""
        for request_id, schedule_id in schedule_ids.items():
            conn.execute("UPDATE hold_items SET status=?,schedule_id=?,updated_at=? WHERE batch_id=? AND request_id=? AND status='held'",
                         (ITEM_CONVERTED, schedule_id, now_iso, batch_id, request_id))
        conn.execute("UPDATE hold_batches SET status=?,updated_at=? WHERE id=?",
                     (BATCH_CONVERTED, now_iso, batch_id))

    def sweep_expired(self, conn: sqlite3.Connection, now_iso: str) -> list[int]:
        """释放到期未转换的活动批次，请求退回待排程。"""
        rows = conn.execute("SELECT id FROM hold_batches WHERE status=? AND expires_at<=?",
                            (BATCH_ACTIVE, now_iso)).fetchall()
        released = []
        for row in rows:
            batch_id = row["id"]
            request_ids = [r["request_id"] for r in conn.execute(
                "SELECT request_id FROM hold_items WHERE batch_id=? AND status='held'", (batch_id,)).fetchall()]
            for rid in request_ids:
                conn.execute("UPDATE requests SET status='pending' WHERE id=? AND status='held'", (rid,))
            conn.execute("UPDATE hold_items SET status=?,updated_at=? WHERE batch_id=? AND status='held'",
                         (ITEM_RELEASED, now_iso, batch_id))
            conn.execute("UPDATE hold_batches SET status=?,disposition_reason=?,updated_at=? WHERE id=?",
                         (BATCH_RELEASED, "expired", now_iso, batch_id))
            conn.execute("INSERT INTO audit_log(request_id,schedule_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                         (None, None, "system", "system", "hold_expired",
                          json.dumps({"batch_id": batch_id, "request_ids": request_ids}, ensure_ascii=False, sort_keys=True),
                          now_iso))
            released.append(batch_id)
        return released
