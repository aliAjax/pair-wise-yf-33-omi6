"""临时占位判定层：核验一批占位请求是否成立，只读取账本视图，不做任何写入。

核验项（需求）：请求状态、可见窗口、设备/站点状态与天气、维护、同星接收、
已有排程、租户配额、批内时段互锁。任何一条不通过则整批不入库，
返回每条请求的全部冲突原因，由服务层决定 409 响应。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from hold_ledger import HoldLedger


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def overlap(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    return a_start < b_end and b_start < a_end


class HoldPolicy:
    def __init__(self, ledger: HoldLedger):
        self.ledger = ledger

    def validate(self, conn: Any, station_id: str, candidates: list[dict[str, Any]],
                 exclude_batch: int | None = None) -> dict[str, Any]:
        """candidates: [{request_id, window_id, antenna_id, starts_at, ends_at, rate_mbps}]（已解析时间）。

        通过时返回 {"ok": True, "normalized": [...]}；否则
        {"ok": False, "conflicts": [{"request_id", "reasons": [{"code","message","details"}], ...}]}。
        """
        existing_schedules = self.ledger.active_schedule_rows(conn)
        existing_holds = self.ledger.active_hold_rows(conn, exclude_batch=exclude_batch)
        # station -> antenna -> 占用区间；station -> satellite -> 占用区间（站点级 + 全网站点级，分别索引）
        antenna_busy: dict[tuple[str, str], list[dict[str, Any]]] = {}
        satellite_busy: dict[str, list[dict[str, Any]]] = {}
        for row in existing_schedules + existing_holds:
            antenna_busy.setdefault((row["station_id"], row["antenna_id"]), []).append(row)
            satellite_busy.setdefault(row["satellite_id"], []).append(row)

        station = self.ledger.get_station(conn, station_id)
        if not station:
            return {"ok": False, "global": [{"code": "station_not_found", "message": "地面站不存在"}]}

        request_ids = [c["request_id"] for c in candidates]
        if len(set(request_ids)) != len(request_ids):
            return {"ok": False, "global": [{"code": "duplicate_request_in_batch", "message": "同一请求在批次中重复出现"}]}

        normalized: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        # 批内已占用（先检查后登记）
        batch_antenna: dict[str, list[dict[str, Any]]] = {}
        batch_satellite: dict[str, list[dict[str, Any]]] = {}
        quota_tally: dict[tuple[str, str], int] = {}  # (tenant, day) -> 批内秒数

        for c in candidates:
            reasons: list[dict[str, Any]] = []
            request = self.ledger.get_request(conn, c["request_id"])
            window = self.ledger.get_window(conn, c["window_id"])
            antenna = self.ledger.get_antenna(conn, c["antenna_id"])
            start_iso, end_iso = c["starts_at_iso"], c["ends_at_iso"]
            duration = int((c["ends_at"] - c["starts_at"]).total_seconds())

            if not request:
                reasons.append(self._reason("request_not_found", "数据请求不存在"))
            elif request["status"] not in {"pending", "preempted", "held"}:
                reasons.append(self._reason("request_not_holdable", f"请求状态 {request['status']} 不能占位",
                                            {"status": request["status"]}))

            if not window:
                reasons.append(self._reason("window_not_found", "可见窗口不存在"))
            if not antenna:
                reasons.append(self._reason("antenna_not_found", "天线不存在"))
            elif antenna["station_id"] != station_id:
                reasons.append(self._reason("antenna_station_mismatch", "天线不属于该站"))

            if request and window and request["satellite_id"] != window["satellite_id"]:
                reasons.append(self._reason("window_mismatch", "窗口不属于该请求的卫星"))
            if window and window["station_id"] != station_id:
                reasons.append(self._reason("window_station_mismatch", "可见窗口不属于目标地面站"))

            satellite = self.ledger.get_satellite(conn, request["satellite_id"]) if request else None
            if request and not satellite:
                reasons.append(self._reason("satellite_not_found", "卫星不存在"))

            # 资源状态与天气
            if satellite and satellite["status"] != "active":
                reasons.append(self._reason("resource_inactive", "卫星不可用", {"satellite_status": satellite["status"]}))
            if station["status"] != "active":
                reasons.append(self._reason("resource_inactive", "地面站不可用", {"station_status": station["status"]}))
            if antenna and antenna["status"] != "active":
                reasons.append(self._reason("resource_inactive", "天线不可用", {"antenna_status": antenna["status"]}))
            if station["weather"] != "clear":
                reasons.append(self._reason("weather_blocked", f"站点天气 {station['weather']} 不允许接收",
                                            {"weather": station["weather"]}))

            # 窗口边界、截止时间
            if window:
                w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
                if c["starts_at"] < w_start or c["ends_at"] > w_end:
                    reasons.append(self._reason("outside_visibility", "占位时段超出可见窗口",
                                                {"window_start": window["starts_at"], "window_end": window["ends_at"]}))
            if request and c["ends_at"] > parse_time(request["deadline"]):
                reasons.append(self._reason("deadline_missed", "预计结束时间超过请求截止时间"))

            # 速率与容量
            max_rate = None
            if satellite and window and antenna:
                max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]),
                               float(antenna["max_rate_mbps"]))
                if float(c["rate_mbps"]) > max_rate:
                    reasons.append(self._reason("rate_exceeded", "占位速率超过可用上限",
                                                {"max_rate_mbps": max_rate, "requested_mbps": float(c["rate_mbps"])}))
            if duration > 0 and float(c["rate_mbps"]) > 0:
                capacity_mb = duration * float(c["rate_mbps"]) / 8
                if request and capacity_mb < float(request["data_mb"]):
                    reasons.append(self._reason("insufficient_capacity", "占位时段内可接收数据量不足",
                                                {"capacity_mb": capacity_mb, "required_mb": float(request["data_mb"])}))

            # 维护
            mt = self.ledger.find_maintenance(conn, station_id, c["antenna_id"], end_iso, start_iso)
            if mt:
                reasons.append(self._reason("maintenance_conflict", "天线或地面站处于维护期",
                                            {"maintenance_id": mt["id"], "reason": mt["reason"],
                                             "starts_at": mt["starts_at"], "ends_at": mt["ends_at"]}))

            # 已有排程：天线
            for row in antenna_busy.get((station_id, c["antenna_id"]), []):
                if overlap(start_iso, end_iso, row["starts_at"], row["ends_at"]):
                    if "id" in row:
                        reasons.append(self._reason("antenna_conflict", "天线时段已被正式排程占用",
                                                    {"schedule_id": row["id"], "starts_at": row["starts_at"],
                                                     "ends_at": row["ends_at"]}))
                    else:
                        reasons.append(self._reason("antenna_hold_conflict", "天线时段已被其他占位保留",
                                                    {"hold_batch_id": row["batch_id"], "hold_item_id": row["item_id"],
                                                     "request_id": row["request_id"]}))
                    break
            # 已有排程：同星（任意站，同站也算）
            if satellite:
                for row in satellite_busy.get(request["satellite_id"], []):
                    if overlap(start_iso, end_iso, row["starts_at"], row["ends_at"]):
                        if "id" in row:
                            reasons.append(self._reason("satellite_conflict", "同一卫星时段已被其他站正式接收",
                                                        {"schedule_id": row["id"], "station_id": row["station_id"]}))
                        else:
                            reasons.append(self._reason("satellite_hold_conflict", "同一卫星时段已被其他占位保留",
                                                        {"hold_batch_id": row["batch_id"], "station_id": row["station_id"],
                                                         "request_id": row["request_id"]}))
                        break

            # 批内互锁
            for row in batch_antenna.get(c["antenna_id"], []):
                if overlap(start_iso, end_iso, row["starts_at"], row["ends_at"]):
                    reasons.append(self._reason("batch_antenna_conflict", "批次内两条占位争抢同一天线",
                                                {"other_request_id": row["request_id"]}))
                    break
            if request:
                for row in batch_satellite.get(request["satellite_id"], []):
                    if overlap(start_iso, end_iso, row["starts_at"], row["ends_at"]):
                        reasons.append(self._reason("batch_satellite_conflict", "批次内同一卫星时段重复接收",
                                                    {"other_request_id": row["request_id"]}))
                        break

            # 配额（仅对能确定租户的条目核算；无配额记录视为不限）
            if request and duration > 0:
                tenant = request["tenant"]
                day = start_iso[:10]
                quota = self.ledger.get_quota(conn, tenant, station_id)
                if quota:
                    used_sched, used_hold = self.ledger.used_seconds(conn, tenant, station_id, day)
                    batch_used = quota_tally.get((tenant, day), 0)
                    if used_sched + used_hold + batch_used + duration > int(quota["daily_seconds"]):
                        reasons.append(self._reason("tenant_quota_exceeded", "租户当日地面站配额不足",
                                                    {"tenant": tenant, "used_seconds": used_sched,
                                                     "held_seconds": used_hold, "batch_seconds": batch_used,
                                                     "requested_seconds": duration,
                                                     "limit": int(quota["daily_seconds"])}))

            entry = {"request_id": c["request_id"], "window_id": c["window_id"], "antenna_id": c["antenna_id"],
                     "satellite_id": request["satellite_id"] if request else (window["satellite_id"] if window else None),
                     "starts_at": start_iso, "ends_at": end_iso, "rate_mbps": float(c["rate_mbps"]),
                     "duration_seconds": duration}
            if reasons:
                conflicts.append({"request_id": c["request_id"], "reasons": reasons})
            else:
                normalized.append(entry)
                batch_antenna.setdefault(c["antenna_id"], []).append(entry)
                if request:
                    batch_satellite.setdefault(request["satellite_id"], []).append(entry)
                    quota_tally[(request["tenant"], start_iso[:10])] = quota_tally.get((request["tenant"], start_iso[:10]), 0) + duration

        if conflicts:
            return {"ok": False, "conflicts": conflicts}
        return {"ok": True, "normalized": normalized}

    @staticmethod
    def _reason(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
        r = {"code": code, "message": message}
        if details: r["details"] = details
        return r
