# 多站卫星地面站排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`、`/reschedule`：排程或重排被抢占请求。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`：接收状态和紧急抢占。
- `POST /api/visibility-windows/{id}/change`：窗口变化并返回受影响排程；已接收数据保留。
- `GET /api/state`、`GET /api/schedules/{id}`：权限化状态查询。

## 临时占位（值班预留天线时段）

组长可把若干**尚未排上的请求**整批交给同一地面站的天线临时占位。占位只预占资源、不产生正式排程；到期未转正式会自动释放，转换时重新核验。

- `POST /api/holds`：整批登记。body：`station_id`、`expires_at`（到期时间）、`note`、`items[]`（`request_id`/`window_id`/`antenna_id`/`starts_at`/`ends_at`/`rate_mbps`）。核验可见窗口、站点/天线/卫星状态与天气、维护、同星接收、已有排程、其他占位与批内互锁、租户配额（占位占用计入当日配额）。**任一条冲突则整批不入库**，409 返回每条请求的全部冲突原因。
- `GET /api/holds`（可带 `status`、`station_id` 过滤）、`GET /api/holds/{id}`：查询；requester 只能看到本租户条目。查询会顺带惰性清扫到期批次。
- `POST /api/holds/{id}/cancel`：取消占位（需 `reason`），请求退回 `pending`。requester 只能取消仅含本租户请求的批次。
- `POST /api/holds/{id}/convert`：整批转正式排程，**转换前重新核验全部规则**；核验失败则 409 并**保留占位**，成功后请求与占位分别变为 `scheduled`/`converted`。
- `POST /api/holds/sweep`：手动释放所有到期未转换的批次（建/查/转时也会自动清扫）。

占位的三层实现分开维护：判定规则在 `hold_policy.py`（只读、不写库），持久化与占用核算在 `hold_ledger.py`（批次/条目账本，含占用索引、配额核算、到期释放），HTTP 入口在 `app.py`。请求处于占位期间状态为 `held`，期间不能直接排程；正式排程也会避让活动占位（`antenna_hold_conflict`、`satellite_hold_conflict`）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
