# 基准站遥测连续性服务

纯后端服务（Go 1.25 + PostgreSQL 16）：接收全国基准站各链路观测，维护**每个来源独立的连续水位**与**站点当前投影**，
交班接口直接给出“当前状态依据”与“仍待补齐的序号区间”，避免把几分钟前的正常数据误判为站点刚刚恢复。

## 核心语义

- **不可变事件**：`telemetry_events` 只追加；`event_id` 唯一（重送返回 `duplicate_event`，不重复入账），
  `(station_id, source_id, source_seq)` 唯一——两个接收端争用同一序号时数据库只保留一条，负方收到
  `409 conflict_seq` 与获胜 `winner_event_id`。
- **来源水位**：各来源分别维护 `contiguous_seq`（连续到账前缀）与 `highest_seen`；水位只进不退。
  跳号到达登记缺口，晚到补传回填缺口（保留 `first_seen/filled` 审计痕迹）。
- **投影不被历史回退**：只有 `observed_at` 严格新于当前表头的样本才参与投影；跨链路晚到观测进入事件表、
  回填缺口、推进水位，但回执 `applied_to_projection=false`，当前状态不变。
- **恢复契约（`contracts/recovery-policy.json`）**：`healthy_streak=3`、质量门槛 `satellites>=4`、
  `pdop<=6.0`，观测静默 `90s` 翻 `offline`，晚到窗口 `300s`。
  suspect/offline 必须**重新取得 3 个连续合格样本**才回 healthy；序号跳号或时间断裂都会使旧连续段作废。
  恢复锚点精确记录“达到阈值的那个样本”（`recovered_at_sample`）。
- **单事务落地**：事件、水位、缺口集合、站点投影在同一个数据库事务内提交（`internal/store/store.go`）。
- **可断点续读事件流**：`GET /v1/events?after=<id>&limit=n` 以自增 id 为游标，返回 `next_cursor/has_more`。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/events` | 接收一帧遥测 |
| GET  | `/v1/stations/{station_id}/handoff` | 交班结论：状态依据 + 各来源水位 + 待补序号区间 |
| GET  | `/v1/stations` | 全部站点当前投影 |
| GET  | `/v1/events?after=&limit=` | 事件流断点续读 |
| GET  | `/health` | 应用与数据库探活 |

```bash
curl -s localhost:8080/v1/events -H 'Content-Type: application/json' -d '{
  "event_id":"evt-1","station_id":"station-demo","source_id":"link-a","source_seq":1,
  "observed_at":"2026-09-18T06:00:00Z","received_at":"2026-09-18T06:00:02Z",
  "metrics":{"satellites":8,"pdop":1.7}}'

curl -s localhost:8080/v1/stations/station-demo/handoff
```

交班响应要点：`projection.state` / `state_basis`（锚定具体样本的白话依据）/ `recovered_at_sample`
（真正恢复发生在哪一帧）/ `sources[].contiguous_seq` / `sources[].missing_ranges`（合并后的待补闭区间）。

## 运行

```bash
docker compose up -d --build
# postgres 经 pg_isready 探活；migrate 为一次性迁移作业（service migrate），
# app 等迁移成功完成后启动；station-data 为持久卷。
```

迁移 SQL 同时随二进制 `go:embed`（应用启动幂等执行），并以符号链接暴露在仓库根 `migrations/`。

本地开发：

```bash
go test ./...
# 集成测试需要 PostgreSQL，可用 TEST_DATABASE_URL 指定；连不上时自动跳过
```

## 验收演练

`scripts/acceptance.sh` 全自动完成题目要求的验收路径：

1. seq 1..3 建立 healthy（恢复锚定第 3 个样本）；
2. **人为留下 seq 4 缺口**（seq 5 跳号到达），交班指出 `link_gap` 依据与待补区间 `[4,4]`，水位停在 3；
3. seq 4 跨链路晚到补传：入账、水位推进到 5，但**不回退/推进投影**；
4. **重启整套环境**，交班延续水位 5 与计数，未误判恢复；
5. seq 6、7 到达，**真正恢复发生在 seq 7**（跳号后第 3 个连续合格样本）；
6. event_id 重送幂等、8 路并发争用同序号只入账 1 条、事件流断点续读不重不漏、离线翻转与再恢复。

```bash
docker compose up -d --build
scripts/acceptance.sh    # 默认 BASE_URL=http://127.0.0.1:8080，重启用 docker compose restart
```

## 目录

```
contracts/recovery-policy.json     恢复策略契约（内嵌进二进制）
migrations/001_initial.sql         schema（指向 internal/store/migrations）
internal/contracts/                契约加载
internal/domain/                   质量判定与 healthy/suspect/offline 状态机
internal/sequence/                 缺口序号合并为区间
internal/store/                    事务、迁移、水位/缺口/投影、事件流
internal/httpapi/                  HTTP 接口
scripts/acceptance.sh              验收演练
```
