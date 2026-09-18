# 基准站遥测连续性

纯后端遥测连续性服务(Go 1.25 + PostgreSQL 16)。接收各来源观测,按来源维护连续水位,
按合约判定恢复,并向交班接口提供"当前状态依据 + 仍待补齐的序号区间"。
脱敏样例不对应真实站点。

## 语义约定

- **不可变事件**:每条观测只追加;`event_id` 全局唯一,重送不重复入账;
  `(station_id, source_id, source_seq)` 唯一,两个接收端争用同一序号时只保留先到的一条。
- **投影推进**:新鲜(`received_at - observed_at ≤ allowed_lateness_seconds`)且序号超过
  当前水位的样本才推进投影;序号跳变会开出缺口区间并使 healthy 降级为 suspect。
- **迟到即历史**:超时迟到或序号不高于水位的观测只进历史(可补齐缺口集合),
  不回退投影,也不计入恢复进度——几分钟前的正常数据不会被误认作站点刚刚恢复。
- **恢复门槛**:suspect / offline 只有连续取得合约规定的 `healthy_streak` 个
  新鲜合格样本(卫星数、PDOP 达标)才转回 healthy,恢复样本被精确记录。
- **单事务落地**:事件、缺口集合、来源状态、站点投影在同一事务写入。
- **offline**:来源静默超过 `offline_after_seconds` 由后台清扫标记为 offline。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/observations` | 观测入账(幂等);响应给出分类 `projection`/`backfill`/`duplicate_event`/`duplicate_seq` |
| GET | `/v1/stations/{station_id}/handoff` | 交班视图:状态依据、各来源水位、待补齐区间、真正恢复发生的样本 |
| GET | `/v1/events?station_id=&source_id=&after=&limit=` | 事件流,`next_cursor` 断点续读 |
| GET | `/v1/contract` | 当前生效的恢复策略 |
| GET | `/health` | 探活(含数据库) |

## 运行

```bash
go test ./...
docker compose up -d --build   # 应用、迁移、数据库探活、持久卷
```

迁移由 `migrate` 服务(golang-migrate)在应用启动前执行;数据保存在 `station-data` 卷,
`docker compose restart` 或 `down` 后状态延续。

## 验收

```bash
./scripts/acceptance.sh
```

脚本完整走一遍:人为留下序号缺口 → 跨链路补传(只进历史)→ 连续合格样本在
样本 8 真正恢复 → 超时迟到样本不推进水位 → 幂等与序号争用 → 重启整套环境 →
交班结果延续原水位、恢复样本仍指向 8、补传不被误认 → 事件流断点续读。
