# 基准站遥测连续性

该 Go 后端服务处理站点观测、来源序号和恢复规则。PostgreSQL 16 负责保存不可变事件与当前投影，脱敏样例不对应真实站点。

```bash
go test ./...
docker compose config
docker compose build
```
