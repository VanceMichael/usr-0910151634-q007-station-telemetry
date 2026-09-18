-- 基准站遥测连续性服务：初始 schema
-- 所有写入（事件 / 水位 / 缺口 / 站点投影）都在单一事务内完成。

-- 迁移账本：由应用在启动时维护，保证每条迁移只执行一次。
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 不可变遥测事件。只追加，不更新、不删除。
CREATE TABLE IF NOT EXISTS telemetry_events (
    id             BIGSERIAL PRIMARY KEY,           -- 日志游标：事件流断点续读以此为准
    event_id       TEXT NOT NULL,                   -- 上游幂等键，重送不重复入账
    station_id     TEXT NOT NULL,
    source_id      TEXT NOT NULL,
    source_seq     BIGINT NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL,
    is_qualified   BOOLEAN NOT NULL,
    quality_status TEXT NOT NULL,                   -- qualified | low_satellites | high_pdop | metrics_missing
    is_late        BOOLEAN NOT NULL DEFAULT FALSE,  -- observed_at 与 received_at 差距超过 allowed_lateness_seconds
    payload        JSONB NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 两个接收端争用同一 (站点, 来源, 序号) 时，数据库只保留一条。
    CONSTRAINT telemetry_events_event_uniq UNIQUE (event_id),
    CONSTRAINT telemetry_events_seq_uniq UNIQUE (station_id, source_id, source_seq)
);

CREATE INDEX IF NOT EXISTS telemetry_events_station_observed_idx
    ON telemetry_events (station_id, observed_at);

-- 每个来源各自维护的连续水位。
-- contiguous_seq：从 1 开始无缺口连续到账的最大序号（0 表示尚无样本）。
-- highest_seen  ：实际见过的最大序号，仅用于可观测性。
CREATE TABLE IF NOT EXISTS source_watermarks (
    station_id     TEXT NOT NULL,
    source_id      TEXT NOT NULL,
    contiguous_seq BIGINT NOT NULL DEFAULT 0,
    highest_seen   BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT source_watermarks_pkey PRIMARY KEY (station_id, source_id)
);

-- 缺口集合：一行一个待补序号；补齐时回填 filled_*，保留审计痕迹。
CREATE TABLE IF NOT EXISTS sequence_gaps (
    station_id          TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    missing_seq         BIGINT NOT NULL,
    first_seen_event_id TEXT NOT NULL,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    filled_event_id     TEXT,
    filled_at           TIMESTAMPTZ,
    CONSTRAINT sequence_gaps_pkey PRIMARY KEY (station_id, source_id, missing_seq)
);

CREATE INDEX IF NOT EXISTS sequence_gaps_open_idx
    ON sequence_gaps (station_id, source_id, missing_seq)
    WHERE filled_event_id IS NULL;

-- 站点当前投影：随每个“向前的”新样本更新；晚到历史样本不回退、不推进。
CREATE TABLE IF NOT EXISTS station_projection (
    station_id                  TEXT PRIMARY KEY,
    state                       TEXT NOT NULL CHECK (state IN ('healthy', 'suspect', 'offline')),
    state_reason                TEXT NOT NULL DEFAULT '',
    revision                    BIGINT NOT NULL DEFAULT 0,
    healthy_streak              INTEGER NOT NULL DEFAULT 0,

    -- 当前连续合格样本段的起点（用于说明“从哪个样本开始计数”）
    streak_start_event_id       TEXT,
    streak_start_source_id      TEXT,
    streak_start_seq            BIGINT,
    streak_start_observed_at    TIMESTAMPTZ,

    -- 已处理到的观测时间线表头（晚于它的样本才允许影响投影）
    last_event_id               TEXT,
    last_source_id              TEXT,
    last_seq                    BIGINT,
    last_observed_at            TIMESTAMPTZ,

    last_qualified_event_id     TEXT,
    last_qualified_source_id    TEXT,
    last_qualified_seq          BIGINT,
    last_qualified_observed_at  TIMESTAMPTZ,

    last_quality_status         TEXT,
    offline_since               TIMESTAMPTZ,

    -- 最近一次恢复结论：恢复“真正发生”在哪个样本
    recovered_event_id          TEXT,
    recovered_source_id         TEXT,
    recovered_seq               BIGINT,
    recovered_observed_at       TIMESTAMPTZ,
    recovered_streak            INTEGER NOT NULL DEFAULT 0,

    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
