-- 不可变事件:所有入账观测只追加不修改;event_id 全局唯一保证重送不重复入账,
-- (station_id, source_id, source_seq) 唯一保证两个接收端争用同一序号时只保留一条。
CREATE TABLE telemetry_events (
    id             BIGSERIAL PRIMARY KEY,
    event_id       TEXT        NOT NULL UNIQUE,
    station_id     TEXT        NOT NULL,
    source_id      TEXT        NOT NULL,
    source_seq     BIGINT      NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL,
    metrics        JSONB       NOT NULL,
    qualified      BOOLEAN     NOT NULL,
    late_by_time   BOOLEAN     NOT NULL,
    classification TEXT        NOT NULL CHECK (classification IN ('projection', 'backfill')),
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (station_id, source_id, source_seq)
);

-- 事件流续读与按来源过滤。
CREATE INDEX telemetry_events_stream_idx ON telemetry_events (station_id, source_id, id);

-- 来源状态:每个 (station, source) 一行的连续水位与恢复进度。
CREATE TABLE source_state (
    station_id           TEXT        NOT NULL,
    source_id            TEXT        NOT NULL,
    status               TEXT        NOT NULL CHECK (status IN ('healthy', 'suspect', 'offline')),
    status_reason        TEXT        NOT NULL DEFAULT '',
    status_seq           BIGINT,
    status_since         TIMESTAMPTZ,
    watermark            BIGINT      NOT NULL DEFAULT 0,
    contiguous_watermark BIGINT      NOT NULL DEFAULT 0,
    max_seen_seq         BIGINT      NOT NULL DEFAULT 0,
    streak               INT         NOT NULL DEFAULT 0,
    last_seq             BIGINT,
    last_observed_at     TIMESTAMPTZ,
    last_received_at     TIMESTAMPTZ,
    recovered_seq        BIGINT,
    recovered_at         TIMESTAMPTZ,
    last_backfill_seq    BIGINT,
    last_backfill_at     TIMESTAMPTZ,
    revision             BIGINT      NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, source_id)
);

-- 缺口集合:逐条记录仍待补齐的序号,交班查询时聚合成区间。
CREATE TABLE source_gaps (
    station_id TEXT        NOT NULL,
    source_id  TEXT        NOT NULL,
    seq        BIGINT      NOT NULL,
    opened_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, source_id, seq)
);

-- 站点投影:与事件、缺口在同一事务内刷新,交班接口直接读取。
CREATE TABLE station_projection (
    station_id TEXT        PRIMARY KEY,
    status     TEXT        NOT NULL CHECK (status IN ('healthy', 'suspect', 'offline')),
    basis      JSONB       NOT NULL,
    revision   BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
