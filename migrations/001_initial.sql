CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

INSERT INTO schema_version VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS ingest_state (
    singleton INTEGER PRIMARY KEY,
    next_id BIGINT NOT NULL
);

INSERT INTO ingest_state VALUES (1, 1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS events (
    id BIGINT PRIMARY KEY,
    source VARCHAR NOT NULL,
    event_time BIGINT NOT NULL,
    received_at BIGINT NOT NULL,
    level INTEGER NOT NULL,
    service VARCHAR,
    logger VARCHAR,
    host VARCHAR,
    pid BIGINT,
    message VARCHAR NOT NULL,
    attributes VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS archives (
    path VARCHAR PRIMARY KEY,
    min_time BIGINT NOT NULL,
    max_time BIGINT NOT NULL,
    row_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS retention_state (
    singleton INTEGER PRIMARY KEY,
    before_time BIGINT NOT NULL,
    expired_through BIGINT DEFAULT 0
);

INSERT INTO retention_state (singleton, before_time)
VALUES (1, -9223372036854775808)
ON CONFLICT DO NOTHING;
