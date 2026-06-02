-- Step 2: Database Schema Execution

CREATE TABLE IF NOT EXISTS streamed_metrics (
    id BIGSERIAL,
    transaction_id VARCHAR(50),
    client_id VARCHAR(50),
    timestamp TIMESTAMPTZ,
    request_volume INT,
    error_rate NUMERIC(4,3),
    latency_ms NUMERIC(7,2),
    channel VARCHAR(30),
    region VARCHAR(30),
    PRIMARY KEY (id, timestamp)
);

-- Composite index on (client_id, timestamp DESC)
CREATE INDEX IF NOT EXISTS idx_streamed_metrics_client_ts 
ON streamed_metrics (client_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS detected_anomalies (
    id BIGSERIAL PRIMARY KEY,
    metric_id BIGINT,
    timestamp TIMESTAMPTZ,
    client_id VARCHAR(50),
    anomaly_score NUMERIC(5,4),
    flagged_features TEXT[],
    resolved BOOLEAN DEFAULT FALSE
);

-- Partial index on resolved = FALSE
CREATE INDEX IF NOT EXISTS idx_detected_anomalies_unresolved 
ON detected_anomalies (resolved) 
WHERE resolved = FALSE;
