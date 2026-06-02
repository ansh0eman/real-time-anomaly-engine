# ⚡ Real-Time Anomalous Pattern & Outlier Engine

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis_Streams-7.0-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-Orchestrated-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-IsolationForest-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)

**A production-grade, event-driven streaming backend that ingests high-throughput metric data, detects anomalous patterns in real time using unsupervised machine learning, and broadcasts live alerts via WebSockets.**

[Architecture](#architecture) · [Quick Start](#quick-start) · [API Reference](#api-reference) · [Configuration](#configuration) · [Testing](#testing)

</div>

---

## The Problem This Solves

In high-traffic distributed systems, servers continuously emit metrics — request counts, error rates, latency — across hundreds of services. Identifying a **sudden spike, cascading failure, or attack vector** in real time, before it impacts users, is critical.

Manual threshold alerting ("alert if latency > 500ms") fails in dynamic environments where "normal" changes throughout the day. This engine uses **unsupervised machine learning** to learn what normal looks like automatically, and flags deviations without needing predefined rules.

---

## Architecture

```
                         ┌─────────────────────────────────────────────────────┐
  External Clients       │                  Docker Network                      │
  (Services, Apps,       │                                                       │
   Simulators)           │   ┌──────────────────┐      ┌──────────────────┐    │
        │                │   │   FastAPI (API)   │      │   Redis Streams  │    │
        │ POST /metrics   │   │   main.py         │─────▶│   metrics_stream │    │
        └────────────────┼──▶│   port 8000       │xadd  │                  │    │
                         │   │                   │      └────────┬─────────┘    │
        ◀────────────────┼───│   202 Accepted    │               │xreadgroup    │
        WebSocket alerts │   │   WebSocket /ws   │               │              │
        (anomaly push)   │   │   /alerts         │◀──────────────┼──────────────┼──┐
                         │   └──────────────────┘               │              │  │
                         │            ▲                          ▼              │  │
                         │            │ HTTP POST         ┌──────────────────┐  │  │
                         │            │ broadcast_alert   │ Stream Worker    │  │  │
                         │            └───────────────────│ worker.py        │  │  │
                         │                                │                  │  │  │
                         │                                │ ┌──────────────┐ │  │  │
                         │                                │ │IsolationForest│ │  │  │
                         │                                │ │Z-Score Fbck  │ │  │  │
                         │                                │ └──────────────┘ │  │  │
                         │                                └────────┬─────────┘  │  │
                         │                                         │asyncpg     │  │
                         │                                         ▼            │  │
                         │                              ┌──────────────────┐   │  │
                         │                              │   PostgreSQL 15   │   │  │
                         │                              │   streamed_metrics│   │  │
                         │                              │   detected_anomaly│   │  │
                         │                              └──────────────────┘   │  │
                         └─────────────────────────────────────────────────────┘  │
                                                                                   │
                         Connected Dashboard Clients ◀─────────────────────────────┘
```

### Core Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Strict Decoupling** | The API never touches the database or runs ML. It validates, queues, and returns `202` in < 5ms |
| **Async Everywhere** | Native Python `async`/`await` across all I/O boundaries — no blocking threads |
| **At-Least-Once Delivery** | Redis Consumer Groups with `xack` ensure no message is lost if the worker crashes |
| **Graceful Degradation** | Z-Score statistical fallback activates automatically during cold start (< 1000 rows) |
| **Hot-Swap Retraining** | Model retrains on fresh historical data every hour without stopping the stream |

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **API Framework** | FastAPI + Uvicorn (ASGI) | Native async, automatic Pydantic validation, OpenAPI docs |
| **Message Broker** | Redis 7 Streams | Durable, ordered, consumer-group-aware message queue |
| **Database** | PostgreSQL 15 + asyncpg | Native binary protocol driver, raw SQL for complex time-series CTEs |
| **ML Engine** | Scikit-Learn IsolationForest | Unsupervised outlier detection — no labeled anomaly data required |
| **Data Validation** | Pydantic v2 | Schema enforcement at the API boundary |
| **HTTP Client** | HTTPX | Async HTTP for worker→API broadcast channel |
| **Containerization** | Docker + Docker Compose | Unified multi-service local environment |

---

## Project Structure

```
proud-faraday/
│
├── main.py              # FastAPI app: ingestion endpoint + WebSocket manager
├── worker.py            # Async stream consumer: ML scoring + DB persistence
├── database.py          # asyncpg connection pool lifecycle manager
├── schemas.py           # Pydantic v2 models — the data contract
├── init.sql             # PostgreSQL DDL: tables, indexes, constraints
│
├── simulate_stream.py   # Load generator: continuous metric stream (1 req/sec)
├── test_engine.py       # Integration test: WebSocket + anomaly injection
│
├── Dockerfile           # Container build recipe (Python 3.11-slim)
├── docker-compose.yml   # 4-service orchestration: postgres, redis, api, worker
├── requirements.txt     # Python dependency manifest
└── README.md
```

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Python 3.9+ (for running local simulators)

### 1. Clone & Launch

```bash
git clone <your-repo-url>
cd proud-faraday

# Build images and start all 4 services
docker compose up --build -d
```

Docker will automatically:
1. Pull `postgres:15` and `redis:7-alpine` images
2. Build the Python app image from `Dockerfile`
3. Initialize the database schema from `init.sql`
4. Start the API and worker, waiting for healthy dependencies

### 2. Verify Everything Is Running

```bash
docker compose ps
```

```
NAME                      STATUS          PORTS
outlier_engine_api        Up (healthy)    0.0.0.0:8000->8000/tcp
outlier_engine_worker     Up
outlier_engine_postgres   Up (healthy)    0.0.0.0:5433->5432/tcp
outlier_engine_redis      Up (healthy)    0.0.0.0:6379->6379/tcp
```

### 3. Start Streaming Data

```bash
# Install simulator dependencies
pip3 install requests

# Send 1 metric per second (anomaly spike every 20 records)
python3 simulate_stream.py
```

```
🚀 Starting real-time metric stream simulator... Press Ctrl+C to stop.
[952 reqs]  Sent to API -> Status: 202   ← normal
[836 reqs]  Sent to API -> Status: 202   ← normal
[9699 reqs] Sent to API -> Status: 202   ← anomaly spike!
[1045 reqs] Sent to API -> Status: 202   ← normal
```

### 4. Monitor Live Anomaly Alerts

Open a second terminal and connect to the WebSocket stream:

```bash
# Using wscat (npm install -g wscat)
wscat -c ws://localhost:8000/ws/alerts
```

When an anomaly is detected, you receive a real-time push:

```json
{
  "metric_id": 42,
  "timestamp": "2026-06-03T00:01:00+00:00",
  "client_id": "client_alpha",
  "anomaly_score": 6.72,
  "flagged_features": ["request_volume", "error_rate", "latency_ms"],
  "message": "Anomaly detected!"
}
```

---

## API Reference

### Interactive Docs
Navigate to **`http://localhost:8000/docs`** for the auto-generated Swagger UI.

---

### `POST /api/v1/metrics`

Ingest a metric datapoint. Validated and queued into Redis Stream in < 5ms.

**Request Body:**
```json
{
  "transaction_id": "tx_abc123",
  "client_id":      "client_alpha",
  "timestamp":      "2026-06-03T00:00:00+00:00",
  "channel":        "web_frontend",
  "region":         "us-east",
  "metrics": {
    "request_volume": 1200,
    "error_rate":     0.015,
    "latency_ms":     85.5
  }
}
```

| Field | Type | Constraints |
|-------|------|-------------|
| `transaction_id` | `string` | Required, unique identifier |
| `client_id` | `string` | Required |
| `timestamp` | `ISO 8601 datetime` | Required, timezone-aware |
| `channel` | `string` | e.g. `web_frontend`, `mobile_app` |
| `region` | `string` | e.g. `us-east`, `eu-west` |
| `metrics.request_volume` | `int ≥ 0` | Request count |
| `metrics.error_rate` | `float [0.0, 1.0]` | Fraction of failed requests |
| `metrics.latency_ms` | `float ≥ 0` | Response time in milliseconds |

**Response: `202 Accepted`**
```json
{ "status": "accepted" }
```

---

### `GET /ws/alerts`

WebSocket endpoint. Connect to receive real-time anomaly push notifications.

```
ws://localhost:8000/ws/alerts
```

Multiple clients can connect simultaneously. All receive every broadcast.

---

## Testing

### Integration Test (End-to-End)

Connects to WebSocket, sends 10 normal metrics, fires a massive anomaly, and verifies the alert arrives in real time.

```bash
pip3 install httpx websockets
python3 test_engine.py
```

Expected output:
```
🔌 Connecting to WebSocket for alerts...
✅ Connected to WebSocket! Waiting for anomalies...
🚀 Starting metric ingestion...
Normal Metric Sent -> Status: 202
... (x10)
💥 Sending Anomalous Data Spike...
Anomalous Metric Sent -> Status: 202

🚨 ANOMALY DETECTED 🚨
{
  "metric_id": 11,
  "client_id": "client_A",
  "anomaly_score": 3.16,
  "flagged_features": ["request_volume", "error_rate", "latency_ms"]
}
```

### Manual curl Tests

**Normal metric:**
```bash
curl -X POST http://localhost:8000/api/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "tx_001",
    "client_id": "my_service",
    "timestamp": "2026-06-03T00:00:00+00:00",
    "channel": "web_frontend",
    "region": "us-east",
    "metrics": {"request_volume": 1000, "error_rate": 0.01, "latency_ms": 80.0}
  }'
```

**Anomaly spike:**
```bash
curl -X POST http://localhost:8000/api/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "tx_spike",
    "client_id": "my_service",
    "timestamp": "2026-06-03T00:01:00+00:00",
    "channel": "web_frontend",
    "region": "us-east",
    "metrics": {"request_volume": 9999, "error_rate": 0.85, "latency_ms": 3500.0}
  }'
```

### Inspect the Database Directly

```bash
docker exec -it outlier_engine_postgres psql -U engine_user -d outlier_db
```

```sql
-- Total metrics ingested
SELECT COUNT(*) FROM streamed_metrics;

-- Last 5 anomalies detected
SELECT id, client_id, anomaly_score, flagged_features, timestamp
FROM detected_anomalies
ORDER BY timestamp DESC
LIMIT 5;

-- All unresolved alerts
SELECT * FROM detected_anomalies WHERE resolved = FALSE;
```

---

## How the ML Pipeline Works

```
Worker starts
    │
    ▼
Query streamed_metrics: last 50,000 rows
    │
    ├── rows > 1000 ──▶ Train IsolationForest
    │                    contamination = 0.01 (1% anomaly rate)
    │                    n_estimators  = 100  (100 decision trees)
    │
    └── rows ≤ 1000 ──▶ Z-Score statistical fallback
                         flag if |z| > 3 standard deviations
    │
    ▼
For each message from Redis Stream:
    features = [request_volume, error_rate, latency_ms]
    │
    ├── IsolationForest.predict() == -1  →  anomaly
    │   score_samples() → anomaly_score (more negative = more anomalous)
    │
    └── Z-score > 3 on any feature  →  anomaly
    │
    ▼
If anomaly:
    INSERT detected_anomalies (metric_id, score, flagged_features)
    HTTP POST → /api/internal/broadcast_alert
    → WebSocket.broadcast() → all connected clients
    │
    ▼
xack message (mark as processed in Redis)
    │
    ▼
Every 3600s: retrain model on fresh data (hot-swap, zero downtime)
```

---

## Configuration

All configuration is passed via environment variables — set in `docker-compose.yml` for containers, or exported in your shell for local runs.

| Variable | Default (Docker) | Description |
|----------|-----------------|-------------|
| `POSTGRES_URL` | `postgresql://engine_user:engine_password@postgres:5432/outlier_db` | PostgreSQL DSN |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string |
| `API_URL` | `http://web_api:8000` | Worker → API broadcast URL |

### Tuning the ML Model

In `worker.py`:

```python
# Sensitivity: higher = more anomalies flagged
IsolationForest(contamination=0.01, ...)  # 0.005 → strict, 0.05 → permissive

# Accuracy vs speed trade-off
IsolationForest(..., n_estimators=100)   # Higher = more accurate, slower training

# Retrain frequency (seconds)
await asyncio.sleep(3600)  # 1800 = every 30 min, 86400 = daily

# Z-Score threshold (cold start fallback)
if np.any(np.abs(z_scores) > 3):  # Lower = more sensitive
```

---

## Database Schema

```sql
-- Time-series metric storage
CREATE TABLE streamed_metrics (
    id               BIGSERIAL,
    transaction_id   VARCHAR(50),
    client_id        VARCHAR(50),
    timestamp        TIMESTAMPTZ,          -- UTC, timezone-aware
    request_volume   INT,
    error_rate       NUMERIC(4,3),         -- 0.000 to 1.000
    latency_ms       NUMERIC(7,2),
    channel          VARCHAR(30),
    region           VARCHAR(30),
    PRIMARY KEY (id, timestamp)            -- Composite key for partitioning
);

-- Composite index: fast "give me client X's recent history" queries
CREATE INDEX idx_streamed_metrics_client_ts ON streamed_metrics (client_id, timestamp DESC);

-- Anomaly records
CREATE TABLE detected_anomalies (
    id               BIGSERIAL PRIMARY KEY,
    metric_id        BIGINT,               -- FK → streamed_metrics.id
    timestamp        TIMESTAMPTZ,
    client_id        VARCHAR(50),
    anomaly_score    NUMERIC(5,4),
    flagged_features TEXT[],               -- PostgreSQL native array type
    resolved         BOOLEAN DEFAULT FALSE
);

-- Partial index: only indexes unresolved alerts (keeps it tiny and fast)
CREATE INDEX idx_detected_anomalies_unresolved ON detected_anomalies (resolved)
WHERE resolved = FALSE;
```

---

## Stopping the Application

```bash
# Stop containers, preserve data volumes
docker compose down

# Stop AND wipe all stored data (fresh start)
docker compose down -v
```

---

## Key Technical Decisions

**Why Redis Streams over a simple pub/sub?**
Streams are durable — messages persist until explicitly acknowledged. If the worker restarts, it resumes from where it left off. Simple pub/sub would lose any messages sent while the worker was down.

**Why asyncpg over SQLAlchemy?**
This system writes raw SQL (optimized for time-series CTEs and analytical window functions). SQLAlchemy's ORM abstraction adds overhead without adding value. asyncpg uses PostgreSQL's native binary protocol — it's the fastest Python PostgreSQL driver available.

**Why Isolation Forest over threshold-based alerting?**
Static thresholds break in dynamic systems. "Alert if latency > 500ms" would flood alerts during a legitimate traffic spike. Isolation Forest learns the *shape* of normal behaviour and flags deviations from it, adapting to different time-of-day patterns and client volumes automatically.

**Why `202 Accepted` instead of `200 OK`?**
The API never processes data — it queues it. `200 OK` implies the work is done. `202 Accepted` is the correct semantic: "I received your request and will handle it asynchronously."

---

<div align="center">

Built with FastAPI · Redis Streams · PostgreSQL · Scikit-Learn · Docker

</div>
