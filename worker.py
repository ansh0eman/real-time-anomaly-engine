import os
import json
import asyncio
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
import database
from sklearn.ensemble import IsolationForest
import numpy as np
import httpx
from datetime import datetime

API_URL = os.getenv("API_URL", "http://web_api:8000")

async def train_model_or_fallback(pool):
    # Fetch historical data (last 10000 to 50000 rows as per requirements)
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT request_volume, error_rate, latency_ms FROM streamed_metrics ORDER BY timestamp DESC LIMIT 50000"
        )
    
    if len(records) > 1000:
        data = np.array([[float(r['request_volume']), float(r['error_rate']), float(r['latency_ms'])] for r in records], dtype=np.float64)
        # n_estimators=100 and contamination=0.01 as required
        model = IsolationForest(contamination=0.01, n_estimators=100, random_state=42)
        model.fit(data)
        return model, None
    else:
        # Fallback to rolling statistical calculation (Z-Score)
        if len(records) > 0:
            data = np.array([[float(r['request_volume']), float(r['error_rate']), float(r['latency_ms'])] for r in records], dtype=np.float64)
            mean = np.mean(data, axis=0)
            std = np.std(data, axis=0)
            # Avoid division by zero
            std[std == 0] = 1.0
            return None, (mean, std)
        else:
            # No data at all, provide a dummy stat
            return None, (np.zeros(3), np.ones(3))

async def process_stream():
    await database.create_pool()
    pool = database.get_pool()
    
    # In redis-py 8.x, socket_timeout MUST be None for blocking commands like
    # xreadgroup. A finite socket_timeout fires before the block window completes
    # and raises TimeoutError. Use socket_connect_timeout only for connection setup.
    redis_pool = ConnectionPool.from_url(
        os.getenv("REDIS_URL", "redis://redis:6379/0"),
        decode_responses=True,
        socket_timeout=None,
        socket_connect_timeout=5,
    )
    redis_client = Redis(connection_pool=redis_pool)
    
    # Try to create consumer group (ignore if already exists)
    try:
        await redis_client.xgroup_create("metrics_stream", "worker_group", mkstream=True)
    except Exception:
        pass

    model, stats = await train_model_or_fallback(pool)
    
    # Background cron job to retrain model every hour (3600 seconds)
    async def retrain_task():
        nonlocal model, stats
        while True:
            await asyncio.sleep(3600)
            try:
                model, stats = await train_model_or_fallback(pool)
                print("Model retrained successfully.", flush=True)
            except Exception as e:
                print(f"Failed to retrain model: {e}", flush=True)
            
    asyncio.create_task(retrain_task())

    async with httpx.AsyncClient() as http_client:
        while True:
            # Read from stream using consumer group
            try:
                messages = await redis_client.xreadgroup(
                    "worker_group",
                    "worker_consumer",
                    {"metrics_stream": ">"},
                    count=100,
                    block=5000
                )
            except Exception as e:
                print(f"Redis connection error: {e}", flush=True)
                await asyncio.sleep(5)
                continue
            
            if not messages:
                continue
                
            for stream, entries in messages:
                for message_id, message_data in entries:
                    try:
                        payload = json.loads(message_data['payload'])
                        metrics = payload['metrics']
                        
                        features = np.array([metrics['request_volume'], metrics['error_rate'], metrics['latency_ms']])
                        
                        is_anomaly = False
                        anomaly_score = 0.0
                        flagged_features = []
                        
                        if model is not None:
                            pred = model.predict([features])
                            score = model.score_samples([features])[0]
                            anomaly_score = float(score)
                            is_anomaly = pred[0] == -1
                            if is_anomaly:
                                # Isolation forest doesn't give specific features easily, 
                                # we could just flag 'all' or do additional logic. 
                                # For now, we flag the whole metric set.
                                flagged_features = ['request_volume', 'error_rate', 'latency_ms']
                        else:
                            # Z-Score fallback
                            mean, std = stats
                            z_scores = (features - mean) / std
                            if np.any(np.abs(z_scores) > 3):
                                is_anomaly = True
                                anomaly_score = min(float(np.max(np.abs(z_scores))), 9.9999)
                                feature_names = ['request_volume', 'error_rate', 'latency_ms']
                                flagged_features = [feature_names[i] for i, z in enumerate(z_scores) if abs(z) > 3]
                        
                        # Use raw SQL queries utilizing asyncpg
                        async with pool.acquire() as conn:
                            # 1. Insert into streamed_metrics
                            timestamp = datetime.fromisoformat(payload['timestamp'])
                            
                            metric_id = await conn.fetchval(
                                """
                                INSERT INTO streamed_metrics 
                                (transaction_id, client_id, timestamp, request_volume, error_rate, latency_ms, channel, region)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                RETURNING id
                                """,
                                payload['transaction_id'], payload['client_id'], timestamp,
                                metrics['request_volume'], metrics['error_rate'], metrics['latency_ms'],
                                payload['channel'], payload['region']
                            )
                            
                            # 2. Insert anomaly and broadcast if necessary
                            if is_anomaly:
                                await conn.execute(
                                    """
                                    INSERT INTO detected_anomalies 
                                    (metric_id, timestamp, client_id, anomaly_score, flagged_features)
                                    VALUES ($1, $2, $3, $4, $5)
                                    """,
                                    metric_id, timestamp, payload['client_id'], anomaly_score, flagged_features
                                )
                                
                                alert_data = {
                                    "metric_id": metric_id,
                                    "timestamp": payload['timestamp'],
                                    "client_id": payload['client_id'],
                                    "anomaly_score": anomaly_score,
                                    "flagged_features": flagged_features,
                                    "message": "Anomaly detected!"
                                }
                                try:
                                    await http_client.post(f"{API_URL}/api/internal/broadcast_alert", json=alert_data)
                                except Exception as e:
                                    print(f"Failed to broadcast alert: {e}", flush=True)
                        
                        # 3. Acknowledge message
                        await redis_client.xack("metrics_stream", "worker_group", message_id)
                        
                    except Exception as e:
                        print(f"Error processing message {message_id}: {e}", flush=True)

if __name__ == "__main__":
    asyncio.run(process_stream())
