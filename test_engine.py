import asyncio
import httpx
import websockets
import json
import random
from datetime import datetime
import time

API_URL = "http://localhost:8000/api/v1/metrics"
WS_URL = "ws://localhost:8000/ws/alerts"

async def listen_for_alerts():
    print("🔌 Connecting to WebSocket for alerts...")
    try:
        async with websockets.connect(WS_URL) as websocket:
            print("✅ Connected to WebSocket! Waiting for anomalies...\n")
            while True:
                message = await websocket.recv()
                alert = json.loads(message)
                print("🚨 ANOMALY DETECTED 🚨")
                print(json.dumps(alert, indent=2))
                print("-" * 40)
    except Exception as e:
        print(f"❌ WebSocket disconnected: {e}")

async def send_metrics():
    print("🚀 Starting metric ingestion...")
    async with httpx.AsyncClient() as client:
        # 1. Send Normal Data to build up the Z-Score / Baseline
        for i in range(10):
            payload = {
                "transaction_id": f"txn_{random.randint(1000, 9999)}",
                "client_id": "client_A",
                "timestamp": datetime.utcnow().isoformat(),
                "channel": "web",
                "region": "us-east",
                "metrics": {
                    "request_volume": random.randint(100, 150),
                    "error_rate": random.uniform(0.01, 0.05),
                    "latency_ms": random.uniform(20.0, 50.0)
                }
            }
            response = await client.post(API_URL, json=payload)
            print(f"Normal Metric Sent -> Status: {response.status_code}")
            await asyncio.sleep(0.5)

        print("\n💥 Sending Anomalous Data Spike...")
        # 2. Send Anomalous Data (Massive Spike in Latency & Errors)
        anomalous_payload = {
            "transaction_id": f"txn_spike_{random.randint(1000, 9999)}",
            "client_id": "client_A",
            "timestamp": datetime.utcnow().isoformat(),
            "channel": "web",
            "region": "us-east",
            "metrics": {
                "request_volume": 5000,           # Massive volume spike
                "error_rate": 0.85,               # 85% error rate
                "latency_ms": 3500.5              # 3.5 seconds latency
            }
        }
        
        response = await client.post(API_URL, json=anomalous_payload)
        print(f"Anomalous Metric Sent -> Status: {response.status_code}")
        
        # Keep alive for a bit to receive the WS alert
        await asyncio.sleep(5)

async def main():
    # Run the WebSocket listener and the metric sender concurrently
    listener_task = asyncio.create_task(listen_for_alerts())
    
    # Wait a second to ensure WebSocket connects before sending data
    await asyncio.sleep(1)
    
    sender_task = asyncio.create_task(send_metrics())
    
    await sender_task
    
    # Let listener run a bit longer to catch late alerts
    await asyncio.sleep(2)
    listener_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
