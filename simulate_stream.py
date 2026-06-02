import time
import random
import requests
from datetime import datetime, timezone

API_URL = "http://localhost:8000/api/v1/metrics" # Adjust port if your API container uses a different one

def generate_metric_packet(is_anomaly=False):
    """Generates synthetic multi-dimensional streaming data."""
    if is_anomaly:
        # Simulate a sudden server failure or attack vector
        return {
            "transaction_id": f"tx_{random.randint(100000, 999999)}",
            "client_id": random.choice(["client_alpha", "client_beta"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel": random.choice(["mobile_app", "web_frontend"]),
            "region": "us-east",
            "metrics": {
                "request_volume": random.randint(8000, 12000), # Huge spike
                "error_rate": round(random.uniform(0.15, 0.45), 3), # High errors
                "latency_ms": round(random.uniform(900.0, 2500.0), 2) # High latency
            },
        }
    else:
        # Simulate clean, normal baseline operations
        return {
            "transaction_id": f"tx_{random.randint(100000, 999999)}",
            "client_id": random.choice(["client_alpha", "client_beta"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel": random.choice(["mobile_app", "web_frontend"]),
            "region": "us-east",
            "metrics": {
                "request_volume": random.randint(800, 1500),
                "error_rate": round(random.uniform(0.001, 0.02), 3),
                "latency_ms": round(random.uniform(40.0, 120.0), 2)
            }
        }

if __name__ == "__main__":
    print("🚀 Starting real-time metric stream simulator... Press Ctrl+C to stop.")
    count = 0
    while True:
        # Trigger an intentional multi-dimensional anomaly every 20 records
        make_anomaly = (count % 20 == 0 and count > 0)
        
        payload = generate_metric_packet(is_anomaly=make_anomaly)
        try:
            response = requests.post(API_URL, json=payload)
            print(f"[{payload['metrics']['request_volume']} reqs] Sent to API -> Status: {response.status_code}")
        except Exception as e:
            print(f"❌ Connection Error: {e}")
            
        count += 1
        time.sleep(1.0) # Send 1 metric record every second