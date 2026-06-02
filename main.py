import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from redis.asyncio import Redis
import database
from schemas import IngestionPayload

redis_client = None

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await database.create_pool()
    global redis_client
    redis_client = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
    yield
    # Shutdown
    await database.close_pool()
    if redis_client:
        await redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.post("/api/v1/metrics", status_code=202)
async def ingest_metrics(payload: IngestionPayload):
    # Convert datetime to isoformat string for JSON serialization
    payload_dict = payload.model_dump(mode='json')
    # Validate and push directly to Redis stream
    await redis_client.xadd("metrics_stream", {"payload": json.dumps(payload_dict)})
    return {"status": "accepted"}

@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection open and wait for incoming messages (if any)
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Internal endpoint for worker to push anomaly alerts
@app.post("/api/internal/broadcast_alert")
async def broadcast_alert(request: Request):
    data = await request.json()
    await manager.broadcast(json.dumps(data))
    return {"status": "broadcasted"}
