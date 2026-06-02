from pydantic import BaseModel, Field
from datetime import datetime

class MetricsPayload(BaseModel):
    request_volume: int = Field(..., ge=0)
    error_rate: float = Field(..., ge=0.0, le=1.0)
    latency_ms: float = Field(..., ge=0.0)

class IngestionPayload(BaseModel):
    transaction_id: str
    client_id: str
    timestamp: datetime
    channel: str
    region: str
    metrics: MetricsPayload
