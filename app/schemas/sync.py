from enum import Enum
from pydantic import BaseModel


class SyncStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class SyncResponse(BaseModel):
    status: SyncStatus
    message: str
    details: dict | None = None
