import datetime as dt
from typing import Optional
from pydantic import BaseModel


class AdminBroadcasterResponse(BaseModel):
    id: int
    name: str
    logo_url: Optional[str] = None
    type: Optional[str] = None
    website: Optional[str] = None
    sort_order: int = 0
    is_active: bool = True
    created_at: Optional[dt.datetime] = None
    updated_at: Optional[dt.datetime] = None


class AdminBroadcasterCreateRequest(BaseModel):
    name: str
    logo_url: Optional[str] = None
    type: Optional[str] = None  # "tv" | "youtube"
    website: Optional[str] = None
    sort_order: int = 0
    is_active: bool = True


class AdminBroadcasterUpdateRequest(BaseModel):
    name: Optional[str] = None
    logo_url: Optional[str] = None
    type: Optional[str] = None
    website: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None


class AdminBroadcastersListResponse(BaseModel):
    items: list[AdminBroadcasterResponse]
    total: int


class AdminGameBroadcasterItem(BaseModel):
    id: int  # GameBroadcaster.id (for delete)
    broadcaster_id: int
    broadcaster_name: str
    logo_url: Optional[str] = None
    sort_order: int = 0


class AdminGameBroadcasterAddRequest(BaseModel):
    broadcaster_id: int
    sort_order: int = 0
