from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class PartnerResponse(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None
    website: str | None = None
    sort_order: int = 0
    is_active: bool = True

    class Config:
        from_attributes = True


class PartnerListResponse(BaseModel):
    items: list[PartnerResponse]
    total: int
