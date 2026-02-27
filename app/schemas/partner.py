from pydantic import BaseModel



class PartnerResponse(BaseModel):
    id: int
    name: str
    logo_url: str | None = None
    website: str | None = None
    sort_order: int = 0
    is_active: bool = True

    class Config:
        from_attributes = True


class PartnerListResponse(BaseModel):
    items: list[PartnerResponse]
    total: int
