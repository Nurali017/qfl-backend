from pydantic import BaseModel


class AdminPartnerCreateRequest(BaseModel):
    name: str
    logo_url: str | None = None
    website: str | None = None
    championship_id: int | None = None
    season_id: int | None = None
    sort_order: int = 0
    is_active: bool = True
    show_in_news: bool = False


class AdminPartnerUpdateRequest(BaseModel):
    name: str | None = None
    logo_url: str | None = None
    website: str | None = None
    championship_id: int | None = None
    season_id: int | None = None
    sort_order: int | None = None
    is_active: bool | None = None
    show_in_news: bool | None = None


class AdminPartnerResponse(BaseModel):
    id: int
    name: str
    logo_url: str | None = None
    website: str | None = None
    championship_id: int | None = None
    season_id: int | None = None
    sort_order: int = 0
    is_active: bool = True
    show_in_news: bool = False

    model_config = {"from_attributes": True}


class AdminPartnersListResponse(BaseModel):
    items: list[AdminPartnerResponse]
    total: int
