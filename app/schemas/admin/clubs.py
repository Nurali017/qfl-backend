from pydantic import BaseModel


class AdminClubCreateRequest(BaseModel):
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    short_name: str | None = None
    logo_url: str | None = None
    city_id: int | None = None
    stadium_id: int | None = None
    is_active: bool = True


class AdminClubUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    short_name: str | None = None
    logo_url: str | None = None
    city_id: int | None = None
    stadium_id: int | None = None
    is_active: bool | None = None


class AdminClubResponse(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    short_name: str | None = None
    logo_url: str | None = None
    city_id: int | None = None
    stadium_id: int | None = None
    is_active: bool = True

    model_config = {"from_attributes": True}


class AdminClubsListResponse(BaseModel):
    items: list[AdminClubResponse]
    total: int
