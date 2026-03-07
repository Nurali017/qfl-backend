from pydantic import BaseModel


class AdminTeamUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    website: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_en: str | None = None
    stadium_id: int | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None
    logo_url: str | None = None


class AdminTeamListItem(BaseModel):
    id: int
    name: str
    stadium_id: int | None = None
    stadium_name: str | None = None


class AdminTeamsListResponse(BaseModel):
    items: list[AdminTeamListItem]
    total: int


class AdminTeamDetailResponse(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    website: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_en: str | None = None
    stadium_id: int | None = None
    stadium_name: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None
    logo_url: str | None = None
