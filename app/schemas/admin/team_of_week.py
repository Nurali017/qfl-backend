from pydantic import BaseModel


class AdminTeamOfWeekCreateRequest(BaseModel):
    season_id: int
    tour_key: str
    locale: str = "ru"
    scheme: str | None = None
    payload: list = []


class AdminTeamOfWeekUpdateRequest(BaseModel):
    season_id: int | None = None
    tour_key: str | None = None
    locale: str | None = None
    scheme: str | None = None
    payload: list | None = None


class AdminTeamOfWeekResponse(BaseModel):
    id: int
    season_id: int
    tour_key: str
    locale: str
    scheme: str | None = None
    payload: list = []

    model_config = {"from_attributes": True}


class AdminTeamOfWeekListResponse(BaseModel):
    items: list[AdminTeamOfWeekResponse]
    total: int
