from pydantic import BaseModel


class TeamOfWeekPlayerEntry(BaseModel):
    id: str | None = None
    person_id: int | None = None
    first_name: str = ""
    last_name: str = ""
    full_name: str = ""
    team: dict | None = None
    number: int | None = None
    captain: bool = False
    amplua: str | None = None
    position: str | None = None
    ordering: int | None = None
    photo: str | None = None
    command_logo: str | None = None
    command_id: int | None = None


class TeamOfWeekResponse(BaseModel):
    id: int
    season_id: int
    tour_key: str
    locale: str
    scheme: str | None = None
    payload: list[TeamOfWeekPlayerEntry]

    model_config = {"from_attributes": True}


class TeamOfWeekListResponse(BaseModel):
    items: list[TeamOfWeekResponse]
    total: int
