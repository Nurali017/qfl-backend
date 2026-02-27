from pydantic import BaseModel



class ClubResponse(BaseModel):
    id: int
    name: str
    short_name: str | None = None
    logo_url: str | None = None
    city_name: str | None = None
    is_active: bool = True

    class Config:
        from_attributes = True


class TeamBrief(BaseModel):
    id: int
    name: str
    logo_url: str | None = None

    class Config:
        from_attributes = True


class ClubWithTeamsResponse(ClubResponse):
    teams: list[TeamBrief] = []


class ClubListResponse(BaseModel):
    items: list[ClubResponse]
    total: int
