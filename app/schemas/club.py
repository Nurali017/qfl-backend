from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class ClubResponse(BaseModel):
    id: int
    name: str
    short_name: str | None = None
    logo_url: FileUrl = None
    city_name: str | None = None
    is_active: bool = True

    class Config:
        from_attributes = True


class TeamBrief(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None

    class Config:
        from_attributes = True


class ClubWithTeamsResponse(ClubResponse):
    teams: list[TeamBrief] = []


class ClubListResponse(BaseModel):
    items: list[ClubResponse]
    total: int
