from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class CoachResponse(BaseModel):
    id: int
    first_name: str
    last_name: str
    photo_url: FileUrl = None
    role: str
    country: dict | None = None

    class Config:
        from_attributes = True


class TeamCoachesResponse(BaseModel):
    items: list[CoachResponse]
    total: int
