from pydantic import BaseModel


class CoachResponse(BaseModel):
    id: int
    first_name: str
    last_name: str
    photo_url: str | None = None
    role: str
    country: dict | None = None

    class Config:
        from_attributes = True


class TeamCoachesResponse(BaseModel):
    items: list[CoachResponse]
    total: int
