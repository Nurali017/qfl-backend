from datetime import date

from pydantic import BaseModel


class ChampionshipResponse(BaseModel):
    id: int
    name: str
    short_name: str | None = None
    slug: str | None = None
    sort_order: int = 0
    is_active: bool = True

    class Config:
        from_attributes = True


class ChampionshipListResponse(BaseModel):
    items: list[ChampionshipResponse]
    total: int


# Tree response: Championship → Tournaments → Seasons

class SeasonBrief(BaseModel):
    id: int
    name: str
    date_start: date | None = None
    date_end: date | None = None
    sync_enabled: bool = True

    class Config:
        from_attributes = True


class TournamentInChampionship(BaseModel):
    id: int
    name: str
    seasons: list[SeasonBrief] = []

    class Config:
        from_attributes = True


class ChampionshipTreeResponse(BaseModel):
    id: int
    name: str
    short_name: str | None = None
    slug: str | None = None
    tournaments: list[TournamentInChampionship] = []

    class Config:
        from_attributes = True


class ChampionshipTreeListResponse(BaseModel):
    items: list[ChampionshipTreeResponse]
    total: int
