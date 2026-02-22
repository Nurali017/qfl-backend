from datetime import date
from datetime import time as time_type

from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class BracketGameTeam(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None

    class Config:
        from_attributes = True


class BracketGameBrief(BaseModel):
    id: int
    date: date
    time: time_type | None = None
    home_team: BracketGameTeam | None = None
    away_team: BracketGameTeam | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    status: str | None = None

    class Config:
        from_attributes = True


class PlayoffBracketEntry(BaseModel):
    id: int
    round_name: str
    side: str = "left"
    sort_order: int = 1
    is_third_place: bool = False
    game: BracketGameBrief | None = None

    class Config:
        from_attributes = True


ROUND_LABELS = {
    "1_32": "1/32 финала",
    "1_16": "1/16 финала",
    "1_8": "1/8 финала",
    "1_4": "Четвертьфинал",
    "1_2": "Полуфинал",
    "final": "Финал",
    "3rd_place": "За 3-е место",
}


class PlayoffRound(BaseModel):
    round_name: str
    round_label: str
    entries: list[PlayoffBracketEntry]


class PlayoffBracketResponse(BaseModel):
    season_id: int
    rounds: list[PlayoffRound]
