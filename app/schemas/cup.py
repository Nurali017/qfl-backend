from datetime import date
from datetime import time as time_type

from pydantic import BaseModel

from app.schemas.playoff_bracket import PlayoffBracketResponse
from app.utils.file_urls import FileUrl


class CupTeamBrief(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None

    class Config:
        from_attributes = True


class CupGameBrief(BaseModel):
    id: int
    date: date
    time: time_type | None = None
    stage_name: str | None = None
    home_team: CupTeamBrief | None = None
    away_team: CupTeamBrief | None = None
    home_score: int | None = None
    away_score: int | None = None
    home_penalty_score: int | None = None
    away_penalty_score: int | None = None
    status: str | None = None
    is_live: bool = False

    class Config:
        from_attributes = True


class CupGroupStandingEntry(BaseModel):
    position: int
    team_id: int
    team_name: str | None = None
    team_logo: FileUrl = None
    games_played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_scored: int = 0
    goals_conceded: int = 0
    goal_difference: int = 0
    points: int = 0


class CupGroup(BaseModel):
    group_name: str
    standings: list[CupGroupStandingEntry]


class CupRound(BaseModel):
    stage_id: int | None = None
    round_name: str
    round_key: str
    is_current: bool = False
    total_games: int = 0
    played_games: int = 0
    games: list[CupGameBrief] = []


class CupOverviewResponse(BaseModel):
    season_id: int
    season_name: str | None = None
    tournament_name: str | None = None
    championship_name: str | None = None
    current_round: CupRound | None = None
    groups: list[CupGroup] | None = None
    bracket: PlayoffBracketResponse | None = None
    recent_results: list[CupGameBrief] = []
    upcoming_games: list[CupGameBrief] = []
    rounds: list[CupRound] = []


class CupScheduleResponse(BaseModel):
    season_id: int
    rounds: list[CupRound]
    total_games: int = 0
