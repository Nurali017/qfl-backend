"""Pydantic schemas for GET /games/{id}/stats endpoint."""

from pydantic import BaseModel


class GameStatsTeamEntry(BaseModel):
    team_id: int
    team_name: str | None = None
    logo_url: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None
    possession: float | None = None
    possession_percent: float | None = None
    shots: int | None = None
    shots_on_goal: int | None = None
    passes: int | None = None
    pass_accuracy: float | None = None
    fouls: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    corners: int | None = None
    offsides: int | None = None
    extra_stats: dict | None = None


class StatsCountryBrief(BaseModel):
    id: int
    code: str
    name: str
    flag_url: str | None = None


class GameStatsPlayerEntry(BaseModel):
    player_id: int
    first_name: str | None = None
    last_name: str | None = None
    country: StatsCountryBrief | None = None
    team_id: int | None = None
    team_name: str | None = None
    team_primary_color: str | None = None
    team_secondary_color: str | None = None
    team_accent_color: str | None = None
    position: str | None = None
    minutes_played: int | None = None
    started: bool | None = None
    goals: int = 0
    assists: int = 0
    shots: int | None = None
    passes: int | None = None
    pass_accuracy: float | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    extra_stats: dict | None = None


class GameStatsEventEntry(BaseModel):
    id: int
    half: int | None = None
    minute: int | None = None
    event_type: str
    team_id: int | None = None
    team_name: str | None = None
    player_id: int | None = None
    player_name: str | None = None
    player_number: int | None = None
    player2_id: int | None = None
    player2_name: str | None = None
    player2_number: int | None = None


class GameStatsResponse(BaseModel):
    game_id: int
    is_technical: bool = False
    team_stats: list[GameStatsTeamEntry] = []
    player_stats: list[GameStatsPlayerEntry] = []
    events: list[GameStatsEventEntry] = []
