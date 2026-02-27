"""Pydantic schemas for GET /games/{id}/lineup endpoint."""

from pydantic import BaseModel


class LineupCountryBrief(BaseModel):
    id: int
    code: str
    name: str
    flag_url: str | None = None


class LineupReferee(BaseModel):
    id: int
    first_name: str | None = None
    last_name: str | None = None
    role: str
    photo_url: str | None = None
    country: LineupCountryBrief | None = None


class LineupCoach(BaseModel):
    id: int
    first_name: str | None = None
    last_name: str | None = None
    role: str
    photo_url: str | None = None
    country: LineupCountryBrief | None = None


class LineupPlayer(BaseModel):
    player_id: int
    first_name: str | None = None
    last_name: str | None = None
    country: LineupCountryBrief | None = None
    shirt_number: int | None = None
    is_captain: bool = False
    position: str | None = None
    amplua: str | None = None
    field_position: str | None = None
    photo_url: str | None = None


class LineupTeam(BaseModel):
    team_id: int | None = None
    team_name: str | None = None
    formation: str | None = None
    kit_color: str | None = None
    starters: list[LineupPlayer] = []
    substitutes: list[LineupPlayer] = []


class LineupRendering(BaseModel):
    mode: str
    source: str | None = None
    field_allowed_by_rules: bool = False
    field_data_valid: bool = False


class LineupCoaches(BaseModel):
    home_team: list[LineupCoach] = []
    away_team: list[LineupCoach] = []


class LineupTeams(BaseModel):
    home_team: LineupTeam
    away_team: LineupTeam


class GameLineupResponse(BaseModel):
    game_id: int
    has_lineup: bool = False
    rendering: LineupRendering
    referees: list[LineupReferee] = []
    coaches: LineupCoaches
    lineups: LineupTeams
