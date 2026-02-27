from datetime import date
from pydantic import BaseModel

from app.schemas.country import CountryInPlayer


class PlayerBase(BaseModel):
    id: int
    first_name: str | None = None
    last_name: str | None = None
    birthday: date | None = None
    player_type: str | None = None
    country: CountryInPlayer | None = None
    photo_url: str | None = None
    age: int | None = None
    top_role: str | None = None


class PlayerResponse(PlayerBase):
    class Config:
        from_attributes = True


class PlayerListResponse(BaseModel):
    items: list[PlayerResponse]
    total: int


class PlayerDetailResponse(PlayerResponse):
    teams: list[int] = []
    jersey_number: int | None = None
    height: int | None = None
    weight: int | None = None
    gender: str | None = None


class PlayerWithTeamResponse(PlayerResponse):
    team_id: int | None = None
    number: int | None = None


class PlayerFromSOTA(BaseModel):
    id: str
    first_name: str | None = None
    last_name: str | None = None
    birthday: date | None = None
    type: str | None = None
    country_name: str | None = None
    country_code: str | None = None
    photo: str | None = None
    age: int | None = None
    top_role: str | None = None
    teams: list[int] = []


class PlayerSeasonStatsResponse(BaseModel):
    player_id: int
    season_id: int
    team_id: int | None = None

    # Basic stats
    games_played: int | None = None
    games_starting: int | None = None
    minutes_played: int | None = None

    # Goals & Assists
    goals: int | None = None
    assists: int | None = None
    xg: float | None = None
    xg_per_90: float | None = None

    # Shots
    shots: int | None = None
    shots_on_goal: int | None = None

    # Passes
    passes: int | None = None
    pass_accuracy: float | None = None
    key_passes: int | None = None

    # Duels
    duels: int | None = None
    duels_won: int | None = None

    # Discipline
    yellow_cards: int | None = None
    red_cards: int | None = None

    # All 50+ metrics from v2 API
    extra_stats: dict | None = None

    class Config:
        from_attributes = True


# Player Stats Table (top scorers, assistants, etc.)
class PlayerStatsTableEntry(BaseModel):
    """Single entry in player stats table."""

    player_id: int
    first_name: str | None = None
    last_name: str | None = None
    photo_url: str | None = None
    country: CountryInPlayer | None = None
    team_id: int | None = None
    team_name: str | None = None
    team_logo: str | None = None
    player_type: str | None = None
    top_role: str | None = None
    position_code: str | None = None

    # Stats
    games_played: int | None = None
    minutes_played: int | None = None
    goals: int | None = None
    assists: int | None = None
    goal_and_assist: int | None = None
    xg: float | None = None
    shots: int | None = None
    shots_on_goal: int | None = None
    passes: int | None = None
    key_passes: int | None = None
    pass_accuracy: float | None = None
    duels: int | None = None
    duels_won: int | None = None
    aerial_duel: int | None = None
    ground_duel: int | None = None
    tackle: int | None = None
    interception: int | None = None
    recovery: int | None = None
    dribble: int | None = None
    dribble_success: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    # Goalkeeper stats
    save_shot: int | None = None
    dry_match: int | None = None


class PlayerStatsTableResponse(BaseModel):
    """Response for player stats table endpoint."""

    season_id: int
    sort_by: str
    items: list[PlayerStatsTableEntry]
    total: int


class PlayerTeammateResponse(BaseModel):
    """Response for player teammate."""

    player_id: int
    first_name: str | None = None
    last_name: str | None = None
    jersey_number: int | None = None
    position: str | None = None
    age: int | None = None
    photo_url: str | None = None

    class Config:
        from_attributes = True


class PlayerTeammatesListResponse(BaseModel):
    """Response for player teammates list."""

    items: list[PlayerTeammateResponse]
    total: int


class PlayerTournamentHistoryEntry(BaseModel):
    """Single entry in player tournament history."""

    season_id: int
    season_name: str | None = None
    championship_name: str | None = None
    team_id: int | None = None
    team_name: str | None = None
    position: str | None = None
    games_played: int | None = None
    minutes_played: int | None = None
    goals: int | None = None
    assists: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None


class PlayerTournamentHistoryResponse(BaseModel):
    """Response for player tournament history."""

    items: list[PlayerTournamentHistoryEntry]
    total: int
