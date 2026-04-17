from datetime import date
from typing import Any
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
    photo_url_avatar: str | None = None
    photo_url_leaderboard: str | None = None
    photo_url_player_page: str | None = None
    age: int | None = None
    top_role: str | None = None


class PlayerResponse(PlayerBase):
    class Config:
        from_attributes = True


class PlayerListResponse(BaseModel):
    items: list[PlayerResponse]
    total: int


class PlayerPositionsBlock(BaseModel):
    """Aggregated field positions derived from recent lineups (or top_role fallback)."""

    primary: str | None = None          # e.g. "АП", "ЦЗ"
    secondary: list[str] = []           # up to 2 extra positions
    sample_size: int = 0                # lineup rows considered
    source: str = "unknown"             # "lineups" | "top_role" | "unknown"


class PlayerDetailResponse(PlayerResponse):
    teams: list[int] = []
    jersey_number: int | None = None
    height: int | None = None
    weight: int | None = None
    gender: str | None = None
    contract_end: str | None = None
    # Normalized position group derived from player_type + top_role (GK/DEF/MID/FWD).
    position_code: str | None = None
    # Aggregated positions from recent starter lineups (preferred over top_role).
    positions: PlayerPositionsBlock | None = None


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
    time_on_field_total: int | None = None

    # Goals & Assists
    goal: int | None = None
    goal_pass: int | None = None
    xg: float | None = None
    xg_per_90: float | None = None

    # Shots
    shot: int | None = None
    shots_on_goal: int | None = None

    # Passes
    passes: int | None = None
    pass_ratio: float | None = None
    key_pass: int | None = None

    # Duels
    duel: int | None = None
    duel_success: int | None = None

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
    photo_url_avatar: str | None = None
    photo_url_leaderboard: str | None = None
    country: CountryInPlayer | None = None
    team_id: int | None = None
    team_name: str | None = None
    team_logo: str | None = None
    player_type: str | None = None
    position_code: str | None = None

    # Stats
    games_played: int | None = None
    time_on_field_total: int | None = None
    goal: int | None = None
    goal_pass: int | None = None
    goal_and_assist: int | None = None
    xg: float | None = None
    shot: int | None = None
    shots_on_goal: int | None = None
    passes: int | None = None
    key_pass: int | None = None
    pass_ratio: float | None = None
    duel: int | None = None
    duel_success: int | None = None
    aerial_duel: int | None = None
    ground_duel: int | None = None
    tackle: int | None = None
    interception: int | None = None
    recovery: int | None = None
    dribble: int | None = None
    dribble_success: int | None = None
    yellow_cards: int | None = None
    second_yellow_cards: int | None = None
    red_cards: int | None = None
    # Goals extra
    owngoal: int | None = None
    penalty_success: int | None = None
    goal_out_box: int | None = None
    xg_per_90: float | None = None
    # Shots extra
    shots_blocked_opponent: int | None = None
    # Passes extra
    pass_acc: int | None = None
    pass_forward: int | None = None
    pass_progressive: int | None = None
    pass_cross: int | None = None
    pass_cross_acc: int | None = None
    pass_to_box: int | None = None
    pass_to_3rd: int | None = None
    # Attacking extra
    dribble_per_90: float | None = None
    corner: int | None = None
    offside: int | None = None
    # Defending extra
    tackle_per_90: float | None = None
    aerial_duel_success: int | None = None
    ground_duel_success: int | None = None
    # Discipline
    foul: int | None = None
    foul_taken: int | None = None
    # Goalkeeper stats
    save_shot: int | None = None
    dry_match: int | None = None
    goals_conceded: int | None = None
    goals_conceded_penalty: int | None = None
    save_shot_ratio: float | None = None
    save_shot_penalty: int | None = None
    exit: int | None = None
    exit_success: int | None = None


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
    photo_url_avatar: str | None = None

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
    frontend_code: str | None = None
    season_year: int | None = None
    team_id: int | None = None
    team_name: str | None = None
    position: str | None = None
    games_played: int | None = None
    games_starting: int | None = None
    time_on_field_total: int | None = None
    goal: int | None = None
    goal_pass: int | None = None
    shot: int | None = None
    shots_on_goal: int | None = None
    passes: int | None = None
    pass_ratio: float | None = None
    key_pass: int | None = None
    duel: int | None = None
    duel_success: int | None = None
    tackle: int | None = None
    interception: int | None = None
    recovery: int | None = None
    dribble: int | None = None
    xg: float | None = None
    xg_per_90: float | None = None
    corner: int | None = None
    offside: int | None = None
    foul: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    extra_stats: dict[str, Any] | None = None


class PlayerTournamentHistoryResponse(BaseModel):
    """Response for player tournament history."""

    items: list[PlayerTournamentHistoryEntry]
    total: int
    default_season_id: int | None = None


class PlayerMatchHistoryTeam(BaseModel):
    """Team block embedded in a player match history entry."""

    id: int | None = None
    name: str | None = None
    logo_url: str | None = None
    score: int | None = None


class PlayerMatchHistoryEntry(BaseModel):
    """Single match played by a player, with per-match stats."""

    game_id: int
    date: str | None = None
    tour: int | None = None
    season_id: int
    season_name: str | None = None
    home_team: PlayerMatchHistoryTeam
    away_team: PlayerMatchHistoryTeam
    player_team_id: int | None = None
    position: str | None = None
    minutes_played: int | None = None
    started: bool | None = None
    goals: int = 0
    assists: int = 0
    shots: int = 0
    shots_on_goal: int = 0
    shots_off_goal: int = 0
    passes: int = 0
    pass_accuracy: float | None = None
    duel: int = 0
    tackle: int = 0
    corner: int = 0
    offside: int = 0
    foul: int = 0
    yellow_cards: int = 0
    red_cards: int = 0
    extra_stats: dict[str, Any] | None = None


class PlayerMatchHistoryResponse(BaseModel):
    """Response for player match history."""

    items: list[PlayerMatchHistoryEntry]
    total: int
