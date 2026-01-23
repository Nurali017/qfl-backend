import datetime
from uuid import UUID
from pydantic import BaseModel


class GameTeamStatsResponse(BaseModel):
    team_id: int
    team_name: str | None = None
    logo_url: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None
    possession: float | None = None
    possession_percent: int | None = None
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

    class Config:
        from_attributes = True


class GamePlayerStatsResponse(BaseModel):
    player_id: UUID
    first_name: str | None = None
    last_name: str | None = None
    team_id: int
    team_name: str | None = None
    team_primary_color: str | None = None
    team_secondary_color: str | None = None
    team_accent_color: str | None = None
    number: int | None = None
    position: str | None = None
    minutes_played: int | None = None
    started: bool | None = None
    goals: int = 0
    assists: int = 0
    shots: int = 0
    passes: int = 0
    pass_accuracy: float | None = None
    yellow_cards: int = 0
    red_cards: int = 0
    extra_stats: dict | None = None

    class Config:
        from_attributes = True


class GameStatsResponse(BaseModel):
    game_id: UUID
    team_stats: list[GameTeamStatsResponse] = []
    player_stats: list[GamePlayerStatsResponse] = []


class NextGameInfo(BaseModel):
    game_id: UUID
    date: datetime.date | None = None
    opponent_id: int
    opponent_name: str | None = None
    opponent_logo: str | None = None
    is_home: bool


class ScoreTableEntryResponse(BaseModel):
    position: int | None = None
    team_id: int
    team_name: str | None = None
    team_logo: str | None = None
    games_played: int | None = None
    wins: int | None = None
    draws: int | None = None
    losses: int | None = None
    goals_scored: int | None = None
    goals_conceded: int | None = None
    goal_difference: int | None = None
    points: int | None = None
    form: str | None = None
    next_game: NextGameInfo | None = None

    class Config:
        from_attributes = True


class ScoreTableFilters(BaseModel):
    tour_from: int | None = None
    tour_to: int | None = None
    home_away: str | None = None


class ScoreTableResponse(BaseModel):
    season_id: int
    filters: ScoreTableFilters | None = None
    table: list[ScoreTableEntryResponse] = []


class TeamResultsGridEntry(BaseModel):
    position: int
    team_id: int
    team_name: str | None = None
    team_logo: str | None = None
    results: list[str | None] = []


class ResultsGridResponse(BaseModel):
    season_id: int
    total_tours: int
    teams: list[TeamResultsGridEntry] = []


class PlayerSeasonStatsResponse(BaseModel):
    player_id: UUID
    season_id: int
    games_played: int = 0
    minutes_played: int = 0
    goals: int = 0
    assists: int = 0
    yellow_cards: int = 0
    red_cards: int = 0


class TeamSeasonStatsResponse(BaseModel):
    team_id: int
    season_id: int
    games_played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_scored: int = 0
    goals_conceded: int = 0


class GameTeamStatsFromSOTA(BaseModel):
    id: int
    name: str
    logo: str | None = None
    stats: dict = {}


class PlayerStatsInGameFromSOTA(BaseModel):
    goals: int = 0
    assists: int = 0
    shots: int = 0
    shots_on_target: int = 0
    passes: int = 0
    pass_accuracy: float | None = None
    key_passes: int = 0
    tackles: int = 0
    interceptions: int = 0
    fouls_committed: int = 0
    fouls_won: int = 0
    yellow_cards: int = 0
    red_cards: int = 0
    dribbles_attempted: int = 0
    dribbles_successful: int = 0
    duels_won: int = 0
    duels_lost: int = 0


class GamePlayerStatsFromSOTA(BaseModel):
    id: str
    first_name: str | None = None
    last_name: str | None = None
    number: int | None = None
    team_id: int
    team_name: str | None = None
    position: str | None = None
    minutes_played: int | None = None
    started: bool = False
    stats: PlayerStatsInGameFromSOTA = PlayerStatsInGameFromSOTA()
