from pydantic import BaseModel


class TeamBase(BaseModel):
    id: int
    name: str


class TeamResponse(TeamBase):
    logo_url: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None

    class Config:
        from_attributes = True


class TeamListResponse(BaseModel):
    items: list[TeamResponse]
    total: int


class TeamDetailResponse(TeamResponse):
    pass


class TeamInGame(BaseModel):
    id: int
    name: str
    logo_url: str | None = None
    score: int | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None


class TeamFromSOTA(BaseModel):
    id: int
    name: str
    name_en: str | None = None
    logo: str | None = None
    city: str | None = None
    city_en: str | None = None


class TeamSeasonStatsResponse(BaseModel):
    team_id: int
    season_id: int

    # Basic stats
    games_played: int | None = None
    wins: int | None = None
    draws: int | None = None
    losses: int | None = None
    goals_scored: int | None = None
    goals_conceded: int | None = None
    goal_difference: int | None = None
    points: int | None = None

    # Detailed stats
    shots: int | None = None
    shots_on_goal: int | None = None
    possession_avg: float | None = None
    passes: int | None = None
    pass_accuracy_avg: float | None = None
    fouls: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None
    corners: int | None = None
    offsides: int | None = None

    extra_stats: dict | None = None

    class Config:
        from_attributes = True
