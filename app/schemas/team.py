from datetime import date, time as dt_time

from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class TeamBase(BaseModel):
    id: int
    name: str


class TeamResponse(TeamBase):
    logo_url: FileUrl = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None

    class Config:
        from_attributes = True


class TeamListResponse(BaseModel):
    items: list[TeamResponse]
    total: int


class TeamDetailResponse(TeamResponse):
    city: str | None = None
    website: str | None = None
    club_id: int | None = None
    club_name: str | None = None


class TeamInGame(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None
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

    # xG
    xg: float | None = None
    xg_per_match: float | None = None
    opponent_xg: float | None = None

    # Shots
    shots: int | None = None
    shots_on_goal: int | None = None
    shots_off_goal: int | None = None
    shot_per_match: float | None = None
    goal_to_shot_ratio: float | None = None

    # Possession
    possession_avg: float | None = None

    # Passes
    passes: int | None = None
    pass_accuracy_avg: float | None = None
    pass_per_match: float | None = None
    pass_forward: int | None = None
    pass_long: int | None = None
    pass_long_ratio: float | None = None
    pass_progressive: int | None = None
    pass_cross: int | None = None
    pass_cross_ratio: float | None = None
    pass_to_box: int | None = None
    pass_to_3rd: int | None = None
    key_pass: int | None = None
    key_pass_per_match: float | None = None
    goal_pass: int | None = None  # assists

    # Defense
    tackle: int | None = None
    tackle_per_match: float | None = None
    interception: int | None = None
    interception_per_match: float | None = None
    recovery: int | None = None
    recovery_per_match: float | None = None

    # Duels
    duel: int | None = None
    duel_ratio: float | None = None
    aerial_duel_offence: int | None = None
    aerial_duel_offence_ratio: float | None = None
    ground_duel_offence: int | None = None
    ground_duel_offence_ratio: float | None = None

    # Dribbles
    dribble: int | None = None
    dribble_per_match: float | None = None
    dribble_ratio: float | None = None

    # Discipline
    fouls: int | None = None
    foul_taken: int | None = None
    yellow_cards: int | None = None
    second_yellow_cards: int | None = None
    red_cards: int | None = None

    # Set pieces
    corners: int | None = None
    corner_per_match: float | None = None
    offsides: int | None = None

    # Penalty
    penalty: int | None = None
    penalty_ratio: float | None = None

    # Other
    clean_sheets: int | None = None

    extra_stats: dict | None = None

    class Config:
        from_attributes = True


class TeamStatsTableEntry(BaseModel):
    """Single team entry for the statistics table."""
    team_id: int
    team_name: str
    team_logo: FileUrl = None

    # Basic stats
    games_played: int | None = None
    wins: int | None = None
    draws: int | None = None
    losses: int | None = None
    goals_scored: int | None = None
    goals_conceded: int | None = None
    goal_difference: int | None = None
    points: int | None = None

    # Goals per match
    goals_per_match: float | None = None
    goals_conceded_per_match: float | None = None

    # Shots
    shots: int | None = None
    shots_on_goal: int | None = None
    shot_accuracy: float | None = None
    shots_per_match: float | None = None

    # Passes
    passes: int | None = None
    pass_accuracy: float | None = None
    key_passes: int | None = None
    crosses: int | None = None

    # Possession & Attacking
    possession: float | None = None
    dribbles: int | None = None
    dribble_success: float | None = None

    # Defense
    tackles: int | None = None
    interceptions: int | None = None
    recoveries: int | None = None

    # Discipline
    fouls: int | None = None
    fouls_per_match: float | None = None
    yellow_cards: int | None = None
    second_yellow_cards: int | None = None
    red_cards: int | None = None

    # Set pieces
    corners: int | None = None
    offsides: int | None = None

    # xG
    xg: float | None = None
    xg_per_match: float | None = None

    class Config:
        from_attributes = True


class TeamStatsTableResponse(BaseModel):
    """Response for team stats table endpoint."""
    season_id: int
    sort_by: str
    items: list[TeamStatsTableEntry]
    total: int


class TeamOverviewStadium(BaseModel):
    name: str | None = None
    city: str | None = None


class TeamOverviewTeam(BaseModel):
    id: int
    name: str
    city: str | None = None
    logo_url: FileUrl = None
    website: str | None = None
    stadium: TeamOverviewStadium | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None


class TeamOverviewSeason(BaseModel):
    id: int
    name: str
    tournament_id: int | None = None


class TeamOverviewSummary(BaseModel):
    games_played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_scored: int = 0
    goals_conceded: int = 0
    goal_difference: int = 0
    points: int = 0


class TeamOverviewMatchTeam(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None


class TeamOverviewMatch(BaseModel):
    id: int
    date: date
    time: dt_time | None = None
    tour: int | None = None
    status: str
    home_score: int | None = None
    away_score: int | None = None
    has_stats: bool = False
    has_lineup: bool = False
    home_team: TeamOverviewMatchTeam
    away_team: TeamOverviewMatchTeam
    stadium: TeamOverviewStadium | None = None


class TeamOverviewFormEntry(BaseModel):
    game_id: int
    is_home: bool
    opponent_name: str
    opponent_logo: FileUrl = None
    team_score: int
    opponent_score: int
    result: str


class TeamOverviewStandingEntry(BaseModel):
    position: int
    team_id: int
    team_name: str
    team_logo: FileUrl = None
    games_played: int
    points: int
    goal_difference: int
    goals_scored: int
    goals_conceded: int


class TeamOverviewLeaderPlayer(BaseModel):
    player_id: int
    first_name: str | None = None
    last_name: str | None = None
    photo_url: FileUrl = None
    team_id: int | None = None
    team_name: str | None = None
    team_logo: FileUrl = None
    position: str | None = None
    games_played: int = 0
    goals: int = 0
    assists: int = 0
    passes: int = 0
    save_shot: int = 0
    dry_match: int = 0
    red_cards: int = 0


class TeamOverviewMiniLeaders(BaseModel):
    passes: TeamOverviewLeaderPlayer | None = None
    appearances: TeamOverviewLeaderPlayer | None = None
    saves: TeamOverviewLeaderPlayer | None = None
    clean_sheets: TeamOverviewLeaderPlayer | None = None
    red_cards: TeamOverviewLeaderPlayer | None = None


class TeamOverviewLeaders(BaseModel):
    top_scorer: TeamOverviewLeaderPlayer | None = None
    top_assister: TeamOverviewLeaderPlayer | None = None
    goals_table: list[TeamOverviewLeaderPlayer]
    assists_table: list[TeamOverviewLeaderPlayer]
    mini_leaders: TeamOverviewMiniLeaders


class TeamOverviewCoachPreview(BaseModel):
    id: int
    first_name: str
    last_name: str
    photo_url: FileUrl = None
    role: str
    country_name: str | None = None


class TeamOverviewResponse(BaseModel):
    team: TeamOverviewTeam
    season: TeamOverviewSeason | None = None
    summary: TeamOverviewSummary
    form_last5: list[TeamOverviewFormEntry]
    recent_match: TeamOverviewMatch | None = None
    upcoming_matches: list[TeamOverviewMatch]
    standings_window: list[TeamOverviewStandingEntry]
    leaders: TeamOverviewLeaders
    staff_preview: list[TeamOverviewCoachPreview]
