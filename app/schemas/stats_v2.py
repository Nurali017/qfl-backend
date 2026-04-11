from typing import Literal

from pydantic import BaseModel, Field

from app.schemas.country import CountryInPlayer


class MetricCatalogItemV2(BaseModel):
    group: str
    rankable: bool
    rank_order: Literal["asc", "desc"]


class StatsCatalogEntityV2(BaseModel):
    key_stats: list[str]
    groups: dict[str, list[str]]
    sortable_fields: list[str]
    metrics: dict[str, MetricCatalogItemV2]


class StatsCatalogResponseV2(BaseModel):
    players: StatsCatalogEntityV2
    teams: StatsCatalogEntityV2


class PlayerStatsValuesV2(BaseModel):
    games_played: int | None = None
    time_on_field_total: int | None = None

    goal: int | None = None
    goal_pass: int | None = None
    goal_and_assist: int | None = None
    penalty_success: int | None = None
    owngoal: int | None = None
    goal_out_box: int | None = None
    xg: float | None = None
    xg_per_90: float | None = None

    shot: int | None = None
    shots_on_goal: int | None = None
    shots_blocked_opponent: int | None = None

    passes: int | None = None
    pass_ratio: float | None = None
    pass_acc: int | None = None
    key_pass: int | None = None
    pass_forward: int | None = None
    pass_progressive: int | None = None
    pass_cross: int | None = None
    pass_to_box: int | None = None
    pass_to_3rd: int | None = None

    dribble: int | None = None
    dribble_success: int | None = None
    dribble_per_90: float | None = None
    corner: int | None = None
    offside: int | None = None

    tackle: int | None = None
    tackle_per_90: float | None = None
    interception: int | None = None
    recovery: int | None = None
    duel: int | None = None
    duel_success: int | None = None
    aerial_duel: int | None = None
    aerial_duel_success: int | None = None
    ground_duel: int | None = None
    ground_duel_success: int | None = None

    save_shot: int | None = None
    dry_match: int | None = None
    goals_conceded: int | None = None
    save_shot_ratio: float | None = None
    save_shot_penalty: int | None = None
    exit: int | None = None
    exit_success: int | None = None

    foul: int | None = None
    foul_taken: int | None = None
    yellow_cards: int | None = None
    second_yellow_cards: int | None = None
    red_cards: int | None = None


class PlayerStatsV2(PlayerStatsValuesV2):
    player_id: int
    season_id: int
    team_id: int | None = None
    ranks: dict[str, int | None] = Field(default_factory=dict)


class PlayerStatsTableEntryV2(PlayerStatsValuesV2):
    player_id: int
    season_id: int
    team_id: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    photo_url: str | None = None
    country: CountryInPlayer | None = None
    team_name: str | None = None
    team_logo: str | None = None
    player_type: str | None = None
    position_code: str | None = None


class PlayerStatsTableResponseV2(BaseModel):
    season_id: int
    sort_by: str
    items: list[PlayerStatsTableEntryV2]
    total: int


class TeamStatsValuesV2(BaseModel):
    games_played: int | None = None
    win: int | None = None
    draw: int | None = None
    match_loss: int | None = None
    goal: int | None = None
    goals_conceded: int | None = None
    goal_difference: int | None = None
    points: int | None = None

    goals_per_match: float | None = None
    goals_conceded_per_match: float | None = None
    xg: float | None = None
    opponent_xg: float | None = None
    penalty: int | None = None
    penalty_ratio: float | None = None

    shot: int | None = None
    shots_on_goal: int | None = None
    shots_off_goal: int | None = None
    shot_accuracy: float | None = None
    shot_per_match: float | None = None
    freekick_shot: int | None = None

    passes: int | None = None
    pass_ratio: float | None = None
    pass_per_match: float | None = None
    key_pass: int | None = None
    goal_pass: int | None = None
    pass_forward: int | None = None
    pass_long: int | None = None
    pass_progressive: int | None = None
    pass_cross: int | None = None
    pass_to_box: int | None = None
    pass_to_3rd: int | None = None

    possession_percent_average: float | None = None
    dribble: int | None = None
    dribble_ratio: float | None = None
    corner: int | None = None
    offside: int | None = None

    tackle: int | None = None
    tackle_per_match: float | None = None
    interception: int | None = None
    interception_per_match: float | None = None
    recovery: int | None = None
    recovery_per_match: float | None = None
    duel: int | None = None
    duel_ratio: float | None = None
    aerial_duel_offence: int | None = None
    aerial_duel_defence: int | None = None
    ground_duel_offence: int | None = None
    ground_duel_defence: int | None = None
    tackle1_1: int | None = None
    clean_sheets: int | None = None

    foul: int | None = None
    foul_taken: int | None = None
    yellow_cards: int | None = None
    second_yellow_cards: int | None = None
    red_cards: int | None = None
    foul_per_match: float | None = None


class TeamStatsV2(TeamStatsValuesV2):
    team_id: int
    season_id: int
    ranks: dict[str, int | None] = Field(default_factory=dict)


class TeamStatsTableEntryV2(TeamStatsValuesV2):
    team_id: int
    season_id: int
    team_name: str
    team_logo: str | None = None


class TeamStatsTableResponseV2(BaseModel):
    season_id: int
    sort_by: str
    items: list[TeamStatsTableEntryV2]
    total: int
