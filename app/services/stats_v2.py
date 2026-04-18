from collections import defaultdict
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, PlayerSeasonStats, TeamSeasonStats
from app.utils.numbers import sanitize_non_finite_numbers, to_finite_float

RankOrder = Literal["asc", "desc"]


@dataclass(frozen=True)
class MetricDefinition:
    group: str
    rankable: bool = True
    rank_order: RankOrder = "desc"
    # When True, exclude rows whose value is 0 from rank computation so the
    # "no event" default does not generate a misleading rank badge. Applies
    # universally: in desc metrics 0 means "did not contribute", in asc
    # metrics (own goals, offsides, cards) 0 means "no incident" and most
    # of the league sits there — showing hundreds of players tied at rank 1
    # for having zero own goals is pure noise.
    exclude_zero: bool = True


def _build_metric_registry(
    *,
    base_groups: dict[str, str],
    groups: dict[str, tuple[str, ...]],
    asc_fields: set[str],
) -> dict[str, MetricDefinition]:
    registry: dict[str, MetricDefinition] = {}

    def _make(group: str, key: str) -> MetricDefinition:
        return MetricDefinition(
            group=group,
            rank_order="asc" if key in asc_fields else "desc",
        )

    for key, group in base_groups.items():
        registry[key] = _make(group, key)

    for group, keys in groups.items():
        for key in keys:
            registry[key] = _make(group, key)

    return registry


PLAYER_V2_KEY_STATS = (
    "games_played",
    "time_on_field_total",
    "goal",
    "goal_pass",
    "goal_and_assist",
)

PLAYER_V2_GROUPS: dict[str, tuple[str, ...]] = {
    "goals": (
        "goal",
        "goal_pass",
        "goal_and_assist",
        "penalty_success",
        "owngoal",
        "goal_out_box",
        "xg",
        "xg_per_90",
    ),
    "attempts": (
        "shot",
        "shots_on_goal",
        "shots_blocked_opponent",
    ),
    "distribution": (
        "passes",
        "pass_ratio",
        "pass_acc",
        "key_pass",
        "pass_forward",
        "pass_progressive",
        "pass_cross",
        "pass_to_box",
        "pass_to_3rd",
    ),
    "attacking": (
        "dribble",
        "dribble_success",
        "dribble_per_90",
        "corner",
        "offside",
    ),
    "defending": (
        "tackle",
        "tackle_per_90",
        "interception",
        "recovery",
        "duel",
        "duel_success",
        "aerial_duel",
        "aerial_duel_success",
        "ground_duel",
        "ground_duel_success",
    ),
    "goalkeeping": (
        "save_shot",
        "dry_match",
        "goals_conceded",
        "save_shot_ratio",
        "save_shot_penalty",
        "exit",
        "exit_success",
    ),
    "disciplinary": (
        "foul",
        "foul_taken",
        "yellow_cards",
        "second_yellow_cards",
        "red_cards",
    ),
}

PLAYER_V2_BASE_GROUPS = {
    "games_played": "key_stats",
    "time_on_field_total": "key_stats",
}
PLAYER_V2_ASC_FIELDS = {
    "owngoal",
    "shots_blocked_opponent",
    "offside",
    "foul",
    "yellow_cards",
    "second_yellow_cards",
    "red_cards",
    "goals_conceded",
}
PLAYER_V2_METRICS = _build_metric_registry(
    base_groups=PLAYER_V2_BASE_GROUPS,
    groups=PLAYER_V2_GROUPS,
    asc_fields=PLAYER_V2_ASC_FIELDS,
)
PLAYER_V2_SORT_FIELDS = tuple(PLAYER_V2_METRICS.keys())

TEAM_V2_KEY_STATS = (
    "games_played",
    "win",
    "draw",
    "match_loss",
    "goal",
    "goals_conceded",
    "goal_difference",
    "points",
)

TEAM_V2_GROUPS: dict[str, tuple[str, ...]] = {
    "goals": (
        "goal",
        "goals_per_match",
        "goals_conceded",
        "goals_conceded_per_match",
        "goal_difference",
        "xg",
        "opponent_xg",
        "penalty",
        "penalty_ratio",
    ),
    "attempts": (
        "shot",
        "shots_on_goal",
        "shots_off_goal",
        "shot_accuracy",
        "shot_per_match",
        "freekick_shot",
    ),
    "distribution": (
        "passes",
        "pass_ratio",
        "pass_per_match",
        "key_pass",
        "goal_pass",
        "pass_forward",
        "pass_long",
        "pass_progressive",
        "pass_cross",
        "pass_to_box",
        "pass_to_3rd",
    ),
    "attacking": (
        "possession_percent_average",
        "dribble",
        "dribble_ratio",
        "corner",
        "offside",
    ),
    "defending": (
        "tackle",
        "tackle_per_match",
        "interception",
        "interception_per_match",
        "recovery",
        "recovery_per_match",
        "duel",
        "duel_ratio",
        "aerial_duel_offence",
        "aerial_duel_defence",
        "ground_duel_offence",
        "ground_duel_defence",
        "tackle1_1",
        "clean_sheets",
    ),
    "disciplinary": (
        "foul",
        "foul_taken",
        "yellow_cards",
        "second_yellow_cards",
        "red_cards",
        "foul_per_match",
    ),
}

TEAM_V2_BASE_GROUPS = {
    "games_played": "key_stats",
    "win": "key_stats",
    "draw": "key_stats",
    "match_loss": "key_stats",
    "points": "key_stats",
}
TEAM_V2_ASC_FIELDS = {
    "match_loss",
    "goals_conceded",
    "goals_conceded_per_match",
    "opponent_xg",
    "shots_off_goal",
    "offside",
    "foul",
    "yellow_cards",
    "second_yellow_cards",
    "red_cards",
}
TEAM_V2_METRICS = _build_metric_registry(
    base_groups=TEAM_V2_BASE_GROUPS,
    groups=TEAM_V2_GROUPS,
    asc_fields=TEAM_V2_ASC_FIELDS,
)
TEAM_V2_SORT_FIELDS = tuple(TEAM_V2_METRICS.keys())

_PLAYER_INT_FIELDS = {
    "games_played",
    "time_on_field_total",
    "goal",
    "goal_pass",
    "goal_and_assist",
    "penalty_success",
    "owngoal",
    "goal_out_box",
    "shot",
    "shots_on_goal",
    "shots_blocked_opponent",
    "passes",
    "pass_acc",
    "key_pass",
    "pass_forward",
    "pass_progressive",
    "pass_cross",
    "pass_to_box",
    "pass_to_3rd",
    "dribble",
    "dribble_success",
    "corner",
    "offside",
    "tackle",
    "interception",
    "recovery",
    "duel",
    "duel_success",
    "aerial_duel",
    "aerial_duel_success",
    "ground_duel",
    "ground_duel_success",
    "save_shot",
    "dry_match",
    "goals_conceded",
    "save_shot_penalty",
    "exit",
    "exit_success",
    "foul",
    "foul_taken",
    "yellow_cards",
    "second_yellow_cards",
    "red_cards",
}

_PLAYER_FLOAT_FIELDS = {
    "xg",
    "xg_per_90",
    "pass_ratio",
    "dribble_per_90",
    "tackle_per_90",
    "save_shot_ratio",
}


def _as_int(value: object) -> int | None:
    number = to_finite_float(value)
    if number is None:
        return None
    return int(number)


def _per_match(total: object, games_played: object, fallback: object = None) -> float | None:
    total_num = to_finite_float(total)
    gp_num = to_finite_float(games_played)
    if total_num is not None and gp_num is not None and gp_num > 0:
        return round(total_num / gp_num, 2)
    return to_finite_float(fallback)


def _percentage(part: object, whole: object, fallback: object = None) -> float | None:
    part_num = to_finite_float(part)
    whole_num = to_finite_float(whole)
    if part_num is not None and whole_num is not None and whole_num > 0:
        return round((part_num / whole_num) * 100, 1)
    return to_finite_float(fallback)


def _sort_tuple(value: object) -> tuple[int, float]:
    num = to_finite_float(value)
    return (1, 0.0) if num is None else (0, -num)


def _get_player_metric(stats: PlayerSeasonStats, key: str) -> object:
    value = getattr(stats, key, None)
    if value is not None:
        return value

    extra_stats = stats.extra_stats or {}
    return extra_stats.get(key)


def build_player_stats_payload(stats: PlayerSeasonStats) -> dict[str, object]:
    payload: dict[str, object] = {
        "player_id": stats.player_id,
        "season_id": stats.season_id,
        "team_id": stats.team_id,
    }

    for field in _PLAYER_INT_FIELDS:
        payload[field] = _as_int(_get_player_metric(stats, field))

    for field in _PLAYER_FLOAT_FIELDS:
        payload[field] = to_finite_float(_get_player_metric(stats, field))

    if payload["goal_and_assist"] is None:
        goal = to_finite_float(payload.get("goal"))
        goal_pass = to_finite_float(payload.get("goal_pass"))
        if goal is not None or goal_pass is not None:
            payload["goal_and_assist"] = int((goal or 0) + (goal_pass or 0))

    return sanitize_non_finite_numbers(payload)


def build_team_stats_payload(
    stats: TeamSeasonStats,
    *,
    clean_sheets: int | None = None,
) -> dict[str, object]:
    goal = _as_int(stats.goal)
    goals_conceded = _as_int(stats.goals_conceded)
    games_played = _as_int(stats.games_played)
    shot = _as_int(stats.shot)
    shots_on_goal = _as_int(stats.shots_on_goal)
    passes = _as_int(stats.passes)
    foul = _as_int(stats.foul)
    tackle = _as_int(stats.tackle)
    interception = _as_int(stats.interception)
    recovery = _as_int(stats.recovery)

    payload = {
        "team_id": stats.team_id,
        "season_id": stats.season_id,
        "games_played": games_played,
        "win": _as_int(stats.win),
        "draw": _as_int(stats.draw),
        "match_loss": _as_int(stats.match_loss),
        "goal": goal,
        "goals_conceded": goals_conceded,
        "goal_difference": (
            (goal - goals_conceded)
            if goal is not None and goals_conceded is not None
            else _as_int(stats.goals_difference)
        ),
        "points": _as_int(stats.points),
        "goals_per_match": _per_match(goal, games_played),
        "goals_conceded_per_match": _per_match(goals_conceded, games_played),
        "xg": to_finite_float(stats.xg),
        "opponent_xg": to_finite_float(stats.opponent_xg),
        "penalty": _as_int(stats.penalty),
        "penalty_ratio": to_finite_float(stats.penalty_ratio),
        "shot": shot,
        "shots_on_goal": shots_on_goal,
        "shots_off_goal": _as_int(stats.shots_off_goal),
        "shot_accuracy": _percentage(shots_on_goal, shot),
        "shot_per_match": _per_match(shot, games_played, stats.shot_per_match),
        "freekick_shot": _as_int(stats.freekick_shot),
        "passes": passes,
        "pass_ratio": to_finite_float(stats.pass_ratio),
        "pass_per_match": _per_match(passes, games_played, stats.pass_per_match),
        "key_pass": _as_int(stats.key_pass),
        "goal_pass": _as_int(stats.goal_pass),
        "pass_forward": _as_int(stats.pass_forward),
        "pass_long": _as_int(stats.pass_long),
        "pass_progressive": _as_int(stats.pass_progressive),
        "pass_cross": _as_int(stats.pass_cross),
        "pass_to_box": _as_int(stats.pass_to_box),
        "pass_to_3rd": _as_int(stats.pass_to_3rd),
        "possession_percent_average": to_finite_float(stats.possession_percent_average),
        "dribble": _as_int(stats.dribble),
        "dribble_ratio": to_finite_float(stats.dribble_ratio),
        "corner": _as_int(stats.corner),
        "offside": _as_int(stats.offside),
        "tackle": tackle,
        "tackle_per_match": _per_match(tackle, games_played, stats.tackle_per_match),
        "interception": interception,
        "interception_per_match": _per_match(interception, games_played, stats.interception_per_match),
        "recovery": recovery,
        "recovery_per_match": _per_match(recovery, games_played, stats.recovery_per_match),
        "duel": _as_int(stats.duel),
        "duel_ratio": to_finite_float(stats.duel_ratio),
        "aerial_duel_offence": _as_int(stats.aerial_duel_offence),
        "aerial_duel_defence": _as_int(stats.aerial_duel_defence),
        "ground_duel_offence": _as_int(stats.ground_duel_offence),
        "ground_duel_defence": _as_int(stats.ground_duel_defence),
        "tackle1_1": _as_int(stats.tackle1_1),
        "clean_sheets": clean_sheets,
        "foul": foul,
        "foul_taken": _as_int(stats.foul_taken),
        "yellow_cards": _as_int(stats.yellow_cards),
        "second_yellow_cards": _as_int(stats.second_yellow_cards),
        "red_cards": _as_int(stats.red_cards),
        "foul_per_match": _per_match(foul, games_played, None),
    }
    return sanitize_non_finite_numbers(payload)


def build_empty_ranks(
    registry: dict[str, MetricDefinition],
) -> dict[str, int | None]:
    return {
        key: None
        for key, definition in registry.items()
        if definition.rankable
    }


def compute_metric_ranks(
    items: list[dict[str, object]],
    *,
    entity_id_field: str,
    registry: dict[str, MetricDefinition],
) -> dict[int, dict[str, int | None]]:
    ranks: dict[int, dict[str, int | None]] = {}

    for item in items:
        entity_id = _as_int(item.get(entity_id_field))
        if entity_id is None:
            continue
        ranks[entity_id] = build_empty_ranks(registry)

    for key, definition in registry.items():
        if not definition.rankable:
            continue

        ranked_values: list[tuple[int, float]] = []
        for item in items:
            entity_id = _as_int(item.get(entity_id_field))
            if entity_id is None:
                continue

            value = to_finite_float(item.get(key))
            if value is None:
                continue
            if definition.exclude_zero and value == 0:
                continue

            ranked_values.append((entity_id, value))

        ranked_values.sort(
            key=lambda item: item[1],
            reverse=definition.rank_order == "desc",
        )

        # Standard competition ranking ("1224"): tied entries share the
        # higher rank, then subsequent ranks skip to reflect how many
        # entries are actually ahead. For assists this means 9 players tied
        # at 2 share rank 1, and the next 44 players at 1 get rank 10,
        # which matches how sports leaderboards are read.
        last_value: float | None = None
        current_rank = 0
        for index, (entity_id, value) in enumerate(ranked_values, start=1):
            if last_value is None or value != last_value:
                current_rank = index
                last_value = value

            ranks[entity_id][key] = current_rank

    return ranks


def _build_catalog_metrics_payload(
    registry: dict[str, MetricDefinition],
) -> dict[str, dict[str, object]]:
    return {
        key: {
            "group": definition.group,
            "rankable": definition.rankable,
            "rank_order": definition.rank_order,
        }
        for key, definition in registry.items()
    }


def build_stats_catalog_payload() -> dict[str, object]:
    return {
        "players": {
            "key_stats": list(PLAYER_V2_KEY_STATS),
            "groups": {group: list(keys) for group, keys in PLAYER_V2_GROUPS.items()},
            "sortable_fields": list(PLAYER_V2_SORT_FIELDS),
            "metrics": _build_catalog_metrics_payload(PLAYER_V2_METRICS),
        },
        "teams": {
            "key_stats": list(TEAM_V2_KEY_STATS),
            "groups": {group: list(keys) for group, keys in TEAM_V2_GROUPS.items()},
            "sortable_fields": list(TEAM_V2_SORT_FIELDS),
            "metrics": _build_catalog_metrics_payload(TEAM_V2_METRICS),
        },
    }


async def get_player_detail_payload_with_ranks(
    db: AsyncSession,
    *,
    season_id: int,
    player_id: int,
) -> dict[str, object] | None:
    result = await db.execute(
        select(PlayerSeasonStats).where(PlayerSeasonStats.season_id == season_id)
    )
    season_rows = result.scalars().all()
    if not season_rows:
        return None

    payloads = [build_player_stats_payload(stats) for stats in season_rows]
    ranks_by_player_id = compute_metric_ranks(
        payloads,
        entity_id_field="player_id",
        registry=PLAYER_V2_METRICS,
    )

    for payload in payloads:
        if _as_int(payload.get("player_id")) != player_id:
            continue

        payload_with_ranks = dict(payload)
        payload_with_ranks["ranks"] = ranks_by_player_id.get(
            player_id,
            build_empty_ranks(PLAYER_V2_METRICS),
        )
        return payload_with_ranks

    return None


async def get_team_detail_payload_with_ranks(
    db: AsyncSession,
    *,
    season_id: int,
    team_id: int,
) -> dict[str, object] | None:
    result = await db.execute(
        select(TeamSeasonStats).where(TeamSeasonStats.season_id == season_id)
    )
    season_rows = result.scalars().all()
    if not season_rows:
        return None

    team_ids = [stats.team_id for stats in season_rows]
    clean_sheets_map = await get_team_clean_sheets_map(db, season_id, team_ids=team_ids)
    payloads = [
        build_team_stats_payload(
            stats,
            clean_sheets=clean_sheets_map.get(stats.team_id, 0),
        )
        for stats in season_rows
    ]
    ranks_by_team_id = compute_metric_ranks(
        payloads,
        entity_id_field="team_id",
        registry=TEAM_V2_METRICS,
    )

    for payload in payloads:
        if _as_int(payload.get("team_id")) != team_id:
            continue

        payload_with_ranks = dict(payload)
        payload_with_ranks["ranks"] = ranks_by_team_id.get(
            team_id,
            build_empty_ranks(TEAM_V2_METRICS),
        )
        return payload_with_ranks

    return None


def sort_player_stats_items(items: list[object], sort_by: str) -> list[object]:
    def key(item: object) -> tuple[tuple[int, float], ...]:
        primary = getattr(item, sort_by, None)
        return (
            _sort_tuple(primary),
            _sort_tuple(getattr(item, "goal", None)),
            _sort_tuple(getattr(item, "goal_pass", None)),
            _sort_tuple(getattr(item, "games_played", None)),
            _sort_tuple(getattr(item, "time_on_field_total", None)),
        )

    return sorted(items, key=key)


def sort_team_stats_items(items: list[object], sort_by: str) -> list[object]:
    def key(item: object) -> tuple[tuple[int, float], ...]:
        primary = getattr(item, sort_by, None)
        return (
            _sort_tuple(primary),
            _sort_tuple(getattr(item, "points", None)),
            _sort_tuple(getattr(item, "goal_difference", None)),
            _sort_tuple(getattr(item, "goal", None)),
        )

    return sorted(items, key=key)


async def get_team_clean_sheets_map(
    db: AsyncSession,
    season_id: int,
    *,
    team_ids: list[int] | None = None,
) -> dict[int, int]:
    query = select(
        Game.home_team_id,
        Game.away_team_id,
        Game.home_score,
        Game.away_score,
    ).where(
        Game.season_id == season_id,
        Game.home_score.is_not(None),
        Game.away_score.is_not(None),
    )

    if team_ids:
        query = query.where(
            or_(Game.home_team_id.in_(team_ids), Game.away_team_id.in_(team_ids))
        )

    result = await db.execute(query)
    rows = result.all()

    clean_sheets: dict[int, int] = defaultdict(int)
    for home_team_id, away_team_id, home_score, away_score in rows:
        if away_score == 0:
            clean_sheets[home_team_id] += 1
        if home_score == 0:
            clean_sheets[away_team_id] += 1

    return dict(clean_sheets)
