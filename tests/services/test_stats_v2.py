from app.services.stats_v2 import MetricDefinition, compute_metric_ranks


def test_compute_metric_ranks_desc_uses_competition_rank():
    """Standard competition ranking: tied entries share the higher rank,
    and subsequent ranks skip the gap so a player's rank reflects how many
    entries are actually ahead of them."""
    registry = {
        "goal": MetricDefinition(group="goals", rank_order="desc"),
    }
    items = [
        {"player_id": 1, "goal": 7},
        {"player_id": 2, "goal": 5},
        {"player_id": 3, "goal": 5},
        {"player_id": 4, "goal": 1},
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["goal"] == 1
    assert ranks[2]["goal"] == 2
    assert ranks[3]["goal"] == 2
    assert ranks[4]["goal"] == 4


def test_compute_metric_ranks_desc_excludes_zero_values():
    """DESC-order count metrics skip zero values so a player with 0
    assists is not ranked alongside everyone else who did not contribute."""
    registry = {
        "goal_pass": MetricDefinition(
            group="goals", rank_order="desc", exclude_zero=True
        ),
    }
    items = [
        {"player_id": 1, "goal_pass": 3},
        {"player_id": 2, "goal_pass": 1},
        {"player_id": 3, "goal_pass": 0},
        {"player_id": 4, "goal_pass": 0},
        {"player_id": 5, "goal_pass": None},
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["goal_pass"] == 1
    assert ranks[2]["goal_pass"] == 2
    assert ranks[3]["goal_pass"] is None
    assert ranks[4]["goal_pass"] is None
    assert ranks[5]["goal_pass"] is None


def test_compute_metric_ranks_desc_skips_tied_group_count():
    """After a group of N tied leaders, the next rank should be N+1, not 2."""
    registry = {
        "goal_pass": MetricDefinition(
            group="goals", rank_order="desc", exclude_zero=True
        ),
    }
    items = [
        {"player_id": 1, "goal_pass": 2},
        {"player_id": 2, "goal_pass": 2},
        {"player_id": 3, "goal_pass": 2},
        {"player_id": 4, "goal_pass": 1},
        {"player_id": 5, "goal_pass": 1},
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["goal_pass"] == 1
    assert ranks[2]["goal_pass"] == 1
    assert ranks[3]["goal_pass"] == 1
    assert ranks[4]["goal_pass"] == 4
    assert ranks[5]["goal_pass"] == 4


def test_compute_metric_ranks_asc_excludes_zero():
    """ASC-order metrics (fouls, cards, own goals) also exclude zero —
    showing a "★ 1 место" badge for hundreds of players with zero red
    cards is noise, not a ranking signal."""
    registry = {
        "yellow_cards": MetricDefinition(group="disciplinary", rank_order="asc"),
    }
    items = [
        {"player_id": 1, "yellow_cards": 0},
        {"player_id": 2, "yellow_cards": 2},
        {"player_id": 3, "yellow_cards": 0},
        {"player_id": 4, "yellow_cards": 1},
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["yellow_cards"] is None
    assert ranks[3]["yellow_cards"] is None
    assert ranks[4]["yellow_cards"] == 1
    assert ranks[2]["yellow_cards"] == 2


def test_compute_metric_ranks_asc_skips_nulls():
    registry = {
        "red_cards": MetricDefinition(group="disciplinary", rank_order="asc"),
    }
    items = [
        {"player_id": 1, "red_cards": 0},
        {"player_id": 2, "red_cards": 2},
        {"player_id": 3, "red_cards": None},
        {"player_id": 4, "red_cards": 0},
        {"player_id": 5, "red_cards": 1},
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["red_cards"] is None
    assert ranks[4]["red_cards"] is None
    assert ranks[3]["red_cards"] is None
    assert ranks[5]["red_cards"] == 1
    assert ranks[2]["red_cards"] == 2
