from app.services.stats_v2 import MetricDefinition, compute_metric_ranks


def test_compute_metric_ranks_desc_uses_dense_rank():
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
    assert ranks[4]["goal"] == 3


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


def test_compute_metric_ranks_asc_keeps_zero_as_best():
    """ASC-order metrics (fouls, cards) treat 0 as the best outcome and
    must keep zero values in the ranking."""
    registry = {
        "yellow_cards": MetricDefinition(
            group="disciplinary", rank_order="asc", exclude_zero=False
        ),
    }
    items = [
        {"player_id": 1, "yellow_cards": 0},
        {"player_id": 2, "yellow_cards": 2},
        {"player_id": 3, "yellow_cards": 0},
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["yellow_cards"] == 1
    assert ranks[3]["yellow_cards"] == 1
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
    ]

    ranks = compute_metric_ranks(
        items,
        entity_id_field="player_id",
        registry=registry,
    )

    assert ranks[1]["red_cards"] == 1
    assert ranks[4]["red_cards"] == 1
    assert ranks[2]["red_cards"] == 2
    assert ranks[3]["red_cards"] is None
