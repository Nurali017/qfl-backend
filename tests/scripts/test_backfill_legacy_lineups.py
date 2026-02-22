from scripts.backfill_legacy_lineups import (
    compute_render_mode,
    pick_game_source,
    pick_team_source,
)


def test_pick_team_source_prefers_team_squad_over_api_and_fallback():
    source, payload = pick_team_source(
        team_squad_raw='[{"number":"STARTING"},{"number":1,"first_name":"A","last_name":"B","amplua":"Gk","position":"C"},{"number":"SUBS"}]',
        sota_payload=[{"number": "STARTING"}, {"number": 2}],
        vsporte_payload=[{"Number": "STARTING"}, {"Number": 3}],
        fallback_rows=[{"line": 1, "number": 7, "captain": 0}],
    )

    assert source == "team_squad"
    assert len(payload["main"]) == 1


def test_pick_team_source_falls_back_from_sota_to_vsporte_to_matches_players():
    source_sota, _ = pick_team_source(
        team_squad_raw=None,
        sota_payload=[{"number": "STARTING"}, {"number": 10, "amplua": "D", "position": "C"}],
        vsporte_payload=[{"Number": "STARTING"}, {"Number": 11}],
        fallback_rows=[],
    )
    assert source_sota == "sota_api"

    source_vsporte, _ = pick_team_source(
        team_squad_raw=None,
        sota_payload=None,
        vsporte_payload=[{"Number": "STARTING"}, {"Number": 11}],
        fallback_rows=[],
    )
    assert source_vsporte == "vsporte_api"

    source_fallback, payload_fallback = pick_team_source(
        team_squad_raw=None,
        sota_payload=None,
        vsporte_payload=None,
        fallback_rows=[{"line": 1, "number": 15, "captain": 1, "name": "Test Player"}],
    )
    assert source_fallback == "matches_players"
    assert len(payload_fallback["main"]) == 1


def test_pick_game_source_uses_highest_priority_available_source():
    assert pick_game_source(("matches_players", "vsporte_api"), has_lineup=True) == "vsporte_api"
    assert pick_game_source(("team_squad", "matches_players"), has_lineup=True) == "team_squad"
    assert pick_game_source(("none", "none"), has_lineup=False) == "none"


def test_compute_render_mode_field_list_hidden():
    assert compute_render_mode(
        has_lineup=True,
        field_allowed_by_rules=True,
        home_valid=True,
        away_valid=True,
    ) == "field"
    assert compute_render_mode(
        has_lineup=True,
        field_allowed_by_rules=True,
        home_valid=True,
        away_valid=False,
    ) == "list"
    assert compute_render_mode(
        has_lineup=False,
        field_allowed_by_rules=True,
        home_valid=True,
        away_valid=True,
    ) == "hidden"

