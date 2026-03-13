"""Tests for lineup position validation helpers."""

from app.services.lineup import team_has_valid_field_data


def _make_starter(amplua: str, field_position: str) -> dict:
    return {"amplua": amplua, "field_position": field_position}


def _make_lineup(starters: list[dict]) -> dict:
    return {"starters": starters}


class TestTeamHasValidFieldData:
    def test_valid_formation_distinct_positions(self):
        starters = [
            _make_starter("Gk", "C"),
            _make_starter("D", "L"),
            _make_starter("D", "LC"),
            _make_starter("D", "RC"),
            _make_starter("D", "R"),
            _make_starter("DM", "C"),
            _make_starter("M", "L"),
            _make_starter("M", "C"),
            _make_starter("M", "R"),
            _make_starter("F", "L"),
            _make_starter("F", "R"),
        ]
        assert team_has_valid_field_data(_make_lineup(starters)) is True

    def test_two_players_same_slot_is_ok(self):
        """2 center-backs at (D, C) is normal."""
        starters = [
            _make_starter("Gk", "C"),
            _make_starter("D", "L"),
            _make_starter("D", "C"),
            _make_starter("D", "C"),  # 2nd at same slot
            _make_starter("D", "R"),
            _make_starter("M", "L"),
            _make_starter("M", "LC"),
            _make_starter("M", "C"),
            _make_starter("M", "RC"),
            _make_starter("M", "R"),
            _make_starter("F", "C"),
        ]
        assert team_has_valid_field_data(_make_lineup(starters)) is True

    def test_three_players_same_slot_fails(self):
        """3 players at (D, C) means bad data."""
        starters = [
            _make_starter("Gk", "C"),
            _make_starter("D", "C"),
            _make_starter("D", "C"),
            _make_starter("D", "C"),  # 3rd at same slot
            _make_starter("D", "R"),
            _make_starter("M", "L"),
            _make_starter("M", "C"),
            _make_starter("M", "R"),
            _make_starter("AM", "L"),
            _make_starter("AM", "R"),
            _make_starter("F", "C"),
        ]
        assert team_has_valid_field_data(_make_lineup(starters)) is False

    def test_all_defaulted_to_midfield_center_fails(self):
        """When all 11 players default to (M, C), positions are clearly missing."""
        starters = [_make_starter("M", "C") for _ in range(11)]
        assert team_has_valid_field_data(_make_lineup(starters)) is False

    def test_fewer_than_11_starters_fails(self):
        starters = [_make_starter("Gk", "C") for _ in range(10)]
        assert team_has_valid_field_data(_make_lineup(starters)) is False

    def test_invalid_amplua_fails(self):
        starters = [_make_starter("Gk", "C")] * 10 + [_make_starter("INVALID", "C")]
        assert team_has_valid_field_data(_make_lineup(starters)) is False

    def test_invalid_field_position_fails(self):
        starters = [_make_starter("Gk", "C")] * 10 + [_make_starter("D", "INVALID")]
        assert team_has_valid_field_data(_make_lineup(starters)) is False
