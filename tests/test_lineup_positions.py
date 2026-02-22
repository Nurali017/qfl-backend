"""Tests for derive_field_positions utility."""

import pytest

from app.utils.lineup_positions import (
    derive_field_positions,
    get_lateral_hint,
    infer_formation,
    parse_formation,
)


# ---------------------------------------------------------------------------
# parse_formation
# ---------------------------------------------------------------------------

class TestParseFormation:
    def test_valid_442(self):
        assert parse_formation("4-4-2") == [4, 4, 2]

    def test_valid_4231(self):
        assert parse_formation("4-2-3-1") == [4, 2, 3, 1]

    def test_valid_352(self):
        assert parse_formation("3-5-2") == [3, 5, 2]

    def test_valid_532(self):
        assert parse_formation("5-3-2") == [5, 3, 2]

    def test_valid_343(self):
        assert parse_formation("3-4-3") == [3, 4, 3]

    def test_invalid_sum(self):
        assert parse_formation("4-4-3") is None

    def test_invalid_format(self):
        assert parse_formation("abc") is None

    def test_none(self):
        assert parse_formation(None) is None

    def test_empty(self):
        assert parse_formation("") is None

    def test_single_number(self):
        assert parse_formation("10") is None


# ---------------------------------------------------------------------------
# get_lateral_hint
# ---------------------------------------------------------------------------

class TestGetLateralHint:
    def test_left_defender_ru(self):
        assert get_lateral_hint("ЛЗ") == 0  # L

    def test_right_defender_ru(self):
        assert get_lateral_hint("ПЗ") == 4  # R

    def test_center_defender_ru(self):
        assert get_lateral_hint("ЦЗ") == 2  # C (default)

    def test_left_back_en(self):
        assert get_lateral_hint("LB") == 0

    def test_right_back_en(self):
        assert get_lateral_hint("RB") == 4

    def test_left_keyword(self):
        assert get_lateral_hint("Левый защитник") == 0

    def test_right_keyword(self):
        assert get_lateral_hint("Правый полузащитник") == 4

    def test_center_default(self):
        assert get_lateral_hint("Центральный защитник") == 2

    def test_none(self):
        assert get_lateral_hint(None) == 2

    def test_empty(self):
        assert get_lateral_hint("") == 2


# ---------------------------------------------------------------------------
# derive_field_positions
# ---------------------------------------------------------------------------

def _make_starters(specs: list[tuple[int, str, str | None]]) -> list[dict]:
    """Helper: [(player_id, amplua, top_role), ...] -> starters list."""
    return [
        {"player_id": pid, "amplua": amp, "top_role": tr}
        for pid, amp, tr in specs
    ]


def _positions_by_player(result: list[dict]) -> dict[int, str]:
    """Convert result to {player_id: field_position}."""
    return {r["player_id"]: r["field_position"] for r in result}


class TestDeriveFieldPositions442:
    """4-4-2: layers [4, 4, 2] → D(4), M(4), F(2)."""

    def test_basic_442(self):
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"),   # left defender
            (3, "D", "ЦЗ"),   # center defender
            (4, "D", "ЦЗ"),   # center defender
            (5, "D", "ПЗ"),   # right defender
            (6, "M", "ЛП"),   # left mid
            (7, "M", "ЦП"),   # center mid
            (8, "M", "ЦП"),   # center mid
            (9, "M", "ПП"),   # right mid
            (10, "F", None),  # forward
            (11, "F", None),  # forward
        ])
        result = derive_field_positions("4-4-2", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        assert pos[1] == "C"  # GK

        # Defenders: L, LC, RC, R
        assert pos[2] == "L"
        assert pos[5] == "R"
        # Two center defenders should get LC and RC
        assert {pos[3], pos[4]} == {"LC", "RC"}

        # Midfielders: L, LC, RC, R
        assert pos[6] == "L"
        assert pos[9] == "R"
        assert {pos[7], pos[8]} == {"LC", "RC"}

        # Forwards: LC, RC
        assert {pos[10], pos[11]} == {"LC", "RC"}

    def test_442_no_collisions(self):
        """Verify no two outfield players share (amplua, field_position)."""
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ЦЗ"), (5, "D", "ПЗ"),
            (6, "M", "ЛП"), (7, "M", "ЦП"), (8, "M", "ЦП"), (9, "M", "ПП"),
            (10, "F", None), (11, "F", None),
        ])
        result = derive_field_positions("4-4-2", starters)
        pos = _positions_by_player(result)

        # Group by amplua and check uniqueness within each group
        by_amplua: dict[str, list[str]] = {}
        for s in starters:
            pid = s["player_id"]
            amp = s["amplua"]
            by_amplua.setdefault(amp, []).append(pos[pid])

        for amp, positions in by_amplua.items():
            assert len(positions) == len(set(positions)), (
                f"Collision in amplua={amp}: {positions}"
            )


class TestDeriveFieldPositions4231:
    """4-2-3-1: layers [4, 2, 3, 1] → D(4), DM(2), AM(3), F(1)."""

    def test_basic_4231(self):
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ЦЗ"), (5, "D", "ПЗ"),
            (6, "DM", None), (7, "DM", None),
            (8, "AM", "ЛП"), (9, "AM", None), (10, "AM", "ПП"),
            (11, "F", None),
        ])
        result = derive_field_positions("4-2-3-1", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        assert pos[1] == "C"   # GK
        assert pos[11] == "C"  # Sole striker

        # Defenders: 4 slots → L, LC, RC, R
        assert pos[2] == "L"
        assert pos[5] == "R"

        # DMs: 2 slots → LC, RC
        assert {pos[6], pos[7]} == {"LC", "RC"}

        # AMs: 3 slots → L, C, R
        assert pos[8] == "L"
        assert pos[10] == "R"
        assert pos[9] == "C"

    def test_4231_with_M_instead_of_AM(self):
        """When coach uses M for attacking mids instead of AM."""
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ЦЗ"), (5, "D", "ПЗ"),
            (6, "DM", None), (7, "DM", None),
            (8, "M", "ЛП"), (9, "M", None), (10, "M", "ПП"),
            (11, "F", None),
        ])
        result = derive_field_positions("4-2-3-1", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        # M group gets 3 slots
        assert pos[8] == "L"
        assert pos[10] == "R"


class TestDeriveFieldPositions352:
    """3-5-2: layers [3, 5, 2] → D(3), M(5), F(2)."""

    def test_basic_352(self):
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ПЗ"),
            (5, "M", "ЛП"), (6, "M", None), (7, "M", None),
            (8, "M", None), (9, "M", "ПП"),
            (10, "F", None), (11, "F", None),
        ])
        result = derive_field_positions("3-5-2", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        # Defenders: 3 slots → L, C, R
        assert pos[2] == "L"
        assert pos[3] == "C"
        assert pos[4] == "R"

        # Midfielders: 5 slots → L, LC, C, RC, R
        assert pos[5] == "L"
        assert pos[9] == "R"

        # Forwards: 2 slots → LC, RC
        assert {pos[10], pos[11]} == {"LC", "RC"}

    def test_352_no_collisions(self):
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ПЗ"),
            (5, "M", "ЛП"), (6, "M", None), (7, "M", None),
            (8, "M", None), (9, "M", "ПП"),
            (10, "F", None), (11, "F", None),
        ])
        result = derive_field_positions("3-5-2", starters)
        pos = _positions_by_player(result)

        by_amplua: dict[str, list[str]] = {}
        for s in starters:
            pid = s["player_id"]
            amp = s["amplua"]
            by_amplua.setdefault(amp, []).append(pos[pid])

        for amp, positions in by_amplua.items():
            assert len(positions) == len(set(positions)), (
                f"Collision in amplua={amp}: {positions}"
            )


class TestDeriveFieldPositions343:
    """3-4-3: layers [3, 4, 3]."""

    def test_basic_343(self):
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ПЗ"),
            (5, "M", "ЛП"), (6, "M", None), (7, "M", None), (8, "M", "ПП"),
            (9, "F", "ЛН"), (10, "F", None), (11, "F", "ПН"),
        ])
        result = derive_field_positions("3-4-3", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        # Defenders: L, C, R
        assert pos[2] == "L"
        assert pos[3] == "C"
        assert pos[4] == "R"

        # Midfielders: L, LC, RC, R
        assert pos[5] == "L"
        assert pos[8] == "R"

        # Forwards: L, C, R
        assert pos[9] == "L"
        assert pos[11] == "R"
        assert pos[10] == "C"


class TestDeriveFieldPositions532:
    """5-3-2: layers [5, 3, 2]."""

    def test_basic_532(self):
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", None), (4, "D", "ЦЗ"),
            (5, "D", None), (6, "D", "ПЗ"),
            (7, "M", "ЛП"), (8, "M", None), (9, "M", "ПП"),
            (10, "F", None), (11, "F", None),
        ])
        result = derive_field_positions("5-3-2", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        # Defenders: 5 slots → L, LC, C, RC, R
        assert pos[2] == "L"
        assert pos[6] == "R"

        # All 5 defender positions should be unique
        d_positions = [pos[i] for i in [2, 3, 4, 5, 6]]
        assert len(set(d_positions)) == 5

        # Midfielders: L, C, R
        assert pos[7] == "L"
        assert pos[9] == "R"

        # Forwards: LC, RC
        assert {pos[10], pos[11]} == {"LC", "RC"}


class TestDeriveFieldPositionsMixedAmplua:
    """Test formations where amplua groups don't match layers perfectly."""

    def test_442_with_dm_and_am(self):
        """4-4-2 but SOTA labels some midfielders as DM/AM instead of M."""
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ЦЗ"), (5, "D", "ПЗ"),
            (6, "DM", None), (7, "DM", None),
            (8, "AM", None), (9, "AM", None),
            (10, "F", None), (11, "F", None),
        ])
        result = derive_field_positions("4-4-2", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        # No collisions within any amplua group
        by_amplua: dict[str, list[str]] = {}
        for s in starters:
            if s["amplua"] == "Gk":
                continue
            by_amplua.setdefault(s["amplua"], []).append(pos[s["player_id"]])

        for amp, positions in by_amplua.items():
            assert len(positions) == len(set(positions)), (
                f"Collision in amplua={amp}: {positions}"
            )


class TestParseFormationSuffixes:
    """Test formation strings with trailing text like 'classic', 'down'."""

    def test_442_classic(self):
        assert parse_formation("4-4-2 classic") == [4, 4, 2]

    def test_433_down(self):
        assert parse_formation("4-3-3 down") == [4, 3, 3]

    def test_442_diamond(self):
        assert parse_formation("4-4-2 diamond") == [4, 4, 2]

    def test_4231_with_spaces(self):
        assert parse_formation("  4-2-3-1  ") == [4, 2, 3, 1]


class TestDeriveFieldPositionsLargeGroup:
    """Test when SOTA amplua data is skewed (e.g., 7 D + 4 DM)."""

    def test_7d_no_collision_within_5(self):
        """7 defenders: first 5 get unique, 2 overflow to C."""
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", None), (4, "D", None),
            (5, "D", None), (6, "D", None), (7, "D", None), (8, "D", "ПЗ"),
            (9, "DM", None), (10, "DM", None), (11, "DM", None),
        ])
        result = derive_field_positions("4-2-3-1", starters)
        pos = _positions_by_player(result)

        assert len(result) == 11
        # DM group of 3 should have no collisions
        dm_positions = [pos[9], pos[10], pos[11]]
        assert len(set(dm_positions)) == 3

    def test_mismatched_amplua_still_assigns(self):
        """4 DM + 2 AM for 4-2-3-1 — each group gets unique slots."""
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", "ЛЗ"), (3, "D", "ЦЗ"), (4, "D", "ЦЗ"), (5, "D", "ПЗ"),
            (6, "DM", "ЛП"), (7, "DM", None), (8, "DM", None), (9, "DM", "ПП"),
            (10, "AM", None), (11, "F", None),
        ])
        result = derive_field_positions("4-2-3-1", starters)
        pos = _positions_by_player(result)

        # 4 DM → L, LC, RC, R — all unique
        dm_positions = [pos[6], pos[7], pos[8], pos[9]]
        assert len(set(dm_positions)) == 4


class TestDeriveFieldPositionsEdgeCases:
    def test_invalid_formation(self):
        starters = _make_starters([(1, "Gk", None)])
        assert derive_field_positions("invalid", starters) == []

    def test_none_formation(self):
        assert derive_field_positions(None, []) == []

    def test_empty_starters(self):
        assert derive_field_positions("4-4-2", []) == []

    def test_only_gk(self):
        starters = _make_starters([(1, "Gk", None)])
        result = derive_field_positions("4-4-2", starters)
        assert len(result) == 1
        assert result[0] == {"player_id": 1, "field_position": "C"}

    def test_missing_amplua_excluded(self):
        """Players with None amplua should be excluded."""
        starters = _make_starters([
            (1, "Gk", None),
            (2, "D", None),
            (3, None, None),  # no amplua — skip
        ])
        result = derive_field_positions("4-4-2", starters)
        pids = {r["player_id"] for r in result}
        assert 3 not in pids


# ---------------------------------------------------------------------------
# infer_formation
# ---------------------------------------------------------------------------

def _make_amplua_starters(specs: list[str]) -> list[dict]:
    """Helper: ["D", "D", "M", ...] -> starters list with amplua only."""
    return [{"amplua": amp} for amp in specs]


class TestInferFormation:
    def test_442(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "M", "M", "M", "M", "F", "F"]
        )
        assert infer_formation(starters) == "4-4-2"

    def test_4231(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "DM", "DM", "AM", "AM", "AM", "F"]
        )
        assert infer_formation(starters) == "4-2-3-1"

    def test_352(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "M", "M", "M", "M", "M", "F", "F"]
        )
        assert infer_formation(starters) == "3-5-2"

    def test_4141(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "DM", "M", "M", "M", "M", "F"]
        )
        assert infer_formation(starters) == "4-1-4-1"

    def test_343(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "M", "M", "M", "M", "F", "F", "F"]
        )
        assert infer_formation(starters) == "3-4-3"

    def test_532(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "D", "M", "M", "M", "F", "F"]
        )
        assert infer_formation(starters) == "5-3-2"

    def test_4321(self):
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "DM", "DM", "DM", "AM", "AM", "F"]
        )
        assert infer_formation(starters) == "4-3-2-1"

    def test_not_enough_outfield(self):
        """9 outfield players → None."""
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "M", "M", "M", "M", "F"]
        )
        assert infer_formation(starters) is None

    def test_too_many_outfield(self):
        """11 outfield players → None."""
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "M", "M", "M", "M", "F", "F", "F"]
        )
        assert infer_formation(starters) is None

    def test_empty(self):
        assert infer_formation([]) is None

    def test_only_gk(self):
        assert infer_formation([{"amplua": "Gk"}]) is None

    def test_unknown_amplua_ignored(self):
        """Unknown amplua values should be skipped."""
        starters = _make_amplua_starters(
            ["Gk", "D", "D", "D", "D", "M", "M", "M", "M", "F", "F"]
        )
        starters.append({"amplua": "UNKNOWN"})
        assert infer_formation(starters) == "4-4-2"

    def test_no_gk_still_works(self):
        """Input without Gk — only outfield matters."""
        starters = _make_amplua_starters(
            ["D", "D", "D", "D", "M", "M", "M", "M", "F", "F"]
        )
        assert infer_formation(starters) == "4-4-2"

    def test_none_amplua_ignored(self):
        starters = [{"amplua": None}] + _make_amplua_starters(
            ["D", "D", "D", "D", "M", "M", "M", "M", "F", "F"]
        )
        assert infer_formation(starters) == "4-4-2"
