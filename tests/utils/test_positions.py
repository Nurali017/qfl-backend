"""Unit tests for backend/app/utils/positions.py lineup aggregation."""
from app.utils.positions import (
    AggregatedPositions,
    aggregate_lineup_positions,
    fallback_positions_from_top_role,
)


class TestMapLineupSlot:
    """Via aggregate_lineup_positions with a single sample."""

    def test_goalkeeper(self):
        result = aggregate_lineup_positions([("Gk", "C")])
        assert result.primary == "ВР"
        assert result.secondary == ()
        assert result.sample_size == 1
        assert result.source == "lineups"

    def test_center_back_from_c(self):
        assert aggregate_lineup_positions([("D", "C")]).primary == "ЦЗ"

    def test_center_back_from_lc_rc(self):
        # Left-center and right-center are still center backs (pair of CBs).
        assert aggregate_lineup_positions([("D", "LC")]).primary == "ЦЗ"
        assert aggregate_lineup_positions([("D", "RC")]).primary == "ЦЗ"

    def test_left_back(self):
        assert aggregate_lineup_positions([("D", "L")]).primary == "ЛЗ"

    def test_right_back(self):
        assert aggregate_lineup_positions([("D", "R")]).primary == "ПЗ"

    def test_defensive_mid_central(self):
        # C / LC / RC / NULL → real central defensive midfielder
        assert aggregate_lineup_positions([("DM", "C")]).primary == "ОП"
        assert aggregate_lineup_positions([("DM", "LC")]).primary == "ОП"
        assert aggregate_lineup_positions([("DM", "RC")]).primary == "ОП"
        assert aggregate_lineup_positions([("DM", None)]).primary == "ОП"

    def test_defensive_mid_wide_is_wing_midfielder(self):
        # SOTA "DM+L/R" → Russian terminology has no "wide defensive mid";
        # any wide midfielder is just ЛП / ПП.
        assert aggregate_lineup_positions([("DM", "L")]).primary == "ЛП"
        assert aggregate_lineup_positions([("DM", "R")]).primary == "ПП"

    def test_central_mid_variants(self):
        assert aggregate_lineup_positions([("M", "C")]).primary == "ЦП"
        assert aggregate_lineup_positions([("M", "L")]).primary == "ЛП"
        assert aggregate_lineup_positions([("M", "R")]).primary == "ПП"
        assert aggregate_lineup_positions([("M", None)]).primary == "ЦП"

    def test_attacking_mid_variants(self):
        assert aggregate_lineup_positions([("AM", "C")]).primary == "АП"
        assert aggregate_lineup_positions([("AM", "L")]).primary == "ЛАП"
        assert aggregate_lineup_positions([("AM", "R")]).primary == "ПАП"
        assert aggregate_lineup_positions([("AM", None)]).primary == "АП"
        assert aggregate_lineup_positions([("AM", "LC")]).primary == "АП"
        assert aggregate_lineup_positions([("AM", "RC")]).primary == "АП"

    def test_forward_variants(self):
        assert aggregate_lineup_positions([("F", "C")]).primary == "ЦН"
        assert aggregate_lineup_positions([("F", "LC")]).primary == "ЦН"
        assert aggregate_lineup_positions([("F", "RC")]).primary == "ЦН"
        assert aggregate_lineup_positions([("F", "L")]).primary == "ЛН"
        assert aggregate_lineup_positions([("F", "R")]).primary == "ПН"

    def test_unknown_amplua_ignored(self):
        result = aggregate_lineup_positions([("XX", "C")])
        assert result.primary is None
        assert result.source == "unknown"

    def test_null_amplua_ignored(self):
        result = aggregate_lineup_positions([(None, "C")])
        assert result.primary is None
        assert result.sample_size == 0


class TestAggregation:
    """Multi-sample aggregation: primary, secondary, thresholds."""

    def test_single_position_dominant(self):
        # 12x AP + 1x CF → primary=АП, secondary=[] (CF share 7.7% < 15%)
        lineups = [("AM", "C")] * 12 + [("F", "C")]
        result = aggregate_lineup_positions(lineups)
        assert result.primary == "АП"
        assert result.secondary == ()
        assert result.sample_size == 13

    def test_secondary_at_threshold(self):
        # 8x CB + 2x LB → primary=ЦЗ, secondary=[ЛЗ] (ЛЗ share 20% >= 15%)
        lineups = [("D", "C")] * 8 + [("D", "L")] * 2
        result = aggregate_lineup_positions(lineups)
        assert result.primary == "ЦЗ"
        assert result.secondary == ("ЛЗ",)

    def test_secondary_below_threshold_excluded(self):
        # 9x CB + 1x LB → LB share 10% < 15% → no secondary
        lineups = [("D", "C")] * 9 + [("D", "L")]
        result = aggregate_lineup_positions(lineups)
        assert result.primary == "ЦЗ"
        assert result.secondary == ()

    def test_max_two_secondary(self):
        # 5x CB + 2x LB + 2x RB + 1x DM → expect primary=ЦЗ, secondary=[ЛЗ, ПЗ]
        lineups = (
            [("D", "C")] * 5
            + [("D", "L")] * 2
            + [("D", "R")] * 2
            + [("DM", "C")]
        )
        result = aggregate_lineup_positions(lineups)
        assert result.primary == "ЦЗ"
        assert len(result.secondary) == 2
        assert set(result.secondary) == {"ЛЗ", "ПЗ"}

    def test_ignores_substitutes_with_null_amplua(self):
        # A real 1646-like mix including 3 substitute rows with empty amplua
        lineups = (
            [("AM", "C")] * 12
            + [("AM", "L")]
            + [("F", "RC")]
            + [(None, None)] * 3
        )
        result = aggregate_lineup_positions(lineups)
        assert result.primary == "АП"
        # 13 АП, 1 ЦН; ЦН share = 1/14 = 7.1% < 15% → no secondary
        assert result.secondary == ()
        assert result.sample_size == 14
        assert result.source == "lineups"

    def test_empty_lineups(self):
        result = aggregate_lineup_positions([])
        assert result == AggregatedPositions(
            primary=None, secondary=(), sample_size=0, source="unknown"
        )


class TestTopRoleFallback:
    def test_forward_keyword(self):
        result = fallback_positions_from_top_role(None, "ЦН (центральный нападающий)")
        assert result.primary == "ЦН"
        assert result.secondary == ()
        assert result.sample_size == 0
        assert result.source == "top_role"

    def test_defender_keyword(self):
        assert (
            fallback_positions_from_top_role(None, "Защитник").primary == "ЦЗ"
        )

    def test_goalkeeper_keyword(self):
        assert (
            fallback_positions_from_top_role(None, "Вратарь").primary == "ВР"
        )

    def test_unknown_returns_source_unknown(self):
        result = fallback_positions_from_top_role(None, None)
        assert result.primary is None
        assert result.source == "unknown"
