"""Helpers for assigning standings table zone labels."""

from typing import Literal

TableZone = Literal["champion", "euro_cups", "relegation"]


def _normalize_spots(value: int | None) -> int:
    if value is None:
        return 0
    return max(int(value), 0)


def resolve_table_zone(
    *,
    position: int | None,
    total_rows: int,
    champion_spots: int | None,
    euro_cup_spots: int | None,
    relegation_spots: int | None,
) -> TableZone | None:
    """Resolve zone for a single table row by position and season config."""
    if position is None or total_rows <= 0:
        return None

    champion_spots = _normalize_spots(champion_spots)
    euro_cup_spots = _normalize_spots(euro_cup_spots)
    relegation_spots = _normalize_spots(relegation_spots)

    if position <= champion_spots:
        return "champion"

    if position <= champion_spots + euro_cup_spots:
        return "euro_cups"

    if relegation_spots > 0 and position > total_rows - relegation_spots:
        return "relegation"

    return None
