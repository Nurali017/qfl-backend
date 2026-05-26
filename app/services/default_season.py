from typing import Iterable

TOURNAMENT_PRIORITY: dict[str, int] = {
    "pl": 0,
    "1l": 1,
    "2l": 2,
    "el": 3,
    "cup": 4,
}

_UNKNOWN_PRIORITY = len(TOURNAMENT_PRIORITY)


def _priority(frontend_code: str | None) -> int:
    if frontend_code is None:
        return _UNKNOWN_PRIORITY
    return TOURNAMENT_PRIORITY.get(frontend_code, _UNKNOWN_PRIORITY)


def pick_default_season(
    entries: Iterable[tuple[int, int | None, str | None]],
) -> int | None:
    """Pick the default season for a player or team.

    Priority: newest season_year first, then tournament priority
    (pl > 1l > 2l > el > cup, unknown codes last), tie-break by season_id desc.

    Args:
        entries: iterable of (season_id, season_year, frontend_code) tuples.
    Returns:
        Winning season_id, or None if entries is empty.
    """
    best: tuple[int, int, int] | None = None  # (-year, priority, -id)
    best_id: int | None = None
    for season_id, year, code in entries:
        year_key = -(year if year is not None else -10_000)
        key = (year_key, _priority(code), -season_id)
        if best is None or key < best:
            best = key
            best_id = season_id
    return best_id


def pick_active_season_by_playtime(
    candidates: Iterable[tuple[int, int | None, str | None]],
    playtime: dict[int, tuple[int, int]],
) -> int | None:
    """Pick the season where the player plays most among active candidates.

    Used for the "current league" of a player who holds several active contracts
    in the current season (e.g. youth registered in both Premier and First League).
    Ranks by minutes desc, then games desc; ties — including the start-of-season
    case where nobody has playtime yet — fall back to ``pick_default_season``
    (league priority + recency).

    Args:
        candidates: iterable of (season_id, season_year, frontend_code) tuples.
        playtime: mapping season_id -> (minutes, games).
    Returns:
        Winning season_id, or None if candidates is empty.
    """
    cands = list(candidates)
    if not cands:
        return None

    def _key(candidate: tuple[int, int | None, str | None]) -> tuple[int, int]:
        return playtime.get(candidate[0], (0, 0))

    best = max(_key(c) for c in cands)
    top = [c for c in cands if _key(c) == best]
    return pick_default_season(top)
