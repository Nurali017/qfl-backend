import re
from collections import Counter
from dataclasses import dataclass


def _normalize(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[-_/()]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value


def infer_position_code(player_type: str | None, top_role: str | None) -> str | None:
    """
    Best-effort mapping for player position group.

    Returns one of: GK, DEF, MID, FWD, or None if unknown.
    """
    parts = [p for p in (player_type, top_role) if p]
    if not parts:
        return None

    normalized = _normalize(" ".join(parts))
    tokens = set(normalized.split())

    # GK
    if (
        "gk" in tokens
        or "goalkeeper" in tokens
        or "keeper" in tokens
        or "врат" in normalized
        or "қақп" in normalized
    ):
        return "GK"

    # MID (check before DEF to avoid "halfback" => "back")
    if (
        "halfback" in tokens
        or "mid" in tokens
        or "midfielder" in tokens
        or "полузащит" in normalized
        or "жартылай" in normalized
    ):
        return "MID"

    # DEF
    if (
        "def" in tokens
        or "defender" in tokens
        or "defence" in tokens
        or "defense" in tokens
        or "fullback" in tokens
        or ("centre" in tokens and "back" in tokens)
        or ("center" in tokens and "back" in tokens)
        or "back" in tokens
        or "защит" in normalized
        or "қорған" in normalized
    ):
        return "DEF"

    # FWD
    if (
        "fw" in tokens
        or "forward" in tokens
        or "striker" in tokens
        or "attacker" in tokens
        or "winger" in tokens
        or "напад" in normalized
        or "шабуыл" in normalized
    ):
        return "FWD"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Lineup-based position aggregation
# ─────────────────────────────────────────────────────────────────────────────

# Russian short codes used on the frontend mini-field.
# These are the standard football shorthand used in QFL content regardless of
# the UI language.
POSITION_SHORT = {
    "GK": "ВР",   # вратарь
    "LB": "ЛЗ",   # левый защитник
    "CB": "ЦЗ",   # центральный защитник
    "RB": "ПЗ",   # правый защитник
    "DM": "ОП",   # опорный полузащитник
    "LM": "ЛП",   # левый полузащитник
    "CM": "ЦП",   # центральный полузащитник
    "RM": "ПП",   # правый полузащитник
    "LAM": "ЛАП",  # левый атакующий полузащитник
    "AM": "АП",    # атакующий полузащитник
    "RAM": "ПАП",  # правый атакующий полузащитник
    "LF": "ЛН",   # левый нападающий
    "CF": "ЦН",   # центральный нападающий
    "RF": "ПН",   # правый нападающий
}


def _map_lineup_slot(amplua: str | None, field_position: str | None) -> str | None:
    """
    Map a raw (amplua, field_position) pair from GameLineup to an internal
    position key (LB/CB/RB/DM/...).

    SOTA values:
      amplua:         Gk, D, DM, M, AM, F
      field_position: C, L, R, LC, RC (or NULL)
    """
    if not amplua:
        return None

    amp = amplua.strip().lower()
    side = (field_position or "").strip().upper()

    if amp == "gk":
        return "GK"

    if amp == "d":
        if side == "L":
            return "LB"
        if side == "R":
            return "RB"
        # C, LC, RC, NULL → center back
        return "CB"

    if amp == "dm":
        if side == "L":
            return "LM"
        if side == "R":
            return "RM"
        # C, LC, RC, NULL → central defensive midfielder
        return "DM"

    if amp == "m":
        if side == "L":
            return "LM"
        if side == "R":
            return "RM"
        return "CM"

    if amp == "am":
        if side == "L":
            return "LAM"
        if side == "R":
            return "RAM"
        return "AM"

    if amp == "f":
        if side == "L":
            return "LF"
        if side == "R":
            return "RF"
        # C, LC, RC, NULL → center forward
        return "CF"

    return None


@dataclass(frozen=True)
class AggregatedPositions:
    primary: str | None                 # short code like "АП", "ЦЗ"
    secondary: tuple[str, ...]          # up to 2 extra short codes
    sample_size: int                    # how many lineup rows contributed
    source: str                         # "lineups" or "top_role" or "unknown"


def aggregate_lineup_positions(
    lineups: list[tuple[str | None, str | None]],
    *,
    primary_threshold: float = 0.0,     # primary = most frequent, always set
    secondary_threshold: float = 0.15,  # secondary needs >=15% share
    max_secondary: int = 2,
) -> AggregatedPositions:
    """
    Aggregate lineup (amplua, field_position) pairs into primary + secondary
    position short codes.

    - Primary = single most frequent position (regardless of threshold, so a
      player with even 1 sample still gets a primary).
    - Secondary = other positions with share >= secondary_threshold, limited
      to `max_secondary` entries.

    Returns AggregatedPositions with source="lineups" if any pair mapped,
    otherwise source="unknown" with primary=None.
    """
    del primary_threshold  # reserved for future tuning; not currently used
    counter: Counter[str] = Counter()
    contributed = 0
    for amplua, field_position in lineups:
        slot = _map_lineup_slot(amplua, field_position)
        if slot is None:
            continue
        counter[slot] += 1
        contributed += 1

    if contributed == 0:
        return AggregatedPositions(
            primary=None, secondary=(), sample_size=0, source="unknown"
        )

    ordered = counter.most_common()
    primary_slot, _ = ordered[0]
    primary = POSITION_SHORT.get(primary_slot)

    secondary: list[str] = []
    for slot, count in ordered[1:]:
        share = count / contributed
        if share < secondary_threshold:
            continue
        short = POSITION_SHORT.get(slot)
        if short and short != primary:
            secondary.append(short)
        if len(secondary) >= max_secondary:
            break

    return AggregatedPositions(
        primary=primary,
        secondary=tuple(secondary),
        sample_size=contributed,
        source="lineups",
    )


# Map `infer_position_code` group codes (GK/DEF/MID/FWD) to the default
# short Russian code when we only have top_role fallback data.
_GROUP_TO_SHORT = {
    "GK": "ВР",
    "DEF": "ЦЗ",
    "MID": "ЦП",
    "FWD": "ЦН",
}


def fallback_positions_from_top_role(
    player_type: str | None, top_role: str | None
) -> AggregatedPositions:
    """
    Build an AggregatedPositions result from admin-entered top_role when no
    lineup data is available.
    """
    group = infer_position_code(player_type, top_role)
    short = _GROUP_TO_SHORT.get(group) if group else None
    return AggregatedPositions(
        primary=short,
        secondary=(),
        sample_size=0,
        source="top_role" if short else "unknown",
    )

