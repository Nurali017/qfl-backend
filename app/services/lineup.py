"""Position/formation normalization helpers for game lineups."""

import re
from datetime import date as date_type


SUPPORTED_FORMATIONS = {
    "4-4-2", "4-3-3", "4-2-3-1", "3-5-2", "5-3-2", "3-4-3", "4-5-1",
    "4-1-4-1", "4-4-1-1", "3-4-1-2", "5-4-1", "4-3-2-1"
}

VALID_AMPLUA_VALUES = {"Gk", "D", "DM", "M", "AM", "F"}
VALID_FIELD_POSITION_VALUES = {"L", "LC", "C", "RC", "R"}
LINEUP_FIELD_ALLOWED_CHAMPIONSHIPS = {1, 2, 3, 5}
LINEUP_FIELD_CUTOFF_DATE = date_type(2025, 6, 1)
VALID_LINEUP_SOURCES = {"team_squad", "sota_api", "vsporte_api", "matches_players", "none"}

POSITION_CODE_TO_AMPLUA = {
    # Goalkeepers
    "GK": "Gk",
    "G": "Gk",
    "ВР": "Gk",
    "ГК": "Gk",
    "ВРТ": "Gk",
    # Defenders
    "CD": "D",
    "LD": "D",
    "RD": "D",
    "LB": "D",
    "RB": "D",
    "CB": "D",
    "D": "D",
    "ЛЗ": "D",
    "ПЗ": "D",
    "ЦЗ": "D",
    "ЗЩ": "D",
    "ЗАЩ": "D",
    # Defensive mid
    "DM": "DM",
    "ОП": "DM",
    # Midfield
    "CM": "M",
    "M": "M",
    "LM": "M",
    "RM": "M",
    "LW": "M",
    "RW": "M",
    "ЦП": "M",
    "ЛП": "M",
    "ПП": "M",
    # Attacking mid
    "AM": "AM",
    "АП": "AM",
    # Forwards
    "CF": "F",
    "ST": "F",
    "FW": "F",
    "F": "F",
    "ЦН": "F",
    "НП": "F",
    "ЦФ": "F",
    "НАП": "F",
    "ЛН": "F",
    "ПН": "F",
}


def normalize_amplua_value(amplua: str | None) -> str | None:
    if not isinstance(amplua, str):
        return None
    value = amplua.strip()
    return value if value in VALID_AMPLUA_VALUES else None


def normalize_field_position_value(field_position: str | None) -> str | None:
    if not isinstance(field_position, str):
        return None
    value = field_position.strip().upper()
    return value if value in VALID_FIELD_POSITION_VALUES else None


def infer_amplua_from_role_hint(role_hint: str | None) -> str | None:
    if not isinstance(role_hint, str) or not role_hint.strip():
        return None

    normalized = role_hint.strip().upper()
    token = (normalized.split(maxsplit=1)[0] if normalized else "").strip()
    code = re.sub(r"[^A-ZА-ЯЁ0-9]", "", token)

    mapped = POSITION_CODE_TO_AMPLUA.get(code)
    if mapped:
        return mapped

    if "ВРАТ" in normalized:
        return "Gk"
    if "ОПОР" in normalized:
        return "DM"
    if "ПОЛУЗАЩ" in normalized:
        return "M"
    if "ЗАЩИТ" in normalized or "ЗАЩ" in normalized:
        return "D"
    if "НАПАД" in normalized or "НАП" in normalized:
        return "F"

    return None


def infer_field_position_from_role_hint(role_hint: str | None) -> str | None:
    if not isinstance(role_hint, str) or not role_hint.strip():
        return None

    normalized = role_hint.strip().upper()
    token = (normalized.split(maxsplit=1)[0] if normalized else "").strip()
    code = re.sub(r"[^A-ZА-ЯЁ0-9]", "", token)

    if code in {"L", "LB", "LD", "LM", "LW", "ЛЗ", "ЛП", "ЛН"}:
        return "L"
    if code in {"R", "RB", "RD", "RM", "RW", "ПЗ", "ПП", "ПН"}:
        return "R"
    if code in {"LC", "ЛЦ", "ЛЦЗ"}:
        return "LC"
    if code in {"RC", "ПЦ", "ПЦЗ"}:
        return "RC"
    if code in {"C", "CB", "CD", "CM", "CF", "ST", "DM", "AM", "ЦЗ", "ЦП", "ЦН", "ЦФ", "ОП"}:
        return "C"

    has_left = "ЛЕВ" in normalized
    has_right = "ПРАВ" in normalized
    has_center = "ЦЕНТР" in normalized or "CENTER" in normalized
    if has_left and has_center:
        return "LC"
    if has_right and has_center:
        return "RC"
    if has_left:
        return "L"
    if has_right:
        return "R"
    if has_center:
        return "C"

    return None


def resolve_lineup_position_fallback(
    amplua: str | None,
    field_position: str | None,
    role_hint: str | None,
) -> tuple[str | None, str | None]:
    resolved_amplua = normalize_amplua_value(amplua) or infer_amplua_from_role_hint(role_hint)
    resolved_field_position = normalize_field_position_value(field_position) or infer_field_position_from_role_hint(role_hint)

    if resolved_amplua == "Gk" and resolved_field_position is None:
        resolved_field_position = "C"

    if resolved_amplua is not None and resolved_field_position is None:
        resolved_field_position = "C"

    if resolved_amplua is None and resolved_field_position is not None:
        resolved_amplua = "M"

    if resolved_amplua is None and resolved_field_position is None:
        resolved_amplua = "M"
        resolved_field_position = "C"

    return resolved_amplua, resolved_field_position


def normalize_formation(formation: str | None) -> str | None:
    """
    Normalize formation string from SOTA.
    Removes suffixes like ' down', ' up', extra spaces, etc.
    Returns None if formation is invalid.
    """
    if not formation:
        return None

    # Remove common suffixes and clean up
    cleaned = formation.lower().replace(" down", "").replace(" up", "").strip()

    # Extract just the numbers with dashes (e.g., "4-3-3")
    match = re.match(r'^[\d]+-[\d]+(?:-[\d]+)*', cleaned)
    if match:
        return match.group(0)

    return None


def detect_formation(positions: list[str | None]) -> str | None:
    """
    Detect team formation from player positions (amplua).
    Returns formation string like "4-2-3-1", "4-4-2", etc.

    Amplua codes from SOTA: Gk, D, DM, M, AM, F
    """
    if not positions or len(positions) < 10:
        return None

    defenders = 0
    defensive_mids = 0  # DM
    central_mids = 0    # M
    attacking_mids = 0  # AM
    forwards = 0

    for pos in positions[:11]:  # Only first 11 players
        if not pos:
            continue
        pos_upper = pos.upper()

        if pos_upper in ("GK",):
            continue  # Goalkeeper doesn't count
        elif pos_upper == "D":
            defenders += 1
        elif pos_upper == "DM":
            defensive_mids += 1
        elif pos_upper == "M":
            central_mids += 1
        elif pos_upper == "AM":
            attacking_mids += 1
        elif pos_upper == "F":
            forwards += 1

    total_outfield = defenders + defensive_mids + central_mids + attacking_mids + forwards
    if total_outfield < 10:
        return None

    # Determine formation based on composition
    total_mids = defensive_mids + central_mids + attacking_mids

    # 4-X-1 formations (4 defenders, 1 forward, varying midfield)
    if defenders == 4 and forwards == 1 and total_mids == 5:
        return "4-2-3-1"  # Standard modern formation
    elif defenders == 4 and forwards == 2:
        return "4-4-2"
    elif defenders == 4 and forwards == 3:
        return "4-3-3"
    elif defenders == 3 and forwards == 2:
        return "3-5-2"
    elif defenders == 5 and forwards == 2:
        return "5-3-2"
    else:
        # Fallback: simple D-M-F format
        return f"{defenders}-{total_mids}-{forwards}"


# Position order for sorting: line (GK→DEF→MID→FWD) + side (L→C→R)
POSITION_ORDER = {
    # Goalkeeper
    ('Gk', 'C'): 0, ('Gk', None): 0,
    # Defenders: L → LC → C → RC → R (positions 1-5)
    ('D', 'L'): 1,
    ('D', 'LC'): 2,
    ('D', 'C'): 3,
    ('D', 'RC'): 4,
    ('D', 'R'): 5,
    ('D', None): 3,
    # Holding line: DM first, then M with central/right positions
    ('DM', 'L'): 6,
    ('DM', 'LC'): 6,
    ('DM', 'C'): 6,
    ('DM', 'RC'): 7,
    ('DM', 'R'): 7,
    ('DM', None): 6,
    ('M', 'C'): 7,
    ('M', 'RC'): 7,
    ('M', 'R'): 8,
    # Attacking line: AM L first, then M LC as center, then AM R
    ('AM', 'L'): 9,
    ('AM', 'LC'): 9,
    ('M', 'L'): 10,
    ('M', 'LC'): 10,
    ('M', None): 10,
    ('AM', 'C'): 10,
    ('AM', 'RC'): 11,
    ('AM', 'R'): 11,
    ('AM', None): 10,
    # Forwards: L → C → R
    ('F', 'L'): 12,
    ('F', 'LC'): 12,
    ('F', 'C'): 13,
    ('F', 'RC'): 14,
    ('F', 'R'): 14,
    ('F', None): 13,
}


def get_position_order(amplua: str | None, field_pos: str | None) -> int:
    """Get sort order for a position (GK→DEF→MID→FWD, L→C→R)."""
    if not amplua:
        return 99
    return POSITION_ORDER.get((amplua, field_pos), POSITION_ORDER.get((amplua, None), 99))


def resolve_championship_gate_id(game) -> int | None:
    season = game.season
    championship = season.championship if season else None
    if championship is None:
        return None
    return championship.legacy_id if championship.legacy_id is not None else championship.id


def is_field_allowed_by_rules(game) -> bool:
    championship_gate_id = resolve_championship_gate_id(game)
    if game.date is None:
        return False
    return (
        championship_gate_id in LINEUP_FIELD_ALLOWED_CHAMPIONSHIPS
        and game.date >= LINEUP_FIELD_CUTOFF_DATE
    )


def team_has_valid_field_data(team_lineup: dict) -> bool:
    starters = team_lineup.get("starters") or []
    if len(starters) < 11:
        return False
    for player in starters[:11]:
        amplua = player.get("amplua")
        field_position = player.get("field_position")
        if amplua not in VALID_AMPLUA_VALUES:
            return False
        if field_position not in VALID_FIELD_POSITION_VALUES:
            return False
    return True


def has_any_lineup_data(home_lineup: dict, away_lineup: dict) -> bool:
    home_total = len(home_lineup.get("starters", [])) + len(home_lineup.get("substitutes", []))
    away_total = len(away_lineup.get("starters", [])) + len(away_lineup.get("substitutes", []))
    return (home_total + away_total) > 0


def normalize_lineup_source(raw_source: str | None, has_data: bool) -> str:
    if isinstance(raw_source, str) and raw_source in VALID_LINEUP_SOURCES:
        return raw_source
    if has_data:
        # Backward compatibility for historical data that was synced before source tracking.
        return "matches_players"
    return "none"
