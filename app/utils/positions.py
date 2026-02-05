import re


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

