"""
Derive field_position from formation + top_role.

Given a formation string (e.g. "4-2-3-1") and a list of starters with their
amplua and top_role, assigns unique (amplua, field_position) pairs so that
no two outfield players on the same team share the same slot.

Pure utility — no DB dependencies.
"""

from __future__ import annotations

import re

# Slot templates by count of players in a group
SLOT_TEMPLATES: dict[int, list[str]] = {
    1: ["C"],
    2: ["LC", "RC"],
    3: ["L", "C", "R"],
    4: ["L", "LC", "RC", "R"],
    5: ["L", "LC", "C", "RC", "R"],
}

# Canonical amplua ordering (defense → attack)
AMPLUA_ORDER: dict[str, int] = {
    "D": 0,
    "DM": 1,
    "M": 2,
    "AM": 3,
    "F": 4,
}

# Lateral sort key extracted from top_role (Russian position codes)
# Lower value = more left
_LATERAL_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"\bЛ[ЗНПЦ]?\b", re.IGNORECASE), 0),   # Л, ЛЗ, ЛН, ЛП, ЛЦ
    (re.compile(r"\bL[BWDM]?\b", re.IGNORECASE), 0),     # L, LB, LW, LD, LM
    (re.compile(r"ЛЕВ", re.IGNORECASE), 0),               # Левый
    (re.compile(r"\bП[ЗНП]?\b", re.IGNORECASE), 4),       # П, ПЗ, ПН, ПП
    (re.compile(r"\bR[BWDM]?\b", re.IGNORECASE), 4),      # R, RB, RW, RD, RM
    (re.compile(r"ПРАВ", re.IGNORECASE), 4),               # Правый
]


def infer_formation(starters: list[dict]) -> str | None:
    """
    Infer formation string from starter amplua counts (excluding Gk).

    Args:
        starters: List of dicts with at least an "amplua" key.
                  Only outfield players (amplua != "Gk") are considered.

    Returns:
        Formation string like "4-4-2" or "4-2-3-1", or None if invalid.
    """
    # Count players by amplua, skipping Gk and unknown
    counts: dict[str, int] = {}
    for s in starters:
        amp = s.get("amplua")
        if not amp or amp == "Gk" or amp not in AMPLUA_ORDER:
            continue
        counts[amp] = counts.get(amp, 0) + 1

    if not counts:
        return None

    # Must sum to exactly 10 outfield players
    if sum(counts.values()) != 10:
        return None

    # Sort by AMPLUA_ORDER (D → DM → M → AM → F), skip zero-count
    layers = [
        str(counts[amp])
        for amp in sorted(counts, key=lambda a: AMPLUA_ORDER[a])
    ]

    return "-".join(layers)


def parse_formation(formation: str | None) -> list[int] | None:
    """
    Parse a formation string like "4-2-3-1" or "4-4-2 classic" into [4, 2, 3, 1].

    Strips trailing text after digit-dash pattern (e.g. "classic", "down", "diamond").
    Returns None if invalid or sum != 10.
    """
    if not formation or not isinstance(formation, str):
        return None
    formation = formation.strip()
    m = re.match(r"^(\d+(?:-\d+)+)", formation)
    if not m:
        return None
    formation = m.group(1)
    layers = [int(x) for x in formation.split("-")]
    if sum(layers) != 10:
        return None
    return layers


def get_lateral_hint(top_role: str | None) -> int:
    """
    Extract lateral hint from top_role string.

    Returns: 0=L, 1=LC, 2=C (default), 3=RC, 4=R
    """
    if not top_role or not isinstance(top_role, str):
        return 2  # center by default

    # Extract the first token (position code)
    token = top_role.strip().split()[0] if top_role.strip() else ""

    for pattern, value in _LATERAL_PATTERNS:
        if pattern.search(token):
            return value

    # Check full string for left/right keywords
    upper = top_role.upper()
    if "ЛЕВ" in upper or "LEFT" in upper:
        return 0
    if "ПРАВ" in upper or "RIGHT" in upper:
        return 4

    return 2  # center by default


def derive_field_positions(
    formation: str | None,
    starters: list[dict],
) -> list[dict]:
    """
    Assign unique field_position to each starter based on formation and top_role.

    Algorithm:
    - GK always gets "C"
    - Group outfield players by amplua
    - For each amplua group of size N, pick the N-slot template
    - Sort players within each group by lateral hint from top_role
    - Assign slots left-to-right

    This guarantees no (amplua, field_position) collisions for groups of 1..5 players.

    Args:
        formation: Formation string, e.g. "4-2-3-1" or "4-4-2 classic"
        starters: List of dicts with keys: player_id, amplua, top_role

    Returns:
        List of dicts: [{player_id, field_position}, ...]
        Empty list if formation is invalid.
    """
    layers = parse_formation(formation)
    if layers is None:
        return []

    result: list[dict] = []

    # GK always gets "C"
    for s in starters:
        if s.get("amplua") == "Gk":
            result.append({"player_id": s["player_id"], "field_position": "C"})

    # Group outfield players by amplua
    groups: dict[str, list[dict]] = {}
    for s in starters:
        amp = s.get("amplua")
        if amp and amp != "Gk" and amp in AMPLUA_ORDER:
            groups.setdefault(amp, []).append(s)

    # For each amplua group: assign unique slots based on group size
    for amp in sorted(groups, key=lambda a: AMPLUA_ORDER[a]):
        players = groups[amp]
        n = len(players)
        slots = SLOT_TEMPLATES.get(n, _generate_slots(n))

        # Sort by lateral hint: leftmost player → leftmost slot
        players_sorted = sorted(
            players, key=lambda p: get_lateral_hint(p.get("top_role"))
        )

        for i, player in enumerate(players_sorted):
            slot = slots[i] if i < len(slots) else "C"
            result.append({"player_id": player["player_id"], "field_position": slot})

    return result


def _generate_slots(count: int) -> list[str]:
    """Generate slot labels for unusual group sizes (>5)."""
    if count <= 0:
        return []
    if count in SLOT_TEMPLATES:
        return SLOT_TEMPLATES[count]
    # >5 players in one amplua group: use all 5 unique + repeat center
    result = ["L", "LC"]
    for _ in range(count - 4):
        result.append("C")
    result.extend(["RC", "R"])
    return result
