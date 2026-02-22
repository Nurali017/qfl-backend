from __future__ import annotations

import json
import re
from typing import Any

STARTING_MARKERS = {"ОСНОВНЫЕ", "STARTING"}
SUBS_MARKERS = {"ЗАПАСНЫЕ", "SUBS"}
COACH_MARKERS = {"COACH", "MAIN"}
FORMATION_MARKER = "FORMATION"
TEAM_MARKER = "TEAM"
ASSISTANT_PREFIX = "ASSISTANT"
VALID_AMPLUA_VALUES = {"Gk", "D", "DM", "M", "AM", "F"}
VALID_FIELD_POSITION_VALUES = {"L", "LC", "C", "RC", "R"}
HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


def _norm_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def _build_key_index(entry: dict[str, Any]) -> dict[str, Any]:
    return {_norm_key(k): v for k, v in entry.items()}


def _get_ci(entry: dict[str, Any], *aliases: str, default: Any = None) -> Any:
    normalized = _build_key_index(entry)
    for alias in aliases:
        key = _norm_key(alias)
        if key in normalized:
            return normalized[key]
    return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n", ""}:
            return False
    return bool(value)


def _to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return int(value)
    return None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


def normalize_lineup_entry(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    number_raw = _get_ci(raw, "number", "Number")
    number_text = _clean_text(number_raw)
    number_upper = number_text.upper()
    number_int = _to_int(number_raw)

    first_name = _clean_text(_get_ci(raw, "first_name", "First_name", "firstname"))
    last_name = _clean_text(_get_ci(raw, "last_name", "Last_name", "lastname"))
    full_name = _clean_text(_get_ci(raw, "full_name", "Full_name", "fullname"))
    amplua_raw = _clean_text(_get_ci(raw, "amplua", "Amplua"))
    position_raw = _clean_text(_get_ci(raw, "position", "Position"))
    player_id = _clean_text(_get_ci(raw, "id", "Id", "player_id", "Player_id"))

    gk_raw = _get_ci(raw, "gk", "Gk", "is_gk", "Is_gk", default=False)
    captain_raw = _get_ci(raw, "capitan", "Capitan", "captain", "Captain", "is_captain", "Is_captain", default=False)

    amplua = amplua_raw.upper()
    if amplua == "GK":
        amplua = "Gk"
    elif amplua not in {v.upper() for v in VALID_AMPLUA_VALUES}:
        amplua = ""
    elif amplua != "Gk":
        # Preserve canonical uppercase non-GK values
        amplua = amplua

    if amplua == "GK":
        amplua = "Gk"

    position = position_raw.upper()
    if position not in VALID_FIELD_POSITION_VALUES:
        position = ""

    return {
        "number_raw": number_raw,
        "number_text": number_text,
        "number_upper": number_upper,
        "number_int": number_int,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "name": full_name or f"{first_name} {last_name}".strip(),
        "amplua": amplua,
        "position": position,
        "id": player_id,
        "gk": _to_bool(gk_raw),
        "capitan": _to_bool(captain_raw),
    }


def parse_team_lineup_feed(payload: Any) -> dict[str, Any]:
    main: list[dict[str, Any]] = []
    subs: list[dict[str, Any]] = []
    assistants: list[str] = []
    coach: str | None = None
    team_name: str | None = None
    formation_string: str | None = None
    formation_color: str | None = None

    if not isinstance(payload, list):
        return {
            "main": main,
            "subs": subs,
            "assistants": assistants,
            "coach": coach,
            "team_name": team_name,
            "formation_string": formation_string,
            "formation_color": formation_color,
            "has_starting_marker": False,
            "has_subs_marker": False,
            "is_valid_for_field": False,
        }

    is_main = False
    is_subs = False
    has_starting_marker = False
    has_subs_marker = False

    for raw_entry in payload:
        entry = normalize_lineup_entry(raw_entry)
        if entry is None:
            continue

        number_upper = entry["number_upper"]
        if number_upper in COACH_MARKERS:
            name = entry["name"]
            if name:
                coach = name
            continue

        if number_upper.startswith(ASSISTANT_PREFIX):
            if entry["name"]:
                assistants.append(entry["name"])
            continue

        if number_upper in STARTING_MARKERS:
            has_starting_marker = True
            is_main = True
            is_subs = False
            continue

        if number_upper in SUBS_MARKERS:
            has_subs_marker = True
            is_main = False
            is_subs = True
            continue

        if number_upper == FORMATION_MARKER:
            formation = _clean_text(entry["first_name"])
            color = _clean_text(entry["full_name"]).upper()
            formation_string = formation or None
            formation_color = color if HEX_COLOR_RE.match(color) else None
            continue

        if number_upper == TEAM_MARKER:
            team_name = entry["first_name"] or entry["name"] or None
            continue

        if entry["number_int"] is None:
            continue

        player = {
            "number": entry["number_int"],
            "name": entry["name"],
            "first_name": entry["first_name"],
            "last_name": entry["last_name"],
            "amplua": "Gk" if entry["gk"] else (entry["amplua"] or ""),
            "position": entry["position"],
            "id": entry["id"],
            "gk": entry["gk"],
            "capitan": entry["capitan"],
        }

        if is_main:
            main.append(player)
        elif is_subs:
            subs.append(player)

    is_valid_for_field = (
        has_starting_marker
        and has_subs_marker
        and len(main) >= 11
        and all(p.get("amplua") and p.get("position") for p in main[:11])
    )

    return {
        "main": main,
        "subs": subs,
        "assistants": assistants,
        "coach": coach,
        "team_name": team_name,
        "formation_string": formation_string,
        "formation_color": formation_color,
        "has_starting_marker": has_starting_marker,
        "has_subs_marker": has_subs_marker,
        "is_valid_for_field": is_valid_for_field,
    }


def parse_maybe_json_array(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, list):
            return parsed
    return None

