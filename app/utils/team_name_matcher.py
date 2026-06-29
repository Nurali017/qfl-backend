from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)

_TEAM_TRANSLATION_TABLE = str.maketrans(
    {
        "ё": "е",
        "ә": "а",
        "ғ": "г",
        "қ": "к",
        "ң": "н",
        "ө": "о",
        "ұ": "у",
        "ү": "у",
        "һ": "х",
        "і": "и",
        "й": "и",
        "i": "и",  # Latin i (YouTube titles mix Latin/Cyrillic)
    }
)


def normalize_team_name(value: str | None) -> str:
    """Normalize team names for deterministic matching."""
    if not value:
        return ""
    normalized = value.casefold().translate(_TEAM_TRANSLATION_TABLE)
    normalized = _PUNCT_RE.sub(" ", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def _strip_trailing_marker(name: str) -> str | None:
    """Drop a trailing single-letter gender/region marker token.

    Broadcast feeds append a one-letter marker to women's/regional sides
    ("Каспий Ж", "Тұран Ә") that the stored name may omit or spell with a
    different letter ("Туран", "Каспий Ж" vs incoming "Каспий Ә"). Returns the
    name without that trailing token, or None when there is nothing to strip.
    Operates on already-normalized names (so "ә" is folded to "а").
    """
    tokens = name.split()
    if len(tokens) > 1 and len(tokens[-1]) == 1:
        return " ".join(tokens[:-1])
    return None


def _hyphen_head_variant(value: str) -> str | None:
    """For hyphenated compound club names, keep only the first component.

    Broadcast titles routinely abbreviate "Ертіс-Павлодар Ә" to "ЕРТІС Ә"
    (region suffix dropped) — return that collapsed form so it still matches.
    "Ертіс-Павлодар Ә" -> "Ертіс Ә"; "Кызыл-Жар" -> "Кызыл".
    """
    if "-" not in value:
        return None
    head, _, tail = value.partition("-")
    head = head.strip()
    if not head:
        return None
    tail_tokens = tail.split()
    # Drop the second compound token ("Павлодар"), keep any trailing markers
    rest = " ".join(tail_tokens[1:]) if len(tail_tokens) > 1 else ""
    collapsed = f"{head} {rest}".strip() if rest else head
    return collapsed


def _collect_team_names(team: Any) -> set[str]:
    if not team:
        return set()
    names: set[str] = set()
    for field in ("name", "name_kz", "name_en"):
        value = getattr(team, field, None)
        normalized = normalize_team_name(value)
        if normalized:
            names.add(normalized)
        if value:
            head_variant = normalize_team_name(_hyphen_head_variant(value))
            if head_variant:
                names.add(head_variant)
    return names


def _build_aliases(names: set[str]) -> set[str]:
    aliases: set[str] = set()
    for name in names:
        tokens = name.split()
        marker_stripped = _strip_trailing_marker(name)
        if marker_stripped:
            aliases.add(marker_stripped)
        if len(tokens) > 1 and tokens[0] in {"fc", "фк"}:
            aliases.add(" ".join(tokens[1:]))
    aliases.discard("")
    aliases.difference_update(names)
    return aliases


@dataclass(slots=True)
class TeamNameMatcher:
    home_team_id: int | None
    away_team_id: int | None
    home_names: set[str]
    away_names: set[str]
    home_aliases: set[str]
    away_aliases: set[str]

    @classmethod
    def from_game(cls, game: Any) -> "TeamNameMatcher":
        home_names = _collect_team_names(getattr(game, "home_team", None))
        away_names = _collect_team_names(getattr(game, "away_team", None))
        return cls(
            home_team_id=getattr(game, "home_team_id", None),
            away_team_id=getattr(game, "away_team_id", None),
            home_names=home_names,
            away_names=away_names,
            home_aliases=_build_aliases(home_names),
            away_aliases=_build_aliases(away_names),
        )

    def match_with_ambiguity(self, team_name: str | None) -> tuple[int | None, bool]:
        normalized = normalize_team_name(team_name)
        if not normalized:
            return None, False

        direct_candidates: set[int] = set()
        if self.home_team_id is not None and normalized in self.home_names:
            direct_candidates.add(self.home_team_id)
        if self.away_team_id is not None and normalized in self.away_names:
            direct_candidates.add(self.away_team_id)

        if len(direct_candidates) == 1:
            return next(iter(direct_candidates)), False
        if len(direct_candidates) > 1:
            return None, True

        alias_candidates: set[int] = set()
        if self.home_team_id is not None and normalized in self.home_aliases:
            alias_candidates.add(self.home_team_id)
        if self.away_team_id is not None and normalized in self.away_aliases:
            alias_candidates.add(self.away_team_id)

        if len(alias_candidates) == 1:
            return next(iter(alias_candidates)), False
        if len(alias_candidates) > 1:
            return None, True

        # Gender/region marker tolerance. Broadcast feeds carry a trailing
        # single-letter marker ("Тұран Ә", "Каспий Ә") that the stored name may
        # lack or spell differently ("Туран", "Каспий Ж"). `_build_aliases`
        # already strips that token from the stored side; mirror it on the
        # incoming side so both directions reconcile.
        stripped = _strip_trailing_marker(normalized)
        if stripped:
            marker_candidates: set[int] = set()
            if self.home_team_id is not None and (
                stripped in self.home_names or stripped in self.home_aliases
            ):
                marker_candidates.add(self.home_team_id)
            if self.away_team_id is not None and (
                stripped in self.away_names or stripped in self.away_aliases
            ):
                marker_candidates.add(self.away_team_id)

            if len(marker_candidates) == 1:
                return next(iter(marker_candidates)), False
            if len(marker_candidates) > 1:
                return None, True

        return None, False

    def match(self, team_name: str | None) -> int | None:
        team_id, _ = self.match_with_ambiguity(team_name)
        return team_id

