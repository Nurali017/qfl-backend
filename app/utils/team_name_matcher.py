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


def _collect_team_names(team: Any) -> set[str]:
    if not team:
        return set()
    names: set[str] = set()
    for field in ("name", "name_kz", "name_en"):
        value = getattr(team, field, None)
        normalized = normalize_team_name(value)
        if normalized:
            names.add(normalized)
    return names


def _build_aliases(names: set[str]) -> set[str]:
    aliases: set[str] = set()
    for name in names:
        tokens = name.split()
        if len(tokens) > 1 and len(tokens[-1]) == 1:
            aliases.add(" ".join(tokens[:-1]))
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

        return None, False

    def match(self, team_name: str | None) -> int | None:
        team_id, _ = self.match_with_ambiguity(team_name)
        return team_id

