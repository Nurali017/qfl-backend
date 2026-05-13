"""Kit colors per match — sourced from the clubs' match-ops system (apps.kffleague.kz).

`games.home_kit_color` / `games.away_kit_color` are HEX strings rendered as the
solid shirt colour on the public lineup pitch.  apps.kffleague.kz stores, per
match and per team, a reference to a uniform PNG icon (plus a Russian colour
name).  We derive the HEX from the dominant colour of that PNG.

This module holds the source-agnostic core:
  * team-name normalisation + matching an apps fixture to a QFL `Game`
  * extracting a HEX colour from a uniform PNG
  * applying the colour to a `Game`
  * `sync_kits_for_matches` — iterate `AppsMatchKit` records (built either from a
    SQL dump or from a live MySQL query) and update games.

The two callers (`scripts/import_apps_kit_colors.py` and
`app/services/apps_kit_mysql_sync.py`) supply a `fetch_png` callable that knows
where to read the PNG bytes from (local backup dir vs the public URL).
"""

from __future__ import annotations

import io
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, time
from typing import Callable, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.models import Game, Team

logger = logging.getLogger(__name__)

HEX_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")

# Canonicalise team names where apps.kffleague.kz and QFL spell a club
# differently (different writing system, transliteration, etc.). Keys/values are
# already-normalised forms (output of the inner normalise step). Applied to both
# sides, so it just needs to be idempotent and collision-free. Extend as the
# import dry-run surfaces more misses.
TEAM_NAME_ALIASES: dict[str, str] = {
    "тобол": "тобыл",        # apps "Тобол" → QFL "Тобыл"
    "тоболм": "тобылм",      # apps "ТОБОЛ М" → QFL "Тобыл М"
    "turkistan": "туркестан",  # apps Latin "Turkistan" → QFL Cyrillic "Туркестан"
    "akas": "акас",          # apps Latin "Akas" → QFL Cyrillic "АКАС"
    "sdfamilyм": "sdfamilym",  # apps writes a Cyrillic "М" suffix
}

# Side label used by callers / counters.
Side = str  # "home" | "away"


# ---------------------------------------------------------------------------
# team-name normalisation & game matching
# ---------------------------------------------------------------------------

_STRIP_CHARS = "«»\"'`“”„ \t\n"
_DROP_RE = re.compile(r"[\s\-_.·•]+")


def normalize_team_name(name: str | None) -> str:
    """Canonical form for fuzzy team-name comparison.

    Strips guillemets/quotes, lowercases, folds ``ё→е``, drops separators
    (spaces, hyphens, dots), then applies :data:`TEAM_NAME_ALIASES`.
    """
    if not name:
        return ""
    s = str(name).strip().strip(_STRIP_CHARS)
    s = s.lower().replace("ё", "е")
    s = _DROP_RE.sub("", s)
    return TEAM_NAME_ALIASES.get(s, s)


async def _games_on(db: AsyncSession, on_date: date) -> list[tuple[Game, str, str]]:
    home_t = aliased(Team)
    away_t = aliased(Team)
    return list(
        (
            await db.execute(
                select(Game, home_t.name, away_t.name)
                .join(home_t, home_t.id == Game.home_team_id)
                .join(away_t, away_t.id == Game.away_team_id)
                .where(Game.date == on_date)
            )
        ).all()
    )


async def match_game(
    db: AsyncSession,
    *,
    match_date: date,
    home_name: str,
    away_name: str,
    match_time: time | None = None,
) -> Game | None:
    """Find the QFL :class:`Game` for an apps fixture.

    Matches on ``date`` + normalised home/away team names.  If nothing is found
    on the exact date, retries on ``date ± 1`` (apps stores the *scheduled*
    date, which can drift by a day after postponements) — but only accepts a
    ±1-day result when it is unambiguous across both neighbouring days.  Returns
    ``None`` (without raising) when there is no unambiguous match.
    """
    home_norm = normalize_team_name(home_name)
    away_norm = normalize_team_name(away_name)
    if not home_norm or not away_norm:
        return None

    def _filter(rows: list[tuple[Game, str, str]]) -> list[Game]:
        return [
            game
            for (game, home_db_name, away_db_name) in rows
            if normalize_team_name(home_db_name) == home_norm
            and normalize_team_name(away_db_name) == away_norm
        ]

    candidates = _filter(await _games_on(db, match_date))

    if not candidates:
        # ±1 day fallback (postponements). Require a single match overall.
        from datetime import timedelta

        neighbour: list[Game] = []
        for delta in (-1, 1):
            neighbour.extend(_filter(await _games_on(db, match_date + timedelta(days=delta))))
        if len(neighbour) == 1:
            return neighbour[0]
        return None

    if len(candidates) == 1:
        return candidates[0]

    if match_time is not None:
        timed = [g for g in candidates if g.time == match_time]
        if len(timed) == 1:
            return timed[0]

    logger.warning(
        "Ambiguous apps→QFL game match: %s %s vs %s -> %d candidates",
        match_date, home_name, away_name, len(candidates),
    )
    return None


# ---------------------------------------------------------------------------
# colour extraction
# ---------------------------------------------------------------------------

def is_hex_color(value: str | None) -> bool:
    return bool(value) and bool(HEX_RE.match(value.strip()))


def _rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{max(0, min(255, r)):02X}{max(0, min(255, g)):02X}{max(0, min(255, b)):02X}"


def _dominant_via_pillow(data: bytes) -> str | None:
    """Most-common opaque, non-near-white/black pixel colour."""
    try:
        from PIL import Image
    except Exception:  # pragma: no cover - Pillow is a hard dep
        return None
    try:
        img = Image.open(io.BytesIO(data)).convert("RGBA")
    except Exception:
        return None
    # Downscale for speed; nearest keeps colours crisp.
    img.thumbnail((96, 96))
    counter: Counter[tuple[int, int, int]] = Counter()
    for r, g, b, a in img.getdata():
        if a < 128:
            continue
        if r > 245 and g > 245 and b > 245:  # near-white background
            continue
        if r < 12 and g < 12 and b < 12:  # near-black outline
            continue
        # Snap to an 8-step grid so anti-aliased edges cluster together.
        counter[(r & ~7, g & ~7, b & ~7)] += 1
    if not counter:
        return None
    (r, g, b), _ = counter.most_common(1)[0]
    # Re-centre the bucket.
    return _rgb_to_hex(r + 4, g + 4, b + 4)


def extract_hex_from_png(data: bytes | None) -> str | None:
    """Dominant colour of a uniform PNG icon as ``#RRGGBB`` (uppercase).

    Tries ``colorthief`` first, falls back to a Pillow histogram, and returns
    ``None`` when neither can produce a sensible colour (caller leaves the field
    untouched rather than writing a wrong colour).
    """
    if not data:
        return None
    try:
        from colorthief import ColorThief

        color = ColorThief(io.BytesIO(data)).get_color(quality=1)
        if color and len(color) >= 3:
            return _rgb_to_hex(int(color[0]), int(color[1]), int(color[2]))
    except Exception as exc:  # noqa: BLE001 - colorthief raises bare Exception
        logger.debug("colorthief failed (%s); falling back to Pillow", exc)
    return _dominant_via_pillow(data)


# ---------------------------------------------------------------------------
# applying colours
# ---------------------------------------------------------------------------

def apply_kit_color(
    game: Game,
    side: Side,
    hex_color: str | None,
    *,
    allow_overwrite: bool = True,
) -> bool:
    """Set ``home_kit_color`` / ``away_kit_color`` on *game*. Returns True if changed.

    ``allow_overwrite=False`` only fills the field when it is currently empty.
    """
    if not is_hex_color(hex_color):
        return False
    hex_color = hex_color.strip().upper()
    attr = "home_kit_color" if side == "home" else "away_kit_color"
    current = getattr(game, attr)
    if current == hex_color:
        return False
    if current and not allow_overwrite:
        return False
    setattr(game, attr, hex_color)
    return True


# ---------------------------------------------------------------------------
# orchestration over apps fixtures
# ---------------------------------------------------------------------------

@dataclass
class AppsMatchKit:
    """A single apps fixture's kit references (field-player shirt only)."""

    apps_match_id: int
    match_date: date
    home_name: str
    away_name: str
    match_time: time | None = None
    # Uniform PNG filename for the field-player shirt of each side; None if the
    # club didn't enter a kit for that side.
    home_image: str | None = None
    away_image: str | None = None
    # Russian colour names (kept only for dry-run readability / debugging).
    home_color_name: str | None = None
    away_color_name: str | None = None


@dataclass
class SyncResult:
    matches_seen: int = 0
    matched_games: int = 0
    unmatched: list[tuple[int, str]] = field(default_factory=list)  # (apps_match_id, "home vs away @date")
    games_updated: int = 0
    colors_set: int = 0  # individual side colours written
    images_missing: int = 0
    color_extract_failed: int = 0

    def as_dict(self) -> dict:
        return {
            "matches_seen": self.matches_seen,
            "matched_games": self.matched_games,
            "unmatched": len(self.unmatched),
            "games_updated": self.games_updated,
            "colors_set": self.colors_set,
            "images_missing": self.images_missing,
            "color_extract_failed": self.color_extract_failed,
        }


async def sync_kits_for_matches(
    db: AsyncSession,
    records: Iterable[AppsMatchKit],
    *,
    fetch_png: Callable[[str], bytes | None],
    allow_overwrite: bool = True,
    dry_run: bool = False,
    on_match: Callable[[AppsMatchKit, Game | None, str | None, str | None], None] | None = None,
) -> SyncResult:
    """Resolve each apps fixture to a QFL game and update its kit colours.

    ``fetch_png(filename)`` returns the PNG bytes (or ``None``).  HEX results are
    cached per image filename so a colour is computed once even when many
    matches reuse the same kit.  When ``dry_run`` is True nothing is committed
    and the DB session is rolled back at the end.  ``on_match`` (optional) is
    called for every record with ``(record, game_or_None, home_hex, away_hex)``
    — used by the import script to print a report.
    """
    result = SyncResult()
    hex_cache: dict[str, str | None] = {}

    def _hex_for(image: str | None) -> str | None:
        if not image:
            return None
        if image in hex_cache:
            return hex_cache[image]
        data = fetch_png(image)
        if data is None:
            result.images_missing += 1
            hex_cache[image] = None
            return None
        hx = extract_hex_from_png(data)
        if hx is None:
            result.color_extract_failed += 1
        hex_cache[image] = hx
        return hx

    for rec in records:
        result.matches_seen += 1
        home_hex = _hex_for(rec.home_image)
        away_hex = _hex_for(rec.away_image)

        game = await match_game(
            db,
            match_date=rec.match_date,
            home_name=rec.home_name,
            away_name=rec.away_name,
            match_time=rec.match_time,
        )
        if on_match is not None:
            on_match(rec, game, home_hex, away_hex)

        if game is None:
            result.unmatched.append(
                (rec.apps_match_id, f"{rec.home_name} vs {rec.away_name} @ {rec.match_date}")
            )
            continue
        result.matched_games += 1

        changed = False
        if apply_kit_color(game, "home", home_hex, allow_overwrite=allow_overwrite):
            changed = True
            result.colors_set += 1
        if apply_kit_color(game, "away", away_hex, allow_overwrite=allow_overwrite):
            changed = True
            result.colors_set += 1
        if changed:
            result.games_updated += 1

    if dry_run:
        await db.rollback()
    else:
        await db.commit()
    return result
