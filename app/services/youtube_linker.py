"""Auto-link YouTube videos to games via YouTube Data API v3."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.models.game import Game, GameStatus
from app.models.game_broadcaster import GameBroadcaster
from app.models.broadcaster import Broadcaster
from app.services.season_visibility import get_current_season_id
from app.models import Season
from app.utils.live_flag import get_redis
from app.utils.team_name_matcher import normalize_team_name, _collect_team_names

logger = logging.getLogger(__name__)

_YT_API = "https://www.googleapis.com/youtube/v3"
_REVIEW_KEYWORDS = {"обзор", "шолу", "highlights", "review"}
# Match both "Тур 5" and "III тур" / "II - тур"
_TOUR_AFTER_RE = re.compile(r"(?:тур|tour|тұр)\s*(\d+)", re.IGNORECASE)
_TOUR_BEFORE_RE = re.compile(r"([IVXLC]+)\s*[-–—]?\s*(?:тур|tour|тұр)", re.IGNORECASE)
_TEAM_SPLIT_RE = re.compile(r"\s+[-–—vs]+\s+", re.IGNORECASE)
# "ҚПЛ - 2026" or "ПФЛ - 2026" — league + year, not teams
_LEAGUE_YEAR_RE = re.compile(r"^[\w]{2,5}\s*[-–—]\s*\d{4}$", re.UNICODE)
# Cup/league/round patterns to strip from candidate team segments
_CUP_LEAGUE_SUFFIX_RE = re.compile(
    r"\s+(?:(?:OLIMPBET\s+)?(?:ҚАЗАҚСТАН|КАЗАХСТАН)\s+)?(?:КУБОГЫ|КУБОК|ЛИГА|ПРЕМЬЕР|БІРІНШІ).*$",
    re.IGNORECASE,
)
_ROUND_RE = re.compile(r"^\d+/\d+\s+\w+$", re.UNICODE)  # "1/16 ФИНАЛ"

_ROMAN_MAP = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100}


def _roman_to_int(s: str) -> int | None:
    """Convert Roman numeral string to int. Returns None if invalid."""
    s = s.upper()
    if not s or not all(c in _ROMAN_MAP for c in s):
        return None
    result = 0
    for i, c in enumerate(s):
        val = _ROMAN_MAP[c]
        if i + 1 < len(s) and _ROMAN_MAP[s[i + 1]] > val:
            result -= val
        else:
            result += val
    return result

# Bounded cache for uploads playlist ID per channel. Max 32 entries.
_uploads_playlist_ids: dict[str, str] = {}
_PLAYLIST_CACHE_MAX = 32


@dataclass(slots=True)
class ParsedTitle:
    team_a: str
    team_b: str
    tour: int | None


@dataclass(slots=True)
class _PendingEntry:
    game: object  # Game ORM instance or mock
    home_names: set[str]
    away_names: set[str]
    needs_live: bool
    needs_review: bool


@dataclass(slots=True)
class PendingGameIndex:
    """Pre-built index of games needing video links.

    Built once per cycle from pending games. Replaces per-video DB queries
    with O(N) in-memory matching over a small set of pending games.
    """
    _entries: list[_PendingEntry]

    @classmethod
    def build(cls, games: list) -> "PendingGameIndex":
        entries: list[_PendingEntry] = []
        for game in games:
            home_names = _collect_team_names(game.home_team)
            away_names = _collect_team_names(game.away_team)

            # Expand with fc/фк stripped variants
            for names_set in (home_names, away_names):
                extra = {
                    normalize_team_name(n.replace("фк ", "").replace("fc ", ""))
                    for n in names_set if " " in n
                }
                names_set |= extra
                # Compact forms (remove spaces): "кызыл жар" → "кызылжар"
                names_set |= {n.replace(" ", "") for n in names_set}
                # Abbreviation aliases: "каират жастар" → "каират ж"
                abbrevs: set[str] = set()
                for n in list(names_set):
                    tokens = n.split()
                    if len(tokens) >= 2 and len(tokens[-1]) > 1:
                        abbrevs.add(" ".join(tokens[:-1]) + " " + tokens[-1][0])
                names_set |= abbrevs

            entries.append(_PendingEntry(
                game=game,
                home_names=home_names,
                away_names=away_names,
                needs_live=game.youtube_live_url is None,
                needs_review=game.video_review_url is None,
            ))
        return cls(_entries=entries)

    def find_match(
        self, parsed: ParsedTitle, match_date: date, video_type: str,
    ) -> object | None:
        """Find a matching game for the parsed video title."""
        tolerance = 2 if video_type == "review" else 1
        date_from = match_date - timedelta(days=tolerance)
        date_to = match_date + timedelta(days=tolerance)

        norm_a = normalize_team_name(parsed.team_a)
        norm_b = normalize_team_name(parsed.team_b)
        compact_a = norm_a.replace(" ", "")
        compact_b = norm_b.replace(" ", "")

        candidates: list[_PendingEntry] = []
        for entry in self._entries:
            # Date check
            if not (date_from <= entry.game.date <= date_to):
                continue

            # Needs-type check
            if video_type in ("live", "replay") and not entry.needs_live:
                continue
            if video_type == "review" and not entry.needs_review:
                continue

            # Team name matching
            def _matches(a: str, ca: str, names: set[str]) -> bool:
                if a in names or ca in names:
                    return True
                a_words = frozenset(a.split())
                if len(a_words) >= 2:
                    return any(frozenset(n.split()) == a_words for n in names)
                return False

            match_fwd = (
                _matches(norm_a, compact_a, entry.home_names)
                and _matches(norm_b, compact_b, entry.away_names)
            )
            match_rev = (
                _matches(norm_a, compact_a, entry.away_names)
                and _matches(norm_b, compact_b, entry.home_names)
            )
            if match_fwd or match_rev:
                candidates.append(entry)

        if not candidates:
            return None

        # Tour disambiguation
        if parsed.tour is not None and len(candidates) > 1:
            tour_matched = [e for e in candidates if e.game.tour == parsed.tour]
            if len(tour_matched) == 1:
                return tour_matched[0].game

        if len(candidates) == 1:
            return candidates[0].game

        if len(candidates) > 1:
            logger.warning(
                "Ambiguous match for '%s vs %s' (date=%s): %d candidates [%s]",
                parsed.team_a, parsed.team_b, match_date,
                len(candidates),
                ", ".join(str(e.game.id) for e in candidates),
            )
        return None


async def _get_uploads_playlist_id(channel_id: str, api_key: str) -> str:
    """Get the uploads playlist ID for a channel (cached per channel)."""
    if channel_id in _uploads_playlist_ids:
        return _uploads_playlist_ids[channel_id]

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{_YT_API}/channels",
            params={"id": channel_id, "part": "contentDetails", "key": api_key},
        )
        resp.raise_for_status()
        data = resp.json()

    items = data.get("items", [])
    if not items:
        raise ValueError(f"YouTube channel {channel_id} not found")

    playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    if len(_uploads_playlist_ids) >= _PLAYLIST_CACHE_MAX:
        _uploads_playlist_ids.clear()
    _uploads_playlist_ids[channel_id] = playlist_id
    return playlist_id


async def _fetch_recent_videos(
    playlist_id: str, api_key: str, max_results: int = 50
) -> list[dict]:
    """Fetch recent video IDs and titles from uploads playlist."""
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{_YT_API}/playlistItems",
            params={
                "playlistId": playlist_id,
                "part": "snippet",
                "maxResults": max_results,
                "key": api_key,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    return [
        {
            "video_id": item["snippet"]["resourceId"]["videoId"],
            "title": item["snippet"]["title"],
            "published_at": item["snippet"]["publishedAt"],
        }
        for item in data.get("items", [])
    ]


async def _enrich_videos(video_ids: list[str], api_key: str) -> dict[str, dict]:
    """Enrich videos with snippet + liveStreamingDetails (batch up to 50)."""
    if not video_ids:
        return {}

    result = {}
    async with httpx.AsyncClient(timeout=10) as client:
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            resp = await client.get(
                f"{_YT_API}/videos",
                params={
                    "id": ",".join(batch),
                    "part": "snippet,liveStreamingDetails",
                    "key": api_key,
                },
            )
            resp.raise_for_status()
            for item in resp.json().get("items", []):
                result[item["id"]] = {
                    "snippet": item["snippet"],
                    "live_streaming_details": item.get("liveStreamingDetails"),
                }

    return result


def classify_video(
    snippet: dict, live_streaming_details: dict | None
) -> str | None:
    """Classify video as live/replay/review/None (skip)."""
    broadcast_content = snippet.get("liveBroadcastContent", "none")

    if broadcast_content in ("upcoming", "live"):
        return "live"

    if broadcast_content == "none" and live_streaming_details:
        return "replay"

    if broadcast_content == "none" and not live_streaming_details:
        title_lower = snippet.get("title", "").lower()
        if any(kw in title_lower for kw in _REVIEW_KEYWORDS):
            return "review"

    return None


def _parse_tour(seg: str) -> int | None:
    """Extract tour number from a segment (Arabic or Roman numerals)."""
    # "Тур 5" / "tour 12"
    m = _TOUR_AFTER_RE.search(seg)
    if m:
        return int(m.group(1))
    # "III тур" / "II - тур"
    m = _TOUR_BEFORE_RE.search(seg)
    if m:
        return _roman_to_int(m.group(1))
    return None


def parse_video_title(title: str) -> ParsedTitle | None:
    """Parse video title to extract team names and tour number.

    Expected formats:
      "АТЫРАУ VS ЖЕҢІС | ҚПЛ - 2026 | III тур"
      "ОБЗОР | Team A vs Team B"
      "Шолу І Каспий - Оқжетпес І ҚПЛ II - тур"
      "Тур 12 | Team A — Team B"
    """
    # Split by | and also Kazakh І (U+0406) when surrounded by spaces (used as pipe)
    segments = [s.strip() for s in re.split(r"\||\s\u0406\s", title) if s.strip()]

    tour: int | None = None
    teams_segment: str | None = None
    candidate_teams: list[str] = []

    for seg in segments:
        # Check for tour number
        seg_tour = _parse_tour(seg)
        if seg_tour is not None:
            tour = seg_tour
            # If segment has more than just tour info, check for teams too
            remainder = _TOUR_AFTER_RE.sub("", _TOUR_BEFORE_RE.sub("", seg)).strip()
            if teams_segment is None and _TEAM_SPLIT_RE.search(remainder):
                teams_segment = remainder
            continue

        # Skip league+year patterns like "ҚПЛ - 2026"
        if _LEAGUE_YEAR_RE.match(seg):
            continue

        # Skip known non-team keywords
        seg_lower = seg.lower()
        if seg_lower in _REVIEW_KEYWORDS or seg_lower in {"пфл", "қпл", "kpl"}:
            continue

        # Check for team names (contains separator like " - ", " vs ", " — ")
        # Only set teams_segment if not already found (first match wins)
        if teams_segment is None and _TEAM_SPLIT_RE.search(seg):
            teams_segment = seg
            continue

        # Collect unclassified segments as candidate team names
        # (for pipe-separated formats like "Шолу | Team A | Team B LEAGUE")
        if not _ROUND_RE.match(seg):
            candidate = _CUP_LEAGUE_SUFFIX_RE.sub("", seg).strip()
            if candidate:
                candidate_teams.append(candidate)

    # Primary: split by team separator within one segment
    if teams_segment:
        parts = _TEAM_SPLIT_RE.split(teams_segment, maxsplit=1)
        if len(parts) == 2:
            team_a = parts[0].strip()
            team_b = parts[1].strip()
            if team_a and team_b:
                return ParsedTitle(team_a=team_a, team_b=team_b, tour=tour)

    # Fallback: pipe-separated team names in consecutive segments
    if len(candidate_teams) >= 2:
        return ParsedTitle(team_a=candidate_teams[0], team_b=candidate_teams[1], tour=tour)

    return None


def _get_match_date(
    video_type: str, snippet: dict, live_streaming_details: dict | None
) -> date | None:
    """Pick the correct date field based on video type."""
    try:
        if video_type in ("live", "replay") and live_streaming_details:
            # Prefer actualStartTime for replays, scheduledStartTime otherwise
            ts = live_streaming_details.get("actualStartTime") or live_streaming_details.get("scheduledStartTime")
            if ts:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).date()

        # Fallback to publishedAt
        published = snippet.get("publishedAt")
        if published:
            return datetime.fromisoformat(published.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        pass
    return None


def _url_fields_for_type(video_type: str) -> list[str]:
    """Map video type to Game URL field names.

    Only auto-links youtube_live_url and video_review_url.
    """
    if video_type in ("live", "replay"):
        return ["youtube_live_url"]
    if video_type == "review":
        return ["video_review_url"]
    return []


async def _ensure_broadcaster_linked(
    db: AsyncSession, game_id: int, broadcaster_id: int
) -> None:
    """Add GameBroadcaster row if not already present."""
    existing = await db.execute(
        select(GameBroadcaster).where(
            GameBroadcaster.game_id == game_id,
            GameBroadcaster.broadcaster_id == broadcaster_id,
        )
    )
    if not existing.scalar_one_or_none():
        db.add(GameBroadcaster(game_id=game_id, broadcaster_id=broadcaster_id))


async def _find_games_needing_videos(
    db: AsyncSession, season_ids: list[int],
) -> list[Game]:
    """Find games that still need video URLs within the active window.

    Window: today-3..today+7 (covers 48h post-match for reviews,
    12h pre-match for live streams, with date-level granularity).
    """
    if not season_ids:
        return []
    today = date.today()
    result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.season_id.in_(season_ids),
            Game.status.not_in([
                GameStatus.postponed,
                GameStatus.cancelled,
                GameStatus.technical_defeat,
            ]),
            Game.date >= today - timedelta(days=3),
            Game.date <= today + timedelta(days=7),
            or_(
                Game.youtube_live_url.is_(None),
                Game.video_review_url.is_(None),
            ),
        )
    )
    return list(result.scalars().all())


async def _get_kff_broadcaster_id(db: AsyncSession) -> int | None:
    """Find the KFF League YouTube broadcaster."""
    result = await db.execute(
        select(Broadcaster.id).where(
            Broadcaster.type == "youtube",
            Broadcaster.is_active.is_(True),
        ).limit(1)
    )
    return result.scalar_one_or_none()


async def _fetch_videos_from_channel(
    channel_id: str, api_key: str, redis: object,
) -> tuple[list[dict], dict]:
    """Fetch new (unprocessed) videos from a single YouTube channel.

    Returns (new_videos, skip_stats) where skip_stats has 'skipped' count.
    """
    skipped = 0
    playlist_id = await _get_uploads_playlist_id(channel_id, api_key)
    recent = await _fetch_recent_videos(playlist_id, api_key)
    if not recent:
        return [], {"skipped": 0}

    new_videos = []
    for v in recent:
        vid = v["video_id"]
        if await redis.exists(f"yt:linked:{vid}"):
            skipped += 1
            continue
        if await redis.exists(f"yt:retry:{vid}"):
            skipped += 1
            continue
        new_videos.append(v)

    return new_videos, {"skipped": skipped}


async def link_youtube_videos(db: AsyncSession) -> dict:
    """Main orchestrator: game-first approach.

    1. Query DB for games needing videos (early exit if none)
    2. Build PendingGameIndex from those games
    3. Fetch YouTube videos and match against the index
    """
    settings = get_settings()
    api_key = settings.youtube_api_key
    channel_id = settings.youtube_channel_id

    if not api_key or not channel_id:
        logger.warning("YouTube API key or channel ID not configured, skipping")
        return {"skipped": "not configured"}

    # Build ordered channel list: primary first, then reserves
    reserve_ids = [
        cid.strip() for cid in settings.youtube_reserve_channel_ids.split(",")
        if cid.strip() and cid.strip() != channel_id
    ]
    channel_ids = [channel_id] + reserve_ids

    redis = await get_redis()
    stats = {
        "processed": 0, "linked": 0, "skipped": 0, "retry": 0,
        "errors": 0, "channels_checked": 0, "pending_games": 0,
    }

    try:
        # Get all current season IDs (multiple leagues can be active)
        season_result = await db.execute(
            select(Season.id).where(Season.is_current.is_(True))
        )
        season_ids = [row[0] for row in season_result.all()]
        if not season_ids:
            season_ids = [await get_current_season_id(db)]

        # ── GAME-FIRST: early exit if no games need videos ──
        pending_games = await _find_games_needing_videos(db, season_ids)
        stats["pending_games"] = len(pending_games)
        if not pending_games:
            logger.info("No games needing videos, skipping YouTube fetch")
            return stats

        index = PendingGameIndex.build(pending_games)

        # Get KFF broadcaster ID for linking
        broadcaster_id = await _get_kff_broadcaster_id(db)

        # Process each channel
        for cid in channel_ids:
            stats["channels_checked"] += 1
            try:
                new_videos, skip_stats = await _fetch_videos_from_channel(cid, api_key, redis)
                stats["skipped"] += skip_stats["skipped"]
            except Exception:
                logger.exception("Error fetching videos from channel %s", cid)
                stats["errors"] += 1
                continue

            if not new_videos:
                continue

            # Enrich with liveStreamingDetails
            video_ids = [v["video_id"] for v in new_videos]
            enriched = await _enrich_videos(video_ids, api_key)

            # Process each video
            for v in new_videos:
                vid = v["video_id"]
                stats["processed"] += 1

                info = enriched.get(vid)
                if not info:
                    continue

                snippet = info["snippet"]
                lsd = info["live_streaming_details"]

                # Classify
                video_type = classify_video(snippet, lsd)
                if video_type is None:
                    await redis.set(f"yt:linked:{vid}", "skip", ex=7 * 86400)
                    stats["skipped"] += 1
                    continue

                # Parse title
                title = snippet.get("title", "")
                parsed = parse_video_title(title)
                if parsed is None:
                    logger.debug("Could not parse title: %s", title)
                    await redis.set(f"yt:retry:{vid}", "1", ex=7200)
                    stats["retry"] += 1
                    continue

                # Get match date
                match_date = _get_match_date(video_type, snippet, lsd)
                if match_date is None:
                    logger.debug("Could not determine date for: %s", title)
                    await redis.set(f"yt:retry:{vid}", "1", ex=7200)
                    stats["retry"] += 1
                    continue

                # Find matching game via pre-built index
                game = index.find_match(parsed, match_date, video_type)
                if game is None:
                    logger.info("No match found for: %s (date=%s)", title, match_date)
                    await redis.set(f"yt:retry:{vid}", "1", ex=7200)
                    stats["retry"] += 1
                    continue

                # Set URL fields only if NULL
                url_fields = _url_fields_for_type(video_type)
                youtube_url = f"https://www.youtube.com/watch?v={vid}"

                for url_field in url_fields:
                    current_value = getattr(game, url_field)
                    if current_value is None:
                        setattr(game, url_field, youtube_url)
                        logger.info(
                            "Linked video %s (channel=%s) → game %d (%s=%s)",
                            vid, cid, game.id, url_field, youtube_url,
                        )

                # Link broadcaster
                if broadcaster_id is not None:
                    await _ensure_broadcaster_linked(db, game.id, broadcaster_id)

                # Mark as processed
                await redis.set(f"yt:linked:{vid}", "1", ex=7 * 86400)
                stats["linked"] += 1

    except Exception:
        logger.exception("Error in link_youtube_videos")
        stats["errors"] += 1

    return stats
