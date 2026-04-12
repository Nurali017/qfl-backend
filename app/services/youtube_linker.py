"""Auto-link YouTube videos to games via YouTube Data API v3."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.models.game import Game
from app.models.game_broadcaster import GameBroadcaster
from app.models.broadcaster import Broadcaster
from app.models.media_video import MediaVideo
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

    if not teams_segment:
        return None

    # Split teams
    parts = _TEAM_SPLIT_RE.split(teams_segment, maxsplit=1)
    if len(parts) != 2:
        return None

    team_a = parts[0].strip()
    team_b = parts[1].strip()

    if not team_a or not team_b:
        return None

    return ParsedTitle(team_a=team_a, team_b=team_b, tour=tour)


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


async def _find_matching_game(
    parsed: ParsedTitle,
    match_date: date,
    video_type: str,
    db: AsyncSession,
    season_ids: list[int],
) -> Game | None:
    """Find a game matching the parsed title and date.

    Searches across all provided season IDs.
    Returns None if no match or ambiguous (multiple matches).
    """
    # Date tolerance: ±1 day for live/replay, ±2 days for review
    tolerance = 2 if video_type == "review" else 1
    date_from = match_date - timedelta(days=tolerance)
    date_to = match_date + timedelta(days=tolerance)

    result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            Game.season_id.in_(season_ids),
            Game.date >= date_from,
            Game.date <= date_to,
        )
    )
    games = result.scalars().all()

    if not games:
        return None

    norm_a = normalize_team_name(parsed.team_a)
    norm_b = normalize_team_name(parsed.team_b)
    # Compact versions: "кызыл жар" → "кызылжар" for hyphenated names
    compact_a = norm_a.replace(" ", "")
    compact_b = norm_b.replace(" ", "")

    candidates: list[Game] = []
    for game in games:
        home_names = _collect_team_names(game.home_team)
        away_names = _collect_team_names(game.away_team)
        all_home = home_names | {normalize_team_name(n.replace("фк ", "").replace("fc ", "")) for n in home_names if " " in n}
        all_away = away_names | {normalize_team_name(n.replace("фк ", "").replace("fc ", "")) for n in away_names if " " in n}
        # Add compact versions for matching "кызыл жар" ↔ "кызылжар"
        all_home |= {n.replace(" ", "") for n in all_home}
        all_away |= {n.replace(" ", "") for n in all_away}

        # Match in either order (home-away or away-home)
        # Also try word-set matching for reversed word order
        # e.g. "онтустик академия" ↔ "академия онтустик"
        def _matches(a: str, ca: str, names: set[str]) -> bool:
            if a in names or ca in names:
                return True
            a_words = frozenset(a.split())
            if len(a_words) >= 2:
                return any(frozenset(n.split()) == a_words for n in names)
            return False

        match_forward = (_matches(norm_a, compact_a, all_home) and _matches(norm_b, compact_b, all_away))
        match_reverse = (_matches(norm_a, compact_a, all_away) and _matches(norm_b, compact_b, all_home))

        if match_forward or match_reverse:
            # If tour is specified, verify it matches
            if parsed.tour is not None and game.tour is not None:
                if game.tour != parsed.tour:
                    continue
            candidates.append(game)

    if len(candidates) == 1:
        return candidates[0]

    if len(candidates) > 1:
        logger.warning(
            "Ambiguous match for '%s vs %s' (date=%s): %d candidates [%s]",
            parsed.team_a, parsed.team_b, match_date,
            len(candidates),
            ", ".join(str(g.id) for g in candidates),
        )
    return None


def _url_fields_for_type(video_type: str) -> list[str]:
    """Map video type to Game URL field names.

    Replay (completed broadcast) sets both video_url and youtube_live_url
    because the recording IS the live stream.
    """
    if video_type == "live":
        return ["youtube_live_url"]
    if video_type == "replay":
        return ["video_url", "youtube_live_url"]
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


async def _ensure_media_video(db: AsyncSession, video_id: str, title: str) -> None:
    """Add MediaVideo row if youtube_id not already present."""
    existing = await db.execute(
        select(MediaVideo.id).where(MediaVideo.youtube_id == video_id)
    )
    if existing.scalar_one_or_none():
        return
    # Get lowest sort_order to place new video on top
    min_order = await db.execute(
        select(MediaVideo.sort_order).order_by(MediaVideo.sort_order).limit(1)
    )
    top_order = (min_order.scalar() or 0) - 1
    db.add(MediaVideo(title=title, youtube_id=video_id, sort_order=top_order))
    logger.info("Added MediaVideo: %s (%s)", title, video_id)


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
    """Main orchestrator: fetch recent videos, classify, match, and link.

    Checks the primary channel first, then any reserve channels.
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
    stats = {"processed": 0, "linked": 0, "skipped": 0, "retry": 0, "errors": 0, "channels_checked": 0}

    try:
        # Get all current season IDs (multiple leagues can be active)
        season_result = await db.execute(
            select(Season.id).where(Season.is_current.is_(True))
        )
        season_ids = [row[0] for row in season_result.all()]
        if not season_ids:
            season_ids = [await get_current_season_id(db)]

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

                # Find matching game
                game = await _find_matching_game(parsed, match_date, video_type, db, season_ids)
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
