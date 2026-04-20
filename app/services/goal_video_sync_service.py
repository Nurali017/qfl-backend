"""Sync goal video clips from Google Drive into MinIO.

Drive layout observed in practice:
    root/
      6- Тур/
        ЕЛИМАЙ АСТАНА/
          <any filename>.mp4
          <another>.mp4
        …

Matching strategy (timing-first — filenames are unreliable):
  1. Resolve each match-folder label to a `Game` by home+away team names.
     If string aliases don't match → AI fallback.
  2. For each goal-event (video_url IS NULL) in that game compute
     ``expected_upload_time = wall_time_of(event) + 7 min``.
  3. For each unprocessed video in the folder, pick the event whose
     ``|video.modifiedTime − expected_upload_time| ≤ 15 min`` and is closest.
  4. Extra / duplicate videos (повторы, разные камеры, голы не в БД) → unmatched.
  5. When timing is ambiguous, hand the whole folder to the AI matcher for
     filename+metadata reasoning.
"""

from __future__ import annotations

import hashlib
import logging
import mimetypes
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.models import Game, GameStatus, Team
from app.models.game_event import GameEvent, GameEventType
from app.services.file_storage import FileStorageService
from app.services.goal_video_ai_matcher import (
    CandidateGame,
    CandidateGoal,
    get_ai_matcher,
)
from app.services.google_drive_client import DriveFile, get_drive_client
from app.utils.goal_video_filename import parse_goal_filename
from app.utils.video_transcode import transcode_mp4

logger = logging.getLogger(__name__)

GOAL_EVENT_TYPES = (
    GameEventType.goal,
    GameEventType.own_goal,
    GameEventType.penalty,
)

_PROCESSED_SET_KEY = "qfl:goal-videos:processed"
_LAST_SYNC_KEY = "qfl:goal-videos:last-sync"
_PROCESSED_TTL_SECONDS = 48 * 3600
_FINISHED_WINDOW_MINUTES = 24 * 60
_UPLOAD_DELAY_MINUTES = 7
_MATCH_WINDOW_MINUTES = 15
_HALFTIME_BREAK_MINUTES = 15
_DEFAULT_LOOKBACK_MINUTES = 24 * 60


@dataclass
class SyncResult:
    listed: int = 0
    matched: int = 0
    ai_folder_matched: int = 0
    ai_event_matched: int = 0
    skipped_already_processed: int = 0
    skipped_no_game: int = 0
    unmatched: int = 0
    errors: int = 0


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

async def _load_last_sync() -> datetime:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        raw = await r.get(_LAST_SYNC_KEY)
        if raw:
            return datetime.fromisoformat(raw.decode())
    except Exception:
        logger.debug("Cannot read last-sync cursor")
    return datetime.now(timezone.utc) - timedelta(minutes=_DEFAULT_LOOKBACK_MINUTES)


async def _store_last_sync(ts: datetime) -> None:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        await r.set(_LAST_SYNC_KEY, ts.isoformat(), ex=7 * 24 * 3600)
    except Exception:
        pass


async def _mark_processed(file_id: str) -> None:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        await r.sadd(_PROCESSED_SET_KEY, file_id)
        await r.expire(_PROCESSED_SET_KEY, _PROCESSED_TTL_SECONDS)
    except Exception:
        pass


async def _is_processed(file_id: str) -> bool:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        return bool(await r.sismember(_PROCESSED_SET_KEY, file_id))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Active games + team-name matching
# ---------------------------------------------------------------------------

@dataclass
class ActiveGame:
    game: Game
    events: list[GameEvent]
    home_name: str
    away_name: str


async def _load_active_games(db: AsyncSession) -> list[ActiveGame]:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=_FINISHED_WINDOW_MINUTES)
    result = await db.execute(
        select(Game)
        .options(selectinload(Game.home_team), selectinload(Game.away_team))
        .where(
            or_(
                Game.status == GameStatus.live,
                Game.finished_at.isnot(None) & (Game.finished_at >= cutoff),
            )
        )
    )
    games = list(result.scalars().all())
    if not games:
        return []

    events_result = await db.execute(
        select(GameEvent).where(
            GameEvent.game_id.in_([g.id for g in games]),
            GameEvent.event_type.in_(GOAL_EVENT_TYPES),
        )
    )
    events_by_game: dict[int, list[GameEvent]] = {}
    for ev in events_result.scalars().all():
        events_by_game.setdefault(ev.game_id, []).append(ev)

    actives: list[ActiveGame] = []
    for g in games:
        home = g.home_team.name if g.home_team else ""
        away = g.away_team.name if g.away_team else ""
        if not home or not away:
            continue
        actives.append(ActiveGame(game=g, events=events_by_game.get(g.id, []),
                                  home_name=home, away_name=away))
    return actives


def _normalize_label(value: str) -> str:
    return "".join(ch for ch in value.casefold() if ch.isalnum())


_TEAM_ALIASES: dict[str, list[str]] = {
    "иртыш": ["ертис", "ertis", "irtysh"],
    "каспий": ["kaspiy"],
    "кайрат": ["qairat", "kairat"],
    "астана": ["astana"],
    "елимай": ["elimay"],
    "шахтёр": ["шахтер", "shakhter"],
    "шахтер": ["шахтёр", "shakhter"],
    "жетысу": ["zhetysu", "jetysu"],
    "ордабасы": ["ordabasy"],
    "окжетпес": ["okzhetpes"],
    "улытау": ["ulytau"],
    "тобыл": ["tobyl", "tobol"],
    "атырау": ["atyrau"],
    "кызылжар": ["kyzylzhar"],
    "женис": ["zhenis"],
    "актобе": ["aktobe", "aqtobe"],
    "кайсар": ["kaisar"],
    "алтай": ["altay"],
    "тараз": ["taraz"],
    "хан-тенгри": ["ханТенгри", "hanTengri", "хантенгри"],
}


def _team_tokens(name: str) -> list[str]:
    base = _normalize_label(name)
    tokens = [base]
    lower = name.casefold().strip()
    for alias in _TEAM_ALIASES.get(lower, []):
        tokens.append(_normalize_label(alias))
    return [t for t in tokens if t]


def folder_matches_game(folder_label: str, home_name: str, away_name: str) -> bool:
    normalized = _normalize_label(folder_label)
    for h in _team_tokens(home_name):
        if h not in normalized:
            continue
        for a in _team_tokens(away_name):
            if a in normalized:
                return True
    return False


def _string_match_folder_to_game(
    folder_label: str, actives: list[ActiveGame]
) -> ActiveGame | None:
    for active in actives:
        if folder_matches_game(folder_label, active.home_name, active.away_name):
            return active
    return None


# ---------------------------------------------------------------------------
# Timing utilities
# ---------------------------------------------------------------------------

def _wall_time_of_event(event: GameEvent, game: Game) -> datetime | None:
    """Approximate real wall-clock time when the goal happened."""
    if game.half1_started_at is None:
        return None
    if event.minute <= 45:
        return game.half1_started_at + timedelta(minutes=event.minute)
    if game.half2_started_at is not None:
        return game.half2_started_at + timedelta(minutes=event.minute - 45)
    # Fallback: assume a standard halftime break.
    return game.half1_started_at + timedelta(
        minutes=45 + _HALFTIME_BREAK_MINUTES + (event.minute - 45)
    )


def _expected_upload_time(event: GameEvent, game: Game) -> datetime | None:
    wall = _wall_time_of_event(event, game)
    if wall is None:
        return None
    return wall + timedelta(minutes=_UPLOAD_DELAY_MINUTES)


def _is_shootout_event(event: GameEvent) -> bool:
    # Shootout events are stored at half >= 5 in our schema; exclude them —
    # they don't belong to a real match-minute timeline.
    return event.half >= 5


# ---------------------------------------------------------------------------
# Matching within one folder (one game)
# ---------------------------------------------------------------------------

def _normalize_player(value: str | None) -> str:
    if not value:
        return ""
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _player_bonus(drive_file: DriveFile, event: GameEvent) -> timedelta:
    """If the filename contains the scorer's surname, shrink the apparent
    time-distance by a large constant — that tips the assignment toward the
    name-matched pair when timings are close.
    """
    parsed = parse_goal_filename(drive_file.name)
    if not parsed or not parsed.player_hint or not event.player_name:
        return timedelta(0)
    if _normalize_player(parsed.player_hint) in _normalize_player(event.player_name):
        return timedelta(minutes=10)
    return timedelta(0)


def _optimal_time_match(
    game: Game,
    events: list[GameEvent],
    videos: list[DriveFile],
) -> list[tuple[DriveFile, GameEvent]]:
    """Stable-marriage-ish assignment: pick pairs in ascending effective-cost
    order, taking the best-scoring free pair each time.

    ``effective_cost = |file.modifiedTime − expected_upload| − player_bonus``

    One-to-one; each video and each event used at most once.
    """
    unmatched_events = [e for e in events if e.video_url is None and not _is_shootout_event(e)]
    if not unmatched_events or not videos:
        return []

    window = timedelta(minutes=_MATCH_WINDOW_MINUTES)
    candidate_pairs: list[tuple[timedelta, DriveFile, GameEvent]] = []

    for event in unmatched_events:
        expected = _expected_upload_time(event, game)
        if expected is None:
            continue
        for v in videos:
            if v.modified_time is None:
                continue
            diff = abs(v.modified_time - expected)
            if diff > window:
                continue
            cost = diff - _player_bonus(v, event)
            candidate_pairs.append((cost, v, event))

    # Pick in ascending cost.
    candidate_pairs.sort(key=lambda p: p[0])
    used_videos: set[str] = set()
    used_events: set[int] = set()
    pairs: list[tuple[DriveFile, GameEvent]] = []
    for _cost, v, event in candidate_pairs:
        if v.id in used_videos or event.id in used_events:
            continue
        pairs.append((v, event))
        used_videos.add(v.id)
        used_events.add(event.id)
    return pairs


# ---------------------------------------------------------------------------
# MinIO upload helpers
# ---------------------------------------------------------------------------

def _content_hash(payload: bytes) -> str:
    """Return a short stable hash for versioned goal-video object names."""
    return hashlib.blake2b(payload, digest_size=8).hexdigest()


def _object_name_for(event: GameEvent, drive_file: DriveFile, payload: bytes) -> str:
    ext = ""
    if "." in drive_file.name:
        ext = drive_file.name.rsplit(".", 1)[-1].lower()
    if not ext:
        guessed = mimetypes.guess_extension(drive_file.mime_type or "") or ".mp4"
        ext = guessed.lstrip(".")
    version = _content_hash(payload)
    return f"goal_videos/{event.game_id}/{event.id}-{version}.{ext}"


def _temp_suffix_for(drive_file: DriveFile) -> str:
    if "." in drive_file.name:
        return "." + drive_file.name.rsplit(".", 1)[-1].lower()
    guessed = mimetypes.guess_extension(drive_file.mime_type or "") or ".mp4"
    return guessed if guessed.startswith(".") else f".{guessed}"


def _enqueue_goal_video_followup(event_id: int) -> None:
    try:
        from app.tasks.telegram_tasks import post_goal_video_task

        post_goal_video_task.delay(event_id)
    except Exception:
        logger.exception("failed to enqueue post_goal_video_task for %s", event_id)


async def _post_goal_video_from_payload(
    db: AsyncSession,
    drive_file: DriveFile,
    event: GameEvent,
    payload: bytes,
) -> bool:
    from app.services.telegram_posts import post_goal_video_from_file

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=_temp_suffix_for(drive_file),
            delete=False,
        ) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        return await post_goal_video_from_file(db, event.id, tmp_path)
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                logger.debug("Cleanup of %s failed", tmp_path)


# ---------------------------------------------------------------------------
# AI fallback (per-event) — when timing left something unmatched
# ---------------------------------------------------------------------------

async def _ai_candidates_for_game(
    active: ActiveGame, db: AsyncSession
) -> list[CandidateGoal]:
    team_ids = {e.team_id for e in active.events if e.team_id}
    team_names: dict[int, str] = {}
    if team_ids:
        teams = await db.execute(select(Team).where(Team.id.in_(team_ids)))
        team_names = {t.id: t.name for t in teams.scalars().all()}

    label = f"{active.home_name} vs {active.away_name}"
    candidates: list[CandidateGoal] = []
    for ev in active.events:
        if ev.video_url is not None or _is_shootout_event(ev):
            continue
        candidates.append(
            CandidateGoal(
                event_id=ev.id,
                game_id=ev.game_id,
                game_label=label,
                minute=ev.minute,
                half=ev.half,
                player_name=ev.player_name,
                team_name=team_names.get(ev.team_id) if ev.team_id else None,
                event_type=ev.event_type.value,
            )
        )
    return candidates


# ---------------------------------------------------------------------------
# Per-file processing (download + upload + link)
# ---------------------------------------------------------------------------

async def _download_and_link(
    drive, db: AsyncSession, drive_file: DriveFile, event: GameEvent
) -> bool:
    try:
        payload = await drive.download_file(drive_file.id)
    except Exception:
        logger.exception("Failed to download Drive file %s", drive_file.id)
        return False

    settings = get_settings()
    if settings.goal_video_transcode_enabled and (drive_file.mime_type or "").startswith("video/"):
        try:
            result = await transcode_mp4(
                payload,
                crf=settings.goal_video_transcode_crf,
                preset=settings.goal_video_transcode_preset,
            )
            payload = result.data
        except Exception:
            logger.exception("Transcode step failed for %s — uploading original", drive_file.id)

    object_name = _object_name_for(event, drive_file, payload)
    content_type = drive_file.mime_type or "video/mp4"
    try:
        await FileStorageService.upload_file(
            file_data=payload,
            filename=drive_file.name,
            content_type=content_type,
            category="goal_videos",
            skip_optimization=True,
            object_name=object_name,
            metadata={"drive-file-id": drive_file.id, "game-id": str(event.game_id)},
        )
    except Exception:
        logger.exception("MinIO upload failed for event %s", event.id)
        return False

    event.video_url = object_name
    await db.commit()
    await _mark_processed(drive_file.id)
    logger.info(
        "Linked Drive file %s → event %s (game %s, %s, minute %s, %.1f MB)",
        drive_file.name, event.id, event.game_id, event.player_name, event.minute,
        len(payload) / 1024 / 1024,
    )

    # Attach the video on the media host while we still have the local payload.
    # If that fails, fall back to the Celery retry path, which will re-read
    # the clip from MinIO via event.video_url.
    if event.telegram_message_id and event.telegram_video_sent_at is None:
        try:
            inline_ok = await _post_goal_video_from_payload(db, drive_file, event, payload)
            if not inline_ok:
                _enqueue_goal_video_followup(event.id)
        except Exception:
            logger.exception(
                "inline goal video post failed for %s; enqueueing fallback",
                event.id,
            )
            _enqueue_goal_video_followup(event.id)
    return True


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

async def sync_goal_videos(db: AsyncSession) -> SyncResult:
    settings = get_settings()
    result = SyncResult()

    if not settings.google_drive_enabled:
        return result
    if not settings.google_drive_goals_folder_id:
        logger.warning("GOOGLE_DRIVE_GOALS_FOLDER_ID is not set; skipping sync")
        return result

    actives = await _load_active_games(db)
    if not actives:
        await _store_last_sync(datetime.now(timezone.utc) - timedelta(minutes=5))
        return result

    since = await _load_last_sync()
    drive = get_drive_client()
    try:
        # max_depth=3 to support root → tour → [optional date] → match → clips.
        videos = await drive.list_recent_videos_recursive(
            settings.google_drive_goals_folder_id, since=since, max_depth=3
        )
    except Exception:
        logger.exception("Failed to list Drive videos")
        result.errors += 1
        return result

    result.listed = len(videos)
    if not videos:
        await _store_last_sync(datetime.now(timezone.utc))
        return result

    # Group videos by their match-folder label (deepest ancestor).
    buckets: dict[str, list[DriveFile]] = defaultdict(list)
    for v in videos:
        if not v.ancestor_names:
            continue
        buckets[v.ancestor_names[-1]].append(v)

    ai = get_ai_matcher()
    all_candidates = [
        CandidateGame(game_id=a.game.id, home=a.home_name, away=a.away_name)
        for a in actives
    ]

    # Phase 1: resolve each folder → game (string first, AI fallback).
    folder_to_game: dict[str, ActiveGame | None] = {}
    for folder_name in buckets:
        game = _string_match_folder_to_game(folder_name, actives)
        if game is None and ai.enabled and all_candidates:
            try:
                ai_res = await ai.match_folder(folder_name=folder_name, candidates=all_candidates)
            except Exception:
                logger.exception("AI folder matcher raised")
                ai_res = None
            if ai_res and ai_res.game_id is not None and ai_res.confidence == "high":
                game = next((a for a in actives if a.game.id == ai_res.game_id), None)
                if game:
                    logger.info(
                        "AI resolved folder %r → game %s (%s)",
                        folder_name, game.game.id, ai_res.reason,
                    )
                    result.ai_folder_matched += 1
            elif ai_res:
                logger.info(
                    "AI declined folder %r (confidence=%s): %s",
                    folder_name, ai_res.confidence, ai_res.reason,
                )
        folder_to_game[folder_name] = game

    # Phase 2: timing-based matching inside each folder.
    for folder_name, folder_videos in buckets.items():
        active = folder_to_game.get(folder_name)
        if active is None:
            logger.info("No game for folder %r (%d videos)", folder_name, len(folder_videos))
            result.skipped_no_game += len(folder_videos)
            continue

        # Prune already-processed early (per file) to save API & bandwidth.
        fresh_videos: list[DriveFile] = []
        for v in folder_videos:
            if await _is_processed(v.id):
                result.skipped_already_processed += 1
                continue
            fresh_videos.append(v)

        if not fresh_videos:
            continue

        # Timing pairs (with optional player-surname tiebreaker).
        pairs = _optimal_time_match(active.game, active.events, fresh_videos)
        used_video_ids = {v.id for v, _ in pairs}

        for drive_file, event in pairs:
            if await _download_and_link(drive, db, drive_file, event):
                result.matched += 1
            else:
                result.errors += 1

        # Phase 3: AI fallback for leftover videos inside this folder.
        leftovers = [v for v in fresh_videos if v.id not in used_video_ids]
        if leftovers and ai.enabled:
            # Reload active.events state (some may have been linked just now)
            for v in leftovers:
                candidates = await _ai_candidates_for_game(active, db)
                if not candidates:
                    result.unmatched += 1
                    logger.warning(
                        "unmatched_goal_video name=%s folder=%s file_id=%s game=%s (no open events)",
                        v.name, folder_name, v.id, active.game.id,
                    )
                    continue
                ai_res = await ai.match(
                    filename=v.name,
                    parent_folder_name=folder_name,
                    drive_created_time=v.created_time.isoformat() if v.created_time else None,
                    candidates=candidates,
                )
                if ai_res and ai_res.event_id is not None and ai_res.confidence == "high":
                    event = next((e for e in active.events if e.id == ai_res.event_id), None)
                    if event and event.video_url is None:
                        if await _download_and_link(drive, db, v, event):
                            result.ai_event_matched += 1
                            continue
                logger.warning(
                    "unmatched_goal_video name=%s folder=%s file_id=%s game=%s ai=%s",
                    v.name, folder_name, v.id, active.game.id,
                    ai_res.reason if ai_res else "disabled",
                )
                result.unmatched += 1
        else:
            for v in leftovers:
                logger.warning(
                    "unmatched_goal_video name=%s folder=%s file_id=%s game=%s (ai disabled)",
                    v.name, folder_name, v.id, active.game.id,
                )
                result.unmatched += 1

    await _store_last_sync(datetime.now(timezone.utc))
    return result
