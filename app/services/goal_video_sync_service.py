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

import asyncio
import hashlib
import json
import logging
import mimetypes
import tempfile
from collections import defaultdict
from dataclasses import asdict, dataclass
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
from app.utils.file_urls import to_object_name
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
# Drive indexes ``modifiedTime`` with a few-minute lag, so we query with a
# time window that overlaps the previously-seen pointer. ``_is_processed``
# keeps this idempotent even when the same file shows up twice.
_SINCE_OVERLAP_MINUTES = 15


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


@dataclass(frozen=True)
class ProcessedGoalVideoRecord:
    file_id: str
    game_id: int
    event_id: int
    object_name: str
    folder_label: str | None = None


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


def _processed_record_key(file_id: str) -> str:
    return f"qfl:goal-videos:file:{file_id}"


async def _load_processed_record(file_id: str) -> ProcessedGoalVideoRecord | None:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        raw = await r.get(_processed_record_key(file_id))
        if not raw:
            return None
        data = json.loads(raw.decode())
        return ProcessedGoalVideoRecord(
            file_id=data["file_id"],
            game_id=int(data["game_id"]),
            event_id=int(data["event_id"]),
            object_name=data["object_name"],
            folder_label=data.get("folder_label"),
        )
    except Exception:
        logger.debug("Cannot read processed goal-video record for %s", file_id, exc_info=True)
        return None


async def clear_processed_goal_video_state(file_id: str) -> None:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        await r.srem(_PROCESSED_SET_KEY, file_id)
        await r.delete(_processed_record_key(file_id))
    except Exception:
        logger.debug("Cannot clear processed goal-video state for %s", file_id, exc_info=True)


async def _mark_processed(
    file_id: str,
    record: ProcessedGoalVideoRecord | None = None,
) -> None:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        await r.sadd(_PROCESSED_SET_KEY, file_id)
        await r.expire(_PROCESSED_SET_KEY, _PROCESSED_TTL_SECONDS)
        if record is not None:
            await r.set(
                _processed_record_key(file_id),
                json.dumps(asdict(record), ensure_ascii=False),
                ex=_PROCESSED_TTL_SECONDS,
            )
    except Exception:
        pass


async def _is_processed(file_id: str) -> bool:
    try:
        from app.utils.live_flag import get_redis

        r = await get_redis()
        return bool(await r.sismember(_PROCESSED_SET_KEY, file_id))
    except Exception:
        return False


def _folder_label_for(drive_file: DriveFile) -> str | None:
    if drive_file.ancestor_names:
        return drive_file.ancestor_names[-1]
    return drive_file.parent_name


async def _find_processed_record_in_storage(
    file_id: str,
) -> ProcessedGoalVideoRecord | None:
    from app.config import get_settings
    from app.minio_client import get_minio_client

    def _scan() -> ProcessedGoalVideoRecord | None:
        client = get_minio_client()
        bucket = get_settings().minio_bucket
        for obj in client.list_objects(bucket, prefix="goal_videos/", recursive=True):
            try:
                stat = client.stat_object(bucket, obj.object_name)
            except Exception:
                logger.debug("Failed to stat MinIO object %s", obj.object_name, exc_info=True)
                continue
            metadata = stat.metadata or {}
            drive_id = metadata.get("x-amz-meta-drive-file-id") or metadata.get("drive-file-id")
            if drive_id != file_id:
                continue
            game_id_raw = metadata.get("x-amz-meta-game-id") or metadata.get("game-id")
            try:
                game_id = int(game_id_raw) if game_id_raw is not None else int(obj.object_name.split("/", 2)[1])
                event_id = int(obj.object_name.rsplit("/", 1)[-1].split("-", 1)[0])
            except (IndexError, TypeError, ValueError):
                logger.warning(
                    "processed goal-video metadata is malformed for %s",
                    obj.object_name,
                )
                return None
            return ProcessedGoalVideoRecord(
                file_id=file_id,
                game_id=game_id,
                event_id=event_id,
                object_name=obj.object_name,
            )
        return None

    return await asyncio.to_thread(_scan)


async def lookup_processed_goal_video_record(
    file_id: str,
) -> ProcessedGoalVideoRecord | None:
    record = await _load_processed_record(file_id)
    if record is not None:
        return record
    return await _find_processed_record_in_storage(file_id)


async def _delete_goal_video_object(object_name: str | None) -> bool:
    normalized = to_object_name(object_name)
    if not normalized:
        return False
    deleted = await FileStorageService.delete_file(normalized)
    if not deleted:
        logger.warning("Failed to delete goal-video object %s", normalized)
    return deleted


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


def _direct_minute_match(
    events: list[GameEvent],
    videos: list[DriveFile],
) -> list[tuple[DriveFile, GameEvent]]:
    """Match videos to events by explicit minute + player name in filename.

    Handles filenames like ``Дүйсенбекұлы 22'.mp4`` — disambiguates two goals
    by the same player without needing Drive modifiedTime.
    """
    unmatched_events = [e for e in events if e.video_url is None and not _is_shootout_event(e)]
    if not unmatched_events or not videos:
        return []

    candidates: list[tuple[int, DriveFile, GameEvent]] = []
    for v in videos:
        parsed = parse_goal_filename(v.name)
        if not parsed or parsed.minute_hint is None or not parsed.player_hint:
            continue
        norm_hint = _normalize_player(parsed.player_hint)
        for event in unmatched_events:
            if event.minute != parsed.minute_hint:
                continue
            if not event.player_name:
                continue
            if norm_hint not in _normalize_player(event.player_name):
                continue
            candidates.append((len(norm_hint), v, event))

    candidates.sort(key=lambda x: -x[0])
    used_videos: set[str] = set()
    used_events: set[int] = set()
    pairs: list[tuple[DriveFile, GameEvent]] = []
    for _score, v, event in candidates:
        if v.id in used_videos or event.id in used_events:
            continue
        pairs.append((v, event))
        used_videos.add(v.id)
        used_events.add(event.id)
    return pairs


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
    drive,
    db: AsyncSession,
    drive_file: DriveFile,
    event: GameEvent,
    *,
    previous_record: ProcessedGoalVideoRecord | None = None,
    folder_label: str | None = None,
    remove_previous_attachment: bool = True,
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
                threads=settings.goal_video_transcode_threads,
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

    previous_game_id: int | None = None
    previous_object_name: str | None = None
    if (
        previous_record is not None
        and remove_previous_attachment
        and (
            previous_record.game_id != event.game_id
            or previous_record.event_id != event.id
            or previous_record.object_name != object_name
        )
    ):
        previous_event = await db.get(GameEvent, previous_record.event_id)
        if previous_event is not None and previous_event.video_url:
            attached_object_name = to_object_name(previous_event.video_url)
            if attached_object_name == previous_record.object_name:
                previous_event.video_url = None
                previous_game_id = previous_event.game_id
                previous_object_name = previous_record.object_name
            else:
                logger.warning(
                    "processed_goal_video_record_stale file_id=%s stored_event=%s stored_object=%s attached_object=%s",
                    drive_file.id,
                    previous_record.event_id,
                    previous_record.object_name,
                    attached_object_name,
                )

    event.video_url = object_name
    await db.commit()

    if previous_object_name:
        await _delete_goal_video_object(previous_object_name)

    record = ProcessedGoalVideoRecord(
        file_id=drive_file.id,
        game_id=event.game_id,
        event_id=event.id,
        object_name=object_name,
        folder_label=folder_label or _folder_label_for(drive_file),
    )
    await _mark_processed(drive_file.id, record=record)
    logger.info(
        "Linked Drive file %s → event %s (game %s, %s, minute %s, %.1f MB)",
        drive_file.name, event.id, event.game_id, event.player_name, event.minute,
        len(payload) / 1024 / 1024,
    )

    # Bust the ISR cache of the match page so the fresh video_url is picked up
    # by the public site without waiting for the revalidate interval.
    try:
        from app.services.game_lifecycle import _revalidate_match_page
        await _revalidate_match_page(event.game_id)
        if previous_game_id and previous_game_id != event.game_id:
            await _revalidate_match_page(previous_game_id)
    except Exception:
        logger.exception(
            "revalidate match page failed for game %s (previous_game=%s)",
            event.game_id,
            previous_game_id,
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


async def relink_drive_file_to_event(
    db: AsyncSession,
    drive_file_id: str,
    target_event_id: int,
    *,
    remove_previous_attachment: bool = True,
    drive=None,
) -> ProcessedGoalVideoRecord:
    event = await db.get(GameEvent, target_event_id)
    if event is None:
        raise ValueError(f"Event {target_event_id} not found")

    drive = drive or get_drive_client()
    drive_file = await drive.get_file(drive_file_id)
    if drive_file is None:
        raise ValueError(f"Drive file {drive_file_id} not found")

    previous_record = await lookup_processed_goal_video_record(drive_file_id)
    ok = await _download_and_link(
        drive,
        db,
        drive_file,
        event,
        previous_record=previous_record,
        folder_label=_folder_label_for(drive_file) or (previous_record.folder_label if previous_record else None),
        remove_previous_attachment=remove_previous_attachment,
    )
    if not ok:
        raise RuntimeError(
            f"Failed to relink Drive file {drive_file_id} to event {target_event_id}"
        )

    record = await lookup_processed_goal_video_record(drive_file_id)
    if record is None:
        raise RuntimeError(
            f"Relink succeeded but processed record is missing for {drive_file_id}"
        )
    return record


# ---------------------------------------------------------------------------
# Pointer advancement
# ---------------------------------------------------------------------------

def _compute_next_sync_pointer(
    videos: list[DriveFile],
    previous: datetime,
) -> datetime:
    """Pick the next ``last-sync`` cursor from what Drive returned.

    Never advance past the latest ``modifiedTime`` we actually saw; if
    Drive returned nothing, leave the pointer alone. Otherwise a file
    whose Drive index arrives late (after our tick that would have
    spotted it) falls off the horizon forever.
    """
    if not videos:
        return previous
    max_mod = max(
        (v.modified_time for v in videos if v.modified_time),
        default=None,
    )
    if max_mod is None:
        return previous
    return max(previous, max_mod)


async def _prepare_bucket_videos_for_processing(
    folder_name: str,
    bucket_videos: list[DriveFile],
    active: ActiveGame,
    result: SyncResult,
) -> tuple[list[DriveFile], dict[str, ProcessedGoalVideoRecord]]:
    fresh_videos: list[DriveFile] = []
    previous_records: dict[str, ProcessedGoalVideoRecord] = {}

    for drive_file in bucket_videos:
        if not await _is_processed(drive_file.id):
            fresh_videos.append(drive_file)
            continue

        record = await lookup_processed_goal_video_record(drive_file.id)
        if record is not None and record.game_id != active.game.id:
            previous_records[drive_file.id] = record
            fresh_videos.append(drive_file)
            logger.warning(
                "processed_goal_video_game_mismatch file_id=%s folder=%s stored_game=%s stored_event=%s target_game=%s",
                drive_file.id,
                folder_name,
                record.game_id,
                record.event_id,
                active.game.id,
            )
            continue

        result.skipped_already_processed += 1

    return fresh_videos, previous_records


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
        return result

    last_seen = await _load_last_sync()
    since_query = last_seen - timedelta(minutes=_SINCE_OVERLAP_MINUTES)
    drive = get_drive_client()
    try:
        # max_depth=3 to support root → tour → [optional date] → match → clips.
        videos = await drive.list_recent_videos_recursive(
            settings.google_drive_goals_folder_id, since=since_query, max_depth=3
        )
    except Exception:
        logger.exception("Failed to list Drive videos")
        result.errors += 1
        return result

    result.listed = len(videos)
    if not videos:
        return result

    # Group videos by their match-folder label (deepest ancestor). Processed
    # files are filtered later, after folder→game resolution, so a Drive clip
    # that was historically linked to the wrong game can be reprocessed when
    # it reappears under a folder resolving to a different match.
    buckets: dict[str, list[DriveFile]] = defaultdict(list)
    for v in videos:
        if not v.ancestor_names:
            continue
        buckets[v.ancestor_names[-1]].append(v)

    if not buckets:
        await _store_last_sync(_compute_next_sync_pointer(videos, last_seen))
        return result

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
    for folder_name, bucket_videos in buckets.items():
        active = folder_to_game.get(folder_name)
        if active is None:
            logger.info("No game for folder %r (%d videos)", folder_name, len(bucket_videos))
            result.skipped_no_game += len(bucket_videos)
            continue

        fresh_videos, previous_records = await _prepare_bucket_videos_for_processing(
            folder_name,
            bucket_videos,
            active,
            result,
        )
        if not fresh_videos:
            continue

        # Phase 2a: direct minute+name match ("Зинадин 24'.mp4" style).
        direct_pairs = _direct_minute_match(active.events, fresh_videos)
        used_video_ids: set[str] = {v.id for v, _ in direct_pairs}
        used_event_ids: set[int] = {e.id for _, e in direct_pairs}

        for drive_file, event in direct_pairs:
            if await _download_and_link(
                drive,
                db,
                drive_file,
                event,
                previous_record=previous_records.get(drive_file.id),
                folder_label=folder_name,
            ):
                result.matched += 1
            else:
                result.errors += 1

        # Phase 2b: timing-based matching for remaining videos/events.
        remaining_videos = [v for v in fresh_videos if v.id not in used_video_ids]
        remaining_events = [e for e in active.events if e.id not in used_event_ids]
        pairs = _optimal_time_match(active.game, remaining_events, remaining_videos)
        used_video_ids.update(v.id for v, _ in pairs)

        for drive_file, event in pairs:
            if await _download_and_link(
                drive,
                db,
                drive_file,
                event,
                previous_record=previous_records.get(drive_file.id),
                folder_label=folder_name,
            ):
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
                        if await _download_and_link(
                            drive,
                            db,
                            v,
                            event,
                            previous_record=previous_records.get(v.id),
                            folder_label=folder_name,
                        ):
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

    await _store_last_sync(_compute_next_sync_pointer(videos, last_seen))
    return result
