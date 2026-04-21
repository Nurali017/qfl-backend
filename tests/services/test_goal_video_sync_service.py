from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.models.game_event import GameEvent, GameEventType
from app.services import goal_video_sync_service as gvs
from app.services.google_drive_client import DriveFile


def _drive_file(name: str = "goal.mp4", mime_type: str = "video/mp4") -> DriveFile:
    now = datetime.now(timezone.utc)
    return DriveFile(
        id="drive-file-1",
        name=name,
        mime_type=mime_type,
        size=123,
        created_time=now,
        modified_time=now,
        parent_id="folder-1",
        parent_name="Match",
        ancestor_names=("6- Тур", "Улытау Тобыл"),
    )


def _event() -> GameEvent:
    return GameEvent(
        id=16852,
        game_id=931,
        half=1,
        minute=7,
        event_type=GameEventType.goal,
        player_name="Урош Милованович",
    )


def test_object_name_for_includes_content_hash_suffix():
    event = _event()
    drive_file = _drive_file(name="goal.mp4")

    object_name = gvs._object_name_for(event, drive_file, b"first-version")

    assert object_name.startswith("goal_videos/931/16852-")
    assert object_name.endswith(".mp4")


def test_object_name_for_changes_when_payload_changes():
    event = _event()
    drive_file = _drive_file(name="goal.mp4")

    first = gvs._object_name_for(event, drive_file, b"first-version")
    second = gvs._object_name_for(event, drive_file, b"corrected-version")

    assert first != second


def test_object_name_for_stays_stable_for_same_payload():
    event = _event()
    drive_file = _drive_file(name="goal.mp4")

    first = gvs._object_name_for(event, drive_file, b"same-payload")
    second = gvs._object_name_for(event, drive_file, b"same-payload")

    assert first == second


def _drive_file_at(mod: datetime | None, file_id: str = "f") -> DriveFile:
    return DriveFile(
        id=file_id,
        name="goal.mp4",
        mime_type="video/mp4",
        size=1,
        created_time=mod,
        modified_time=mod,
        parent_id="folder",
        parent_name="Match",
        ancestor_names=("tour", "match"),
    )


def test_compute_next_sync_pointer_keeps_previous_when_drive_empty():
    previous = datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc)

    assert gvs._compute_next_sync_pointer([], previous) == previous


def test_compute_next_sync_pointer_advances_to_max_modified_time():
    previous = datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc)
    videos = [
        _drive_file_at(datetime(2026, 4, 21, 10, 16, 24, tzinfo=timezone.utc), "a"),
        _drive_file_at(datetime(2026, 4, 21, 10, 27, 57, tzinfo=timezone.utc), "b"),
        _drive_file_at(datetime(2026, 4, 21, 10, 26, 39, tzinfo=timezone.utc), "c"),
    ]

    assert gvs._compute_next_sync_pointer(videos, previous) == datetime(
        2026, 4, 21, 10, 27, 57, tzinfo=timezone.utc
    )


def test_compute_next_sync_pointer_never_rewinds_on_overlap_rereads():
    """Overlap window may re-surface files older than the pointer — that
    must not rewind the cursor."""
    previous = datetime(2026, 4, 21, 10, 30, tzinfo=timezone.utc)
    videos = [
        _drive_file_at(datetime(2026, 4, 21, 10, 16, 24, tzinfo=timezone.utc), "old"),
    ]

    assert gvs._compute_next_sync_pointer(videos, previous) == previous


def test_compute_next_sync_pointer_ignores_videos_without_modified_time():
    previous = datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc)
    videos = [_drive_file_at(None, "no-mod")]

    assert gvs._compute_next_sync_pointer(videos, previous) == previous


@pytest.mark.asyncio
async def test_download_and_link_persists_versioned_video_url(monkeypatch):
    event = _event()
    drive_file = _drive_file(name="goal.mp4")
    db = SimpleNamespace(commit=AsyncMock())
    drive = SimpleNamespace(download_file=AsyncMock(return_value=b"corrected-video-payload"))
    upload_mock = AsyncMock()

    monkeypatch.setattr(
        gvs,
        "get_settings",
        lambda: SimpleNamespace(
            goal_video_transcode_enabled=False,
            goal_video_transcode_crf="20",
            goal_video_transcode_preset="medium",
        ),
    )
    monkeypatch.setattr(gvs.FileStorageService, "upload_file", upload_mock)
    monkeypatch.setattr(gvs, "_mark_processed", AsyncMock())

    ok = await gvs._download_and_link(drive, db, drive_file, event)

    assert ok is True
    assert event.video_url.startswith("goal_videos/931/16852-")
    assert event.video_url.endswith(".mp4")
    upload_mock.assert_awaited_once()
    assert upload_mock.await_args.kwargs["object_name"] == event.video_url
    db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_download_and_link_posts_telegram_inline_from_payload(monkeypatch):
    event = _event()
    event.telegram_message_id = 555
    drive_file = _drive_file(name="goal.mp4")
    db = SimpleNamespace(commit=AsyncMock())
    drive = SimpleNamespace(download_file=AsyncMock(return_value=b"goal-payload"))

    monkeypatch.setattr(
        gvs,
        "get_settings",
        lambda: SimpleNamespace(
            goal_video_transcode_enabled=False,
            goal_video_transcode_crf="20",
            goal_video_transcode_preset="medium",
        ),
    )
    monkeypatch.setattr(gvs.FileStorageService, "upload_file", AsyncMock())
    monkeypatch.setattr(gvs, "_mark_processed", AsyncMock())
    inline_mock = AsyncMock(return_value=True)
    enqueue_mock = Mock()
    monkeypatch.setattr(gvs, "_post_goal_video_from_payload", inline_mock)
    monkeypatch.setattr(gvs, "_enqueue_goal_video_followup", enqueue_mock)

    ok = await gvs._download_and_link(drive, db, drive_file, event)

    assert ok is True
    inline_mock.assert_awaited_once_with(db, drive_file, event, b"goal-payload")
    enqueue_mock.assert_not_called()


@pytest.mark.asyncio
async def test_download_and_link_enqueues_fallback_when_inline_post_fails(monkeypatch):
    event = _event()
    event.telegram_message_id = 555
    drive_file = _drive_file(name="goal.mp4")
    db = SimpleNamespace(commit=AsyncMock())
    drive = SimpleNamespace(download_file=AsyncMock(return_value=b"goal-payload"))

    monkeypatch.setattr(
        gvs,
        "get_settings",
        lambda: SimpleNamespace(
            goal_video_transcode_enabled=False,
            goal_video_transcode_crf="20",
            goal_video_transcode_preset="medium",
        ),
    )
    monkeypatch.setattr(gvs.FileStorageService, "upload_file", AsyncMock())
    monkeypatch.setattr(gvs, "_mark_processed", AsyncMock())
    monkeypatch.setattr(gvs, "_post_goal_video_from_payload", AsyncMock(return_value=False))
    enqueue_mock = Mock()
    monkeypatch.setattr(gvs, "_enqueue_goal_video_followup", enqueue_mock)

    ok = await gvs._download_and_link(drive, db, drive_file, event)

    assert ok is True
    enqueue_mock.assert_called_once_with(event.id)
