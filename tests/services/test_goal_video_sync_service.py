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


# ---------------------------------------------------------------------------
# Path-based pipeline (PR6): _object_name_from_path + _download_and_link
# with drive.download_file_to_path
# ---------------------------------------------------------------------------

def test_object_name_from_path_matches_bytes_version(tmp_path):
    event = _event()
    drive_file = _drive_file(name="goal.mp4")
    payload = b"corrected-video-payload"

    file_path = tmp_path / "raw.mp4"
    file_path.write_bytes(payload)

    bytes_name = gvs._object_name_for(event, drive_file, payload)
    path_name = gvs._object_name_from_path(event, drive_file, file_path)

    # Same blake2b digest, same extension layout — names must match.
    assert bytes_name == path_name


def test_object_name_from_path_changes_with_content(tmp_path):
    event = _event()
    drive_file = _drive_file(name="goal.mp4")

    a = tmp_path / "a.mp4"
    a.write_bytes(b"first-version")
    b = tmp_path / "b.mp4"
    b.write_bytes(b"corrected-version")

    assert gvs._object_name_from_path(event, drive_file, a) != gvs._object_name_from_path(event, drive_file, b)


@pytest.mark.asyncio
async def test_download_and_link_path_pipeline_persists_and_cleans_up(monkeypatch, tmp_path):
    """When drive exposes download_file_to_path, the path-based pipeline runs.

    Verifies:
    - download_file_to_path is called with a path inside a tempdir
    - upload_file_from_path is called with the same path
    - DB.commit() runs
    - The tempdir is cleaned up on success (caller's view: path no longer exists)
    """
    event = _event()
    drive_file = _drive_file(name="goal.mp4")
    db = SimpleNamespace(commit=AsyncMock())

    captured_paths: list[str] = []

    async def fake_download_to_path(file_id, dest):
        from pathlib import Path as _Path
        p = _Path(dest)
        p.write_bytes(b"streamed-video-payload")
        captured_paths.append(str(p))
        return p.stat().st_size

    drive = SimpleNamespace(download_file_to_path=fake_download_to_path)

    upload_paths: list[str] = []

    async def fake_upload(path, *, object_name, content_type, category, metadata):
        from pathlib import Path as _Path
        p = _Path(path)
        assert p.exists(), "upload_file_from_path must run while the tempfile is still on disk"
        upload_paths.append(str(p))
        return {"object_name": object_name}

    monkeypatch.setattr(
        gvs,
        "get_settings",
        lambda: SimpleNamespace(
            goal_video_transcode_enabled=False,
            goal_video_transcode_crf="20",
            goal_video_transcode_preset="medium",
            goal_video_transcode_threads="0",
        ),
    )
    monkeypatch.setattr(gvs.FileStorageService, "upload_file_from_path", fake_upload)
    monkeypatch.setattr(gvs, "_mark_processed", AsyncMock())

    ok = await gvs._download_and_link(drive, db, drive_file, event)

    assert ok is True
    assert event.video_url.startswith("goal_videos/931/16852-")
    assert event.video_url.endswith(".mp4")
    db.commit.assert_awaited_once()

    # download and upload saw the same tempfile path
    assert len(captured_paths) == 1
    assert len(upload_paths) == 1
    assert captured_paths[0] == upload_paths[0]

    # Tempdir auto-removed when the context manager exited
    from pathlib import Path as _Path
    assert not _Path(captured_paths[0]).exists()


@pytest.mark.asyncio
async def test_download_and_link_path_pipeline_cleans_up_on_download_failure(monkeypatch, tmp_path):
    """A failed download_file_to_path must still produce a clean tempdir on exit."""
    event = _event()
    drive_file = _drive_file(name="goal.mp4")
    db = SimpleNamespace(commit=AsyncMock())

    captured_paths: list[str] = []

    async def fake_download_to_path(file_id, dest):
        from pathlib import Path as _Path
        captured_paths.append(str(_Path(dest).parent))
        raise RuntimeError("boom")

    drive = SimpleNamespace(download_file_to_path=fake_download_to_path)

    monkeypatch.setattr(
        gvs,
        "get_settings",
        lambda: SimpleNamespace(
            goal_video_transcode_enabled=False,
            goal_video_transcode_crf="20",
            goal_video_transcode_preset="medium",
            goal_video_transcode_threads="0",
        ),
    )

    ok = await gvs._download_and_link(drive, db, drive_file, event)

    assert ok is False
    db.commit.assert_not_called()

    from pathlib import Path as _Path
    assert len(captured_paths) == 1
    assert not _Path(captured_paths[0]).exists(), "tempdir must be cleaned up after download failure"
