"""Google Drive client for goal video ingest.

Uses a Google Cloud service account (JSON key) for authentication — the
Drive folder is expected to be shared (read) with the service account's
email. Synchronous Drive API calls are wrapped with ``asyncio.to_thread``
so the rest of the codebase can stay async.
"""

from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.config import get_settings

logger = logging.getLogger(__name__)

_DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)
_LIST_FIELDS = "files(id,name,mimeType,size,createdTime,modifiedTime,parents)"


@dataclass(frozen=True)
class DriveFile:
    id: str
    name: str
    mime_type: str
    size: int | None
    created_time: datetime | None
    modified_time: datetime | None
    parent_id: str | None
    parent_name: str | None
    ancestor_names: tuple[str, ...] = ()  # path from root down to parent, e.g. ("6- Тур", "ЕЛИМАЙ АСТАНА")


@dataclass(frozen=True)
class DriveFolder:
    id: str
    name: str


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    # Drive returns RFC3339 with a trailing 'Z'.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class GoogleDriveClient:
    """Lazy, thread-safe wrapper around google-api-python-client."""

    def __init__(self) -> None:
        self._service = None
        self._lock = asyncio.Lock()

    async def _get_service(self):
        if self._service is not None:
            return self._service
        async with self._lock:
            if self._service is not None:
                return self._service
            self._service = await asyncio.to_thread(self._build_service)
            return self._service

    def _build_service(self):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        settings = get_settings()
        if not settings.google_service_account_file:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_FILE is not configured")

        credentials = service_account.Credentials.from_service_account_file(
            settings.google_service_account_file,
            scopes=list(_DRIVE_SCOPES),
        )
        return build("drive", "v3", credentials=credentials, cache_discovery=False)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def list_subfolders(self, parent_id: str) -> list[DriveFolder]:
        """List direct subfolders of *parent_id*."""
        service = await self._get_service()
        query = (
            f"'{parent_id}' in parents "
            "and mimeType = 'application/vnd.google-apps.folder' "
            "and trashed = false"
        )

        def _call():
            return (
                service.files()
                .list(q=query, fields="files(id,name)", pageSize=100, supportsAllDrives=True, includeItemsFromAllDrives=True)
                .execute()
            )

        result = await asyncio.to_thread(_call)
        return [DriveFolder(id=f["id"], name=f["name"]) for f in result.get("files", [])]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def list_videos_in_folder(
        self,
        folder_id: str,
        since: datetime | None = None,
        parent_name: str | None = None,
        ancestor_names: tuple[str, ...] = (),
    ) -> list[DriveFile]:
        """List video files directly in *folder_id* (non-recursive)."""
        service = await self._get_service()
        parts = [
            f"'{folder_id}' in parents",
            "mimeType contains 'video/'",
            "trashed = false",
        ]
        if since is not None:
            parts.append(f"modifiedTime > '{since.isoformat().replace('+00:00', 'Z')}'")
        query = " and ".join(parts)

        def _call():
            return (
                service.files()
                .list(
                    q=query,
                    fields=_LIST_FIELDS,
                    pageSize=100,
                    orderBy="modifiedTime",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )

        result = await asyncio.to_thread(_call)
        files: list[DriveFile] = []
        for item in result.get("files", []):
            files.append(
                DriveFile(
                    id=item["id"],
                    name=item.get("name", ""),
                    mime_type=item.get("mimeType", ""),
                    size=int(item["size"]) if item.get("size") else None,
                    created_time=_parse_dt(item.get("createdTime")),
                    modified_time=_parse_dt(item.get("modifiedTime")),
                    parent_id=folder_id,
                    parent_name=parent_name,
                    ancestor_names=ancestor_names,
                )
            )
        return files

    async def list_recent_videos_recursive(
        self,
        root_folder_id: str,
        since: datetime | None = None,
        max_depth: int = 2,
    ) -> list[DriveFile]:
        """Collect videos from root + N levels of subfolders.

        Real-world layout: root → «<N>- Тур» → «HOME_NAME AWAY_NAME» → clips.
        So ``max_depth=2`` is the default.
        """

        async def _walk(folder_id: str, ancestors: tuple[str, ...], depth_left: int) -> list[DriveFile]:
            collected: list[DriveFile] = []
            try:
                collected.extend(
                    await self.list_videos_in_folder(
                        folder_id,
                        since=since,
                        parent_name=ancestors[-1] if ancestors else None,
                        ancestor_names=ancestors,
                    )
                )
            except Exception:
                logger.exception("Failed to list videos in folder %s (%s)", folder_id, ancestors)

            if depth_left <= 0:
                return collected

            try:
                subfolders = await self.list_subfolders(folder_id)
            except Exception:
                logger.exception("Failed to list subfolders of %s", folder_id)
                return collected

            for sub in subfolders:
                sub_ancestors = ancestors + (sub.name,)
                collected.extend(await _walk(sub.id, sub_ancestors, depth_left - 1))

            return collected

        return await _walk(root_folder_id, (), max_depth)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def download_file(self, file_id: str) -> bytes:
        """Download the entire file into memory.

        Goal clips are small (~10-50 MB), so in-memory download is acceptable
        and keeps the caller free of temp-file management. Caller is expected
        to immediately stream the bytes to MinIO.
        """
        service = await self._get_service()

        def _call() -> bytes:
            from googleapiclient.http import MediaIoBaseDownload

            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _status, done = downloader.next_chunk()
            return buffer.getvalue()

        return await asyncio.to_thread(_call)


_client: GoogleDriveClient | None = None


def get_drive_client() -> GoogleDriveClient:
    global _client
    if _client is None:
        _client = GoogleDriveClient()
    return _client


def reset_drive_client() -> None:
    """Test helper — clears the module-level singleton."""
    global _client
    _client = None
