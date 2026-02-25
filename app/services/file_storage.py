"""File storage service using MinIO S3-compatible storage."""

import asyncio
import io
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import BinaryIO

from minio.error import S3Error

from app.minio_client import get_minio_client, get_public_url
from app.config import get_settings

settings = get_settings()


def _run_sync(fn, *args, **kwargs):
    """Helper to run a sync function in a thread pool (non-blocking)."""
    return asyncio.to_thread(partial(fn, *args, **kwargs))


class FileStorageService:
    """Service for storing and retrieving files using MinIO."""

    @staticmethod
    def _get_object_name(category: str, filename: str, file_id: str | None = None) -> str:
        """Generate object name with category prefix."""
        if file_id is None:
            file_id = str(uuid.uuid4())
        ext = filename.split(".")[-1] if "." in filename else ""
        return f"{category}/{file_id}.{ext}" if ext else f"{category}/{file_id}"

    @staticmethod
    async def upload_file(
        file_data: bytes | BinaryIO,
        filename: str,
        content_type: str,
        category: str = "uploads",
        metadata: dict | None = None,
    ) -> dict:
        """Upload a file to MinIO."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        file_id = str(uuid.uuid4())

        # If news-id is in metadata, include it in the path for prefix-based lookup
        news_id = (metadata or {}).get("news-id")
        if news_id and category == "news_image":
            object_name = f"news_image/{news_id}/{file_id}.{filename.rsplit('.', 1)[-1]}" if "." in filename else f"news_image/{news_id}/{file_id}"
        else:
            object_name = FileStorageService._get_object_name(category, filename, file_id)

        if isinstance(file_data, bytes):
            data = io.BytesIO(file_data)
            size = len(file_data)
        else:
            content = file_data.read()
            data = io.BytesIO(content)
            size = len(content)

        file_metadata = {
            "original-filename": filename,
            "uploaded-at": datetime.now(timezone.utc).isoformat(),
            "category": category,
            **(metadata or {}),
        }

        try:
            await _run_sync(
                client.put_object,
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=size,
                content_type=content_type,
                metadata=file_metadata,
            )

            return {
                "file_id": file_id,
                "object_name": object_name,
                "url": get_public_url(object_name),
                "filename": filename,
                "content_type": content_type,
                "size": size,
                "category": category,
            }

        except S3Error as e:
            raise RuntimeError(f"Failed to upload file: {e}")

    @staticmethod
    async def get_file(object_name: str) -> tuple[bytes, dict] | None:
        """Retrieve a file from MinIO."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        try:
            response = await _run_sync(client.get_object, bucket, object_name)
            content = response.read()
            response.close()
            response.release_conn()

            stat = await _run_sync(client.stat_object, bucket, object_name)
            metadata = {
                "filename": stat.metadata.get("x-amz-meta-original-filename", object_name),
                "content_type": stat.content_type,
                "size": stat.size,
                "last_modified": stat.last_modified,
                "category": stat.metadata.get("x-amz-meta-category"),
            }

            return content, metadata

        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            raise RuntimeError(f"Failed to get file: {e}")

    @staticmethod
    async def delete_file(object_name: str) -> bool:
        """Delete a file from MinIO."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        try:
            await _run_sync(client.remove_object, bucket, object_name)
            return True
        except S3Error:
            return False

    @staticmethod
    async def list_files(
        category: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """List files with optional category filtering.

        Uses only list_objects metadata (no extra stat_object per file).
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        prefix = f"{category}/" if category else ""

        def _list():
            files = []
            for obj in client.list_objects(bucket, prefix=prefix):
                if len(files) >= limit:
                    break
                file_id = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
                files.append({
                    "_id": file_id,
                    "object_name": obj.object_name,
                    "url": get_public_url(obj.object_name),
                    "filename": obj.object_name.split("/")[-1],
                    "content_type": obj.content_type,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                })
            return files

        return await asyncio.to_thread(_list)

    @staticmethod
    async def get_files_by_news_id(news_id: str) -> list[dict]:
        """Get all files for a specific news article.

        Uses prefix-based lookup: news_image/{news_id}/.
        Falls back to scanning news_image/ with stat_object for legacy files.
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        def _get_files():
            files = []

            # Fast path: prefix-based lookup (new upload format)
            for obj in client.list_objects(bucket, prefix=f"news_image/{news_id}/"):
                file_id = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
                files.append({
                    "_id": file_id,
                    "object_name": obj.object_name,
                    "url": get_public_url(obj.object_name),
                    "filename": obj.object_name.split("/")[-1],
                    "content_type": obj.content_type,
                    "size": obj.size,
                })

            if files:
                return files

            # Slow fallback: scan all news_image/ and check metadata (legacy files)
            for obj in client.list_objects(bucket, prefix="news_image/"):
                # Skip sub-prefixed files (already handled above)
                parts = obj.object_name.removeprefix("news_image/").split("/")
                if len(parts) > 1:
                    continue
                try:
                    stat = client.stat_object(bucket, obj.object_name)
                    if stat.metadata.get("x-amz-meta-news-id") == news_id:
                        file_id = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
                        files.append({
                            "_id": file_id,
                            "object_name": obj.object_name,
                            "url": get_public_url(obj.object_name),
                            "filename": stat.metadata.get("x-amz-meta-original-filename", obj.object_name),
                            "content_type": stat.content_type,
                            "size": obj.size,
                        })
                except S3Error:
                    continue

            return files

        return await asyncio.to_thread(_get_files)

    @staticmethod
    async def upload_team_logo(
        file_data: bytes,
        team_name: str,
        content_type: str = "image/webp",
    ) -> dict:
        """Upload team logo with standardized naming."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_name = team_name.lower().replace(" ", "-")
        ext = "webp" if "webp" in content_type else "png"
        object_name = f"public/team-logos/{safe_name}.{ext}"

        data = io.BytesIO(file_data)
        size = len(file_data)

        try:
            await _run_sync(
                client.put_object,
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=size,
                content_type=content_type,
                metadata={"team-name": safe_name},
            )

            return {
                "object_name": object_name,
                "url": get_public_url(object_name),
                "team_name": safe_name,
                "content_type": content_type,
            }

        except S3Error as e:
            raise RuntimeError(f"Failed to upload team logo: {e}")

    @staticmethod
    async def get_team_logo(team_name: str) -> tuple[bytes, dict] | None:
        """Get team logo by team name."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_name = team_name.lower().replace(" ", "-")

        for ext in ["webp", "png", "jpg"]:
            object_name = f"public/team-logos/{safe_name}.{ext}"
            try:
                response = await _run_sync(client.get_object, bucket, object_name)
                content = response.read()
                response.close()
                response.release_conn()

                return content, {
                    "filename": f"{safe_name}.{ext}",
                    "content_type": f"image/{ext}",
                    "team_name": safe_name,
                }
            except S3Error:
                continue

        return None

    @staticmethod
    async def list_team_logos() -> list[dict]:
        """List all team logos."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        def _list():
            logos = []
            for obj in client.list_objects(bucket, prefix="public/team-logos/"):
                name = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
                logos.append({
                    "object_name": obj.object_name,
                    "url": get_public_url(obj.object_name),
                    "team_name": name,
                    "size": obj.size,
                })
            return logos

        return await asyncio.to_thread(_list)

    @staticmethod
    async def upload_country_flag(
        file_data: bytes,
        country_code: str,
        content_type: str = "image/webp",
    ) -> dict:
        """Upload country flag with standardized naming."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_code = country_code.lower()
        ext = "webp" if "webp" in content_type else "png"
        object_name = f"public/country-flags/{safe_code}.{ext}"

        data = io.BytesIO(file_data)
        size = len(file_data)

        try:
            await _run_sync(
                client.put_object,
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=size,
                content_type=content_type,
                metadata={"country-code": safe_code},
            )

            return {
                "object_name": object_name,
                "url": get_public_url(object_name),
                "country_code": safe_code,
                "content_type": content_type,
            }

        except S3Error as e:
            raise RuntimeError(f"Failed to upload country flag: {e}")

    @staticmethod
    async def get_country_flag(country_code: str) -> tuple[bytes, dict] | None:
        """Get country flag by country code."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_code = country_code.lower()

        for ext in ["webp", "png", "svg"]:
            object_name = f"public/country-flags/{safe_code}.{ext}"
            try:
                response = await _run_sync(client.get_object, bucket, object_name)
                content = response.read()
                response.close()
                response.release_conn()

                return content, {
                    "filename": f"{safe_code}.{ext}",
                    "content_type": f"image/{ext}",
                    "country_code": safe_code,
                }
            except S3Error:
                continue

        return None

    @staticmethod
    async def list_country_flags() -> list[dict]:
        """List all country flags."""
        client = get_minio_client()
        bucket = settings.minio_bucket

        def _list():
            flags = []
            for obj in client.list_objects(bucket, prefix="public/country-flags/"):
                code = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
                flags.append({
                    "object_name": obj.object_name,
                    "url": get_public_url(obj.object_name),
                    "country_code": code.upper(),
                    "size": obj.size,
                })
            return flags

        return await asyncio.to_thread(_list)
