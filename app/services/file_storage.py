"""File storage service using MinIO S3-compatible storage."""

import asyncio
import io
import logging
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import BinaryIO
from urllib.parse import quote

import numpy as np
from PIL import Image
from minio.error import S3Error
from rembg import new_session, remove

from app.minio_client import get_minio_client, get_public_url
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Categories where uploaded images should be auto-optimized
_OPTIMIZE_CATEGORIES = {"player_photos", "coach_photos"}
_MAX_IMAGE_SIZE = (800, 1200)
_WEBP_QUALITY = 85

# Lazy-loaded rembg session (u2netp is a lightweight 4.7 MB model)
_rembg_session = None


def _get_rembg_session():
    global _rembg_session
    if _rembg_session is None:
        _rembg_session = new_session("u2netp")
    return _rembg_session


def _remove_background(img: Image.Image) -> Image.Image:
    """Remove background from a portrait photo using rembg.

    Returns RGBA image with transparent background.
    Raises ValueError if the result quality is poor (alpha covers <10% or >95%).
    """
    result = remove(img, session=_get_rembg_session())
    result = result.convert("RGBA")

    # Quality gate: check alpha channel coverage
    alpha = np.array(result.split()[-1])
    opaque_ratio = np.count_nonzero(alpha > 128) / alpha.size

    if opaque_ratio < 0.10 or opaque_ratio > 0.95:
        raise ValueError(
            f"Background removal quality poor (opaque={opaque_ratio:.0%}), skipping"
        )

    return result


def _run_sync(fn, *args, **kwargs):
    """Helper to run a sync function in a thread pool (non-blocking)."""
    return asyncio.to_thread(partial(fn, *args, **kwargs))


def _optimize_image(
    file_data: bytes,
    max_size: tuple[int, int] = _MAX_IMAGE_SIZE,
    quality: int = _WEBP_QUALITY,
    remove_bg: bool = True,
) -> tuple[bytes, str]:
    """Resize and convert an image to WebP.

    If remove_bg=True, removes background first and preserves alpha channel.
    Returns (optimized_bytes, content_type).
    """
    img = Image.open(io.BytesIO(file_data))

    # Try background removal for person photos
    bg_removed = False
    if remove_bg:
        try:
            img = _remove_background(img)
            bg_removed = True
            logger.info("Background removed successfully")
        except Exception:
            logger.warning("Background removal failed, keeping original", exc_info=True)

    if bg_removed:
        # Keep RGBA for transparent WebP
        if img.mode != "RGBA":
            img = img.convert("RGBA")
    else:
        # Convert to RGB (no transparency needed)
        if img.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            background.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")

    # Resize preserving aspect ratio
    img.thumbnail(max_size, Image.LANCZOS)

    buf = io.BytesIO()
    img.save(buf, format="WEBP", quality=quality)
    return buf.getvalue(), "image/webp"


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

        # Read raw bytes once
        if isinstance(file_data, bytes):
            raw_bytes = file_data
        else:
            raw_bytes = file_data.read()

        # Auto-optimize photos on upload
        if category in _OPTIMIZE_CATEGORIES and content_type.startswith("image/"):
            try:
                raw_bytes, content_type = _optimize_image(raw_bytes)
                filename = filename.rsplit(".", 1)[0] + ".webp"
                logger.info("Optimized %s image: %d bytes", category, len(raw_bytes))
            except Exception:
                logger.warning("Image optimization failed, uploading original", exc_info=True)

        # If news-id is in metadata, include it in the path for prefix-based lookup
        news_id = (metadata or {}).get("news-id")
        if news_id and category == "news_image":
            object_name = f"news_image/{news_id}/{file_id}.{filename.rsplit('.', 1)[-1]}" if "." in filename else f"news_image/{news_id}/{file_id}"
        else:
            object_name = FileStorageService._get_object_name(category, filename, file_id)

        data = io.BytesIO(raw_bytes)
        size = len(raw_bytes)

        file_metadata = {
            "original-filename": quote(filename, safe=""),
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
