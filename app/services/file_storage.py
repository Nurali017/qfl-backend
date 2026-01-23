"""File storage service using MinIO S3-compatible storage."""

import io
import uuid
from datetime import datetime
from typing import BinaryIO

from minio.error import S3Error

from app.minio_client import get_minio_client, get_public_url
from app.config import get_settings

settings = get_settings()


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
        """
        Upload a file to MinIO.

        Args:
            file_data: File content as bytes or file-like object
            filename: Original filename
            content_type: MIME type of the file
            category: Category/folder (team_logo, news_image, document)
            metadata: Optional additional metadata

        Returns:
            Dict with file_id, url, and metadata
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        # Generate unique object name
        file_id = str(uuid.uuid4())
        object_name = FileStorageService._get_object_name(category, filename, file_id)

        # Prepare data
        if isinstance(file_data, bytes):
            data = io.BytesIO(file_data)
            size = len(file_data)
        else:
            content = file_data.read()
            data = io.BytesIO(content)
            size = len(content)

        # Prepare metadata
        file_metadata = {
            "original-filename": filename,
            "uploaded-at": datetime.utcnow().isoformat(),
            "category": category,
            **(metadata or {}),
        }

        try:
            client.put_object(
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
        """
        Retrieve a file from MinIO.

        Args:
            object_name: Full object path (category/file_id.ext)

        Returns:
            Tuple of (file_content, metadata) or None if not found
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        try:
            response = client.get_object(bucket, object_name)
            content = response.read()
            response.close()
            response.release_conn()

            # Get object stat for metadata
            stat = client.stat_object(bucket, object_name)
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
        """
        Delete a file from MinIO.

        Args:
            object_name: Full object path

        Returns:
            True if deleted, False otherwise
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        try:
            client.remove_object(bucket, object_name)
            return True
        except S3Error:
            return False

    @staticmethod
    async def list_files(
        category: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """
        List files with optional category filtering.

        Args:
            category: Filter by category (folder prefix)
            limit: Maximum number of results

        Returns:
            List of file metadata dictionaries
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        prefix = f"{category}/" if category else ""

        files = []
        objects = client.list_objects(bucket, prefix=prefix)

        for obj in objects:
            if len(files) >= limit:
                break

            try:
                stat = client.stat_object(bucket, obj.object_name)
                file_id = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
                files.append({
                    "_id": file_id,
                    "object_name": obj.object_name,
                    "url": get_public_url(obj.object_name),
                    "filename": stat.metadata.get("x-amz-meta-original-filename", obj.object_name),
                    "content_type": stat.content_type,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                    "category": stat.metadata.get("x-amz-meta-category"),
                    "language": stat.metadata.get("x-amz-meta-language"),
                    "news_id": stat.metadata.get("x-amz-meta-news-id"),
                })
            except S3Error:
                continue

        return files

    @staticmethod
    async def get_files_by_news_id(news_id: str) -> list[dict]:
        """
        Get all files for a specific news article.

        Args:
            news_id: News article ID

        Returns:
            List of file metadata dictionaries
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        files = []
        # Search in news_image category
        objects = client.list_objects(bucket, prefix="news_image/")

        for obj in objects:
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

    @staticmethod
    async def upload_team_logo(
        file_data: bytes,
        team_name: str,
        content_type: str = "image/webp",
    ) -> dict:
        """
        Upload team logo with standardized naming.

        Args:
            file_data: Image content as bytes
            team_name: Team name (lowercase, no spaces)
            content_type: Image MIME type

        Returns:
            Dict with url and metadata
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        # Standardize team name
        safe_name = team_name.lower().replace(" ", "-")
        ext = "webp" if "webp" in content_type else "png"
        object_name = f"public/team-logos/{safe_name}.{ext}"

        data = io.BytesIO(file_data)
        size = len(file_data)

        try:
            client.put_object(
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
        """
        Get team logo by team name.

        Args:
            team_name: Team name

        Returns:
            Tuple of (file_content, metadata) or None
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_name = team_name.lower().replace(" ", "-")

        # Try webp first, then png
        for ext in ["webp", "png", "jpg"]:
            object_name = f"public/team-logos/{safe_name}.{ext}"
            try:
                response = client.get_object(bucket, object_name)
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

        logos = []
        objects = client.list_objects(bucket, prefix="public/team-logos/")

        for obj in objects:
            name = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
            logos.append({
                "object_name": obj.object_name,
                "url": get_public_url(obj.object_name),
                "team_name": name,
                "size": obj.size,
            })

        return logos

    @staticmethod
    async def upload_country_flag(
        file_data: bytes,
        country_code: str,
        content_type: str = "image/webp",
    ) -> dict:
        """
        Upload country flag with standardized naming.

        Args:
            file_data: Image content as bytes
            country_code: ISO 3166-1 alpha-2 code (e.g., "KZ")
            content_type: Image MIME type

        Returns:
            Dict with url and metadata
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_code = country_code.lower()
        ext = "webp" if "webp" in content_type else "png"
        object_name = f"public/country-flags/{safe_code}.{ext}"

        data = io.BytesIO(file_data)
        size = len(file_data)

        try:
            client.put_object(
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
        """
        Get country flag by country code.

        Args:
            country_code: ISO 3166-1 alpha-2 code

        Returns:
            Tuple of (file_content, metadata) or None
        """
        client = get_minio_client()
        bucket = settings.minio_bucket

        safe_code = country_code.lower()

        for ext in ["webp", "png", "svg"]:
            object_name = f"public/country-flags/{safe_code}.{ext}"
            try:
                response = client.get_object(bucket, object_name)
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

        flags = []
        objects = client.list_objects(bucket, prefix="public/country-flags/")

        for obj in objects:
            code = obj.object_name.split("/")[-1].rsplit(".", 1)[0]
            flags.append({
                "object_name": obj.object_name,
                "url": get_public_url(obj.object_name),
                "country_code": code.upper(),
                "size": obj.size,
            })

        return flags
