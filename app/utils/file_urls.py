"""File URL utilities."""

from __future__ import annotations

from sqlalchemy import Text
from sqlalchemy.types import TypeDecorator

from app.config import get_settings


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def resolve_file_url(value: str | None) -> str | None:
    """Convert a stored object name to a full public URL.

    * If *value* already starts with ``http(s)://`` it is returned as-is
      (external URL from SOTA API, YouTube, etc.).
    * Otherwise it is treated as a MinIO object name and expanded using
      ``MINIO_PUBLIC_ENDPOINT`` + ``MINIO_BUCKET``.

    Delegates to :func:`app.minio_client.get_public_url` for the actual
    URL construction to avoid duplicating the logic.
    """
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        return value
    from app.minio_client import get_public_url
    return get_public_url(value)


def to_object_name(url: str | None) -> str | None:
    """Extract MinIO object name from a full URL.

    If the URL contains ``/{bucket}/`` the part after that marker is returned.
    Otherwise the value is returned unchanged (external URL).
    """
    if not url:
        return None
    settings = get_settings()
    marker = f"/{settings.minio_bucket}/"
    idx = url.find(marker)
    if idx != -1:
        return url[idx + len(marker):]
    return url


# ---------------------------------------------------------------------------
# SQLAlchemy TypeDecorator — resolves URLs at DB-load level
# ---------------------------------------------------------------------------

class FileUrlType(TypeDecorator):
    """Column type that stores MinIO object names and resolves to full URLs on read.

    Use in SQLAlchemy models instead of ``Text`` / ``String`` for columns that
    hold MinIO object paths.  On SELECT the stored object name is expanded to a
    full public URL via ``resolve_file_url()``.  On INSERT/UPDATE the value is
    stored as-is (callers are responsible for passing object names).
    """

    impl = Text
    cache_ok = True

    def process_result_value(self, value, dialect):
        """DB → Python: expand object name to full URL."""
        return resolve_file_url(value)

    def process_bind_param(self, value, dialect):
        """Python → DB: strip full URL back to object name for safety."""
        return to_object_name(value) if value else value


# ---------------------------------------------------------------------------
# Legacy helper (kept for compatibility)
# ---------------------------------------------------------------------------

def get_file_data_with_url(file_doc: dict, base_url: str = "/api/v1") -> dict:
    """Convert file metadata to response dict with URL."""
    return {
        "id": file_doc.get("_id") or file_doc.get("object_name"),
        "filename": file_doc.get("filename"),
        "url": file_doc.get("url"),
        "size": file_doc.get("size"),
    }
