"""File URL utilities."""

from __future__ import annotations

from typing import Annotated

from pydantic import BeforeValidator

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
    """
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        return value
    settings = get_settings()
    endpoint = settings.minio_public_endpoint
    bucket = settings.minio_bucket
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return f"{endpoint}/{bucket}/{value}"
    return f"https://{endpoint}/{bucket}/{value}"


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
# Pydantic annotated type
# ---------------------------------------------------------------------------

FileUrl = Annotated[str | None, BeforeValidator(resolve_file_url)]
"""Use as a field type in Pydantic *response* schemas.

Any stored object name (e.g. ``player_photos/abc.webp``) is automatically
expanded to a full public URL at serialisation time.  External URLs pass
through unchanged.
"""


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
