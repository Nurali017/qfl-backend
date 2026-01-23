"""File URL utilities."""


def get_file_data_with_url(file_doc: dict, base_url: str = "/api/v1") -> dict:
    """
    Convert file metadata to response dict with URL.

    Args:
        file_doc: File metadata from MinIO
        base_url: Base URL for the API (unused, kept for compatibility)

    Returns:
        Dict with id, filename, url, and optional size
    """
    return {
        "id": file_doc.get("_id") or file_doc.get("object_name"),
        "filename": file_doc.get("filename"),
        "url": file_doc.get("url"),
        "size": file_doc.get("size"),
    }
