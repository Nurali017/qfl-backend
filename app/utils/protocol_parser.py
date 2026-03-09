"""Extract attendance from match protocol PDFs stored in MinIO."""

import re
import logging

import fitz  # PyMuPDF
from minio import Minio

logger = logging.getLogger(__name__)


def extract_attendance_from_protocol(
    minio_client: Minio, bucket: str, object_name: str
) -> int | None:
    """Download protocol PDF from MinIO and extract attendance number.

    Looks for "Посещаемость: <number>" in the PDF text.
    Returns the number or None if not found.
    """
    obj = minio_client.get_object(bucket, object_name)
    try:
        data = obj.read()
    finally:
        obj.close()
        obj.release_conn()

    doc = fitz.open(stream=data, filetype="pdf")
    for page in doc:
        text = page.get_text()
        match = re.search(r"Посещаемость:\s*(\d+)", text)
        if match:
            doc.close()
            return int(match.group(1))
    doc.close()
    return None
