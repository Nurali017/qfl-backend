"""Timestamp utilities aligned with DB naive UTC columns."""

from datetime import datetime


def utcnow() -> datetime:
    """Return current UTC time as naive datetime for DB compatibility."""
    return datetime.utcnow()
