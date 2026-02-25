"""Timezone-aware timestamp utilities."""

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime.

    Use instead of deprecated ``datetime.utcnow()`` which returns naive datetimes.
    """
    return datetime.now(timezone.utc)
