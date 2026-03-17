"""Timestamp utilities aligned with DB naive UTC columns."""

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Return current UTC time as naive datetime for DB compatibility."""
    return datetime.utcnow()


def ensure_naive_utc(value: datetime | None) -> datetime | None:
    """Normalize datetimes to naive UTC for DB comparisons and Celery ETA.

    Production currently mixes naive DB columns with some timezone-aware
    values coming back from SQLAlchemy/driver paths. Converting everything
    here keeps comparisons deterministic without changing model semantics.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
