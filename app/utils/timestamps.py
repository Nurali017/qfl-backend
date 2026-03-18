"""Timestamp utilities for canonical UTC storage and comparisons."""

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
ALMATY_TZ = ZoneInfo("Asia/Almaty")


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(value: datetime | None) -> datetime | None:
    """Normalize possibly naive datetimes to timezone-aware UTC.

    SQLite tests still round-trip naive values even for timezone-aware columns.
    During the production migration window some environments may also contain
    legacy naive UTC rows. We treat all naive values as UTC to keep the
    application contract stable while the database is normalized.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def combine_almaty_local_to_utc(
    local_date: date,
    local_time: time,
) -> datetime:
    """Compose Asia/Almaty schedule fields into a UTC datetime."""
    return datetime.combine(
        local_date,
        local_time,
        tzinfo=ALMATY_TZ,
    ).astimezone(UTC)


def to_almaty(value: datetime | None) -> datetime | None:
    """Convert a stored UTC datetime into Asia/Almaty."""
    normalized = ensure_utc(value)
    if normalized is None:
        return None
    return normalized.astimezone(ALMATY_TZ)
