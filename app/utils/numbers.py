import math
from decimal import Decimal, InvalidOperation
from typing import Any


def to_finite_float(value: object) -> float | None:
    """Convert numeric-like values to finite float; return None for NaN/inf/invalid."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, InvalidOperation):
        return None
    return number if math.isfinite(number) else None


def sanitize_non_finite_numbers(value: Any) -> Any:
    """
    Recursively replace non-finite numeric values in dict/list payloads with None.

    This avoids JSON serialization crashes like:
    "Out of range float values are not JSON compliant".
    """
    if isinstance(value, dict):
        return {k: sanitize_non_finite_numbers(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_non_finite_numbers(v) for v in value]
    if isinstance(value, tuple):
        return tuple(sanitize_non_finite_numbers(v) for v in value)
    if isinstance(value, Decimal):
        return to_finite_float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value
