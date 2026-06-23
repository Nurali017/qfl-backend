"""Tests for compute_withdrawal_outcome — Second League reg. 2.16 threshold."""
import pytest

from app.services.regulations import compute_withdrawal_outcome


@pytest.mark.parametrize(
    "played,total,expected",
    [
        (9, 27, "annul"),             # ҚАРШЫҒА real case: 9 < 13.5
        (13, 27, "annul"),            # 13 < 13.5
        (14, 27, "award_remaining"),  # 14 > 13.5
        (13, 26, "award_remaining"),  # exactly half → award
        (12, 26, "annul"),            # 12 < 13
        (0, 27, "annul"),             # played nothing
        (27, 27, "award_remaining"),  # played everything
    ],
)
def test_compute_withdrawal_outcome(played, total, expected):
    assert compute_withdrawal_outcome(played, total) == expected


@pytest.mark.parametrize(
    "played,total",
    [
        (0, 0),    # total not positive
        (-1, 27),  # played below range
        (28, 27),  # played above total
    ],
)
def test_compute_withdrawal_outcome_invalid(played, total):
    with pytest.raises(ValueError):
        compute_withdrawal_outcome(played, total)
