"""Regulation decision helpers (KFF tournament rules).

Pure, side-effect-free functions that encode regulation thresholds so the
logic can be unit-tested in isolation from any DB or HTTP code path.
"""
from typing import Literal

WithdrawalOutcome = Literal["annul", "award_remaining"]


def compute_withdrawal_outcome(played: int, total: int) -> WithdrawalOutcome:
    """Decide how a withdrawn team's results are handled (Second League reg. 2.16).

    Regulation 2.16: if a club excluded from the championship played FEWER
    than half of its matches, all of its results are annulled. If it played
    HALF OR MORE of its matches, it is credited with defeats in the remaining
    matches and the opposing teams are awarded wins (without changing the
    goal difference).

    The "half or more" boundary is inclusive on the award side, so a team that
    played exactly half of its matches falls into ``award_remaining``.

    Args:
        played: matches the team actually played with a result.
        total: total matches scheduled for the team in the season.

    Returns:
        ``"annul"`` when ``played < total / 2``; otherwise ``"award_remaining"``.

    Raises:
        ValueError: if ``total <= 0`` or ``played`` is outside ``[0, total]``.
    """
    if total <= 0:
        raise ValueError(f"total must be positive, got {total}")
    if played < 0 or played > total:
        raise ValueError(f"played must be within [0, {total}], got {played}")

    # Integer-only comparison avoids float rounding: played < total/2  ⇔  2*played < total.
    if 2 * played < total:
        return "annul"
    return "award_remaining"
