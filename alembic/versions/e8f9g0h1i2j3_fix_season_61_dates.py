"""Fix season 61 and other 2025 seasons with NULL dates

Season 61 (PL 2025) has NULL date_start and date_end, making it
"always active" in _pick_current_season(). This causes front-map
to return season 61 instead of 200 (PL 2026).

Set real dates for all 2025 seasons so they no longer match as active.

Revision ID: e8f9g0h1i2j3
Revises: d7e8f9g0h1i2
Create Date: 2026-02-23 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "e8f9g0h1i2j3"
down_revision = "d7e8f9g0h1i2"
branch_labels = None
depends_on = None

# 2025 seasons that may have NULL dates: (id, date_start, date_end)
SEASON_DATES = [
    (61, "2025-03-01", "2025-11-30"),   # PL 2025
    (85, "2025-03-01", "2025-11-30"),   # 1L 2025
    (71, "2025-05-01", "2025-11-30"),   # Cup 2025
    (80, "2025-03-01", "2025-11-30"),   # 2L 2025
    (84, "2025-03-01", "2025-11-30"),   # EL 2025
]


def upgrade() -> None:
    bind = op.get_bind()
    for season_id, start, end in SEASON_DATES:
        # Use inline date casts to avoid asyncpg type issues with string params
        bind.execute(
            sa.text(
                f"UPDATE seasons "
                f"SET date_start = '{start}'::date, date_end = '{end}'::date "
                f"WHERE id = :id AND (date_start IS NULL OR date_end IS NULL)"
            ),
            {"id": season_id},
        )


def downgrade() -> None:
    bind = op.get_bind()
    for season_id, _, _ in SEASON_DATES:
        bind.execute(
            sa.text(
                "UPDATE seasons "
                "SET date_start = NULL, date_end = NULL "
                "WHERE id = :id"
            ),
            {"id": season_id},
        )
