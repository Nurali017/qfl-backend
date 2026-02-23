"""Backfill total/current rounds for 1L and EL seasons from games.

Revision ID: w3x4y5z6a7b8
Revises: u3v4w5x6y7z8
Create Date: 2026-02-24 01:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "w3x4y5z6a7b8"
down_revision = "u3v4w5x6y7z8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()

    # Fill total_rounds from max scheduled tour where it is currently missing.
    bind.execute(
        sa.text(
            """
            WITH season_max_tour AS (
                SELECT s.id AS season_id, MAX(g.tour) AS max_tour
                FROM seasons s
                LEFT JOIN games g
                    ON g.season_id = s.id
                   AND g.tour IS NOT NULL
                WHERE s.frontend_code IN ('1l', 'el')
                GROUP BY s.id
            )
            UPDATE seasons s
               SET total_rounds = smt.max_tour,
                   updated_at = NOW()
              FROM season_max_tour smt
             WHERE s.id = smt.season_id
               AND s.total_rounds IS NULL
               AND smt.max_tour IS NOT NULL
            """
        )
    )

    # Fill current_round from max played tour where it is currently missing.
    bind.execute(
        sa.text(
            """
            WITH season_played_tour AS (
                SELECT s.id AS season_id, MAX(g.tour) AS max_played_tour
                FROM seasons s
                LEFT JOIN games g
                    ON g.season_id = s.id
                   AND g.tour IS NOT NULL
                   AND g.home_score IS NOT NULL
                   AND g.away_score IS NOT NULL
                WHERE s.frontend_code IN ('1l', 'el')
                GROUP BY s.id
            )
            UPDATE seasons s
               SET current_round = spt.max_played_tour,
                   updated_at = NOW()
              FROM season_played_tour spt
             WHERE s.id = spt.season_id
               AND s.current_round IS NULL
               AND spt.max_played_tour IS NOT NULL
            """
        )
    )


def downgrade() -> None:
    # no-op: data backfill is intentionally non-reversible
    pass

