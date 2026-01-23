"""add_games_indexes_for_match_center

Revision ID: n2i3j4k5l6m7
Revises: m1h2i3j4k5l6
Create Date: 2026-01-20 12:15:00.000000

Adds database indexes to games table for efficient Match Center filtering:
- idx_games_season_date: for season + date filtering
- idx_games_season_tour: for season + tour filtering
- idx_games_date_status: for date range and status filtering
- idx_games_is_live: for live match filtering
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'n2i3j4k5l6m7'
down_revision: Union[str, None] = 'm1h2i3j4k5l6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Index for season + date filtering (most common query)
    op.create_index(
        'idx_games_season_date',
        'games',
        ['season_id', 'date'],
        unique=False
    )

    # Index for season + tour filtering
    op.create_index(
        'idx_games_season_tour',
        'games',
        ['season_id', 'tour'],
        unique=False
    )

    # Index for date range and status filtering
    op.create_index(
        'idx_games_date_status',
        'games',
        ['date', 'home_score'],
        unique=False
    )

    # Partial index for live matches (only indexes rows where is_live = true)
    op.create_index(
        'idx_games_is_live',
        'games',
        ['is_live'],
        unique=False,
        postgresql_where=sa.text('is_live = true')
    )


def downgrade() -> None:
    op.drop_index('idx_games_is_live', table_name='games')
    op.drop_index('idx_games_date_status', table_name='games')
    op.drop_index('idx_games_season_tour', table_name='games')
    op.drop_index('idx_games_season_date', table_name='games')
