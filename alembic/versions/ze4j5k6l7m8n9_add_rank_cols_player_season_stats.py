"""add rank columns to player_season_stats

Revision ID: ze4j5k6l7m8n9
Revises: zd3i4j5k6l7m8
Create Date: 2026-03-17 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "ze4j5k6l7m8n9"
down_revision: Union[str, None] = "zd3i4j5k6l7m8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "player_season_stats",
        sa.Column("goal_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "player_season_stats",
        sa.Column("goal_pass_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "player_season_stats",
        sa.Column("dry_match_rank", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("player_season_stats", "dry_match_rank")
    op.drop_column("player_season_stats", "goal_pass_rank")
    op.drop_column("player_season_stats", "goal_rank")
