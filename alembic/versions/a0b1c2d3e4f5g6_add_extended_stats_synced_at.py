"""add extended_stats_synced_at to games

Revision ID: a0b1c2d3e4f5g6
Revises: z9e0f1g2h3i4
Create Date: 2026-03-09 11:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a0b1c2d3e4f5g6"
down_revision: Union[str, None] = "z9e0f1g2h3i4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("extended_stats_synced_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "extended_stats_synced_at")
