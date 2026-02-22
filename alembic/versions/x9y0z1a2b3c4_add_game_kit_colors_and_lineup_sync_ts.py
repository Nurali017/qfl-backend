"""add_game_kit_colors_and_lineup_sync_ts

Revision ID: x9y0z1a2b3c4
Revises: w8x9y0z1a2b3
Create Date: 2026-02-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "x9y0z1a2b3c4"
down_revision: Union[str, None] = "w8x9y0z1a2b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("home_kit_color", sa.String(length=10), nullable=True))
    op.add_column("games", sa.Column("away_kit_color", sa.String(length=10), nullable=True))
    op.add_column("games", sa.Column("lineup_live_synced_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "lineup_live_synced_at")
    op.drop_column("games", "away_kit_color")
    op.drop_column("games", "home_kit_color")
