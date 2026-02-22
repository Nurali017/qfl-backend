"""add_lineup_render_metadata_to_games

Revision ID: c4d5e6f7g8h9
Revises: b3c4d5e6f7g8
Create Date: 2026-02-22
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4d5e6f7g8h9"
down_revision: Union[str, None] = "b3c4d5e6f7g8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("lineup_source", sa.String(length=32), nullable=True))
    op.add_column("games", sa.Column("lineup_render_mode", sa.String(length=16), nullable=True))
    op.add_column("games", sa.Column("lineup_backfilled_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "lineup_backfilled_at")
    op.drop_column("games", "lineup_render_mode")
    op.drop_column("games", "lineup_source")

