"""Add missing FK index on game_broadcasters.broadcaster_id.

Revision ID: zx0y1z2a3b4c5
Revises: zw9x0y1z2a3b4
Create Date: 2026-04-12

game_broadcasters had 770K seq scans (no index on broadcaster_id FK).
"""
from typing import Sequence, Union

from alembic import op

revision: str = "zx0y1z2a3b4c5"
down_revision: Union[str, None] = "zw9x0y1z2a3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_game_broadcasters_broadcaster_id", "game_broadcasters", ["broadcaster_id"])


def downgrade() -> None:
    op.drop_index("ix_game_broadcasters_broadcaster_id", table_name="game_broadcasters")
