"""Add games.lineup_telegram_message_id for Telegram reply threading.

Revision ID: ff3b4c5d6e7f
Revises: ff3a4b5c6d7e
Create Date: 2026-04-18
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "ff3b4c5d6e7f"
down_revision: Union[str, None] = "ff3a4b5c6d7e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "games",
        sa.Column("lineup_telegram_message_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("games", "lineup_telegram_message_id")
