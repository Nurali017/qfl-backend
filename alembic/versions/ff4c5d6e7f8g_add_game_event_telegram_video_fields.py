"""Add game_events.telegram_message_id + telegram_video_sent_at.

Supports sending goal text immediately and attaching video as a reply when
the clip becomes available on MinIO.

Revision ID: ff4c5d6e7f8g
Revises: ff3b4c5d6e7f
Create Date: 2026-04-19
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "ff4c5d6e7f8g"
down_revision: Union[str, None] = "ff3b4c5d6e7f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "game_events",
        sa.Column("telegram_message_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "game_events",
        sa.Column("telegram_video_sent_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("game_events", "telegram_video_sent_at")
    op.drop_column("game_events", "telegram_message_id")
