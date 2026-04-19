"""Add fields for public Telegram posts automation.

Revision ID: cc7d8e9f0a1b
Revises: bb1c2d3e4f5g
Create Date: 2026-04-18
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "cc7d8e9f0a1b"
down_revision: Union[str, None] = "bb1c2d3e4f5g"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "teams",
        sa.Column("tg_custom_emoji_id", sa.String(length=32), nullable=True),
    )

    op.add_column(
        "broadcasters",
        sa.Column("telegram_prefix", sa.String(length=8), nullable=True),
    )

    op.add_column(
        "games",
        sa.Column("announce_telegram_sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "games",
        sa.Column("start_telegram_sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "games",
        sa.Column("finish_telegram_sent_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.add_column(
        "game_events",
        sa.Column("telegram_sent_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("game_events", "telegram_sent_at")

    op.drop_column("games", "finish_telegram_sent_at")
    op.drop_column("games", "start_telegram_sent_at")
    op.drop_column("games", "announce_telegram_sent_at")

    op.drop_column("broadcasters", "telegram_prefix")

    op.drop_column("teams", "tg_custom_emoji_id")
