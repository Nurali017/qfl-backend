"""Add telegram_daily_result_posts dedupe table.

Revision ID: gh6i7j8k9l0m
Revises: ff4c5d6e7f8g
Create Date: 2026-04-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "gh6i7j8k9l0m"
down_revision: Union[str, None] = "ff4c5d6e7f8g"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "telegram_daily_result_posts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("for_date", sa.Date(), nullable=False),
        sa.Column("locale", sa.String(length=8), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=True),
        sa.Column("tour", sa.Integer(), nullable=True),
        sa.Column("game_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "season_id",
            "for_date",
            "locale",
            name="uq_telegram_daily_result_posts_scope",
        ),
    )
    op.create_index(
        "ix_telegram_daily_result_posts_season_id",
        "telegram_daily_result_posts",
        ["season_id"],
    )
    op.create_index(
        "ix_telegram_daily_result_posts_for_date",
        "telegram_daily_result_posts",
        ["for_date"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_telegram_daily_result_posts_for_date",
        table_name="telegram_daily_result_posts",
    )
    op.drop_index(
        "ix_telegram_daily_result_posts_season_id",
        table_name="telegram_daily_result_posts",
    )
    op.drop_table("telegram_daily_result_posts")
