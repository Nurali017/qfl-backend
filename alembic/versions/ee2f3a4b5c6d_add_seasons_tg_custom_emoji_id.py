"""Add seasons.tg_custom_emoji_id for league logo in Telegram posts.

Revision ID: ee2f3a4b5c6d
Revises: dd1e2f3a4b5c
Create Date: 2026-04-18
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "ee2f3a4b5c6d"
down_revision: Union[str, None] = "dd1e2f3a4b5c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "seasons",
        sa.Column("tg_custom_emoji_id", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("seasons", "tg_custom_emoji_id")
