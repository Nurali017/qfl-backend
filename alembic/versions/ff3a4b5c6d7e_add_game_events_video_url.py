"""Add game_events.video_url for goal highlight clips.

Stores MinIO object name (resolved to full URL on read via FileUrlType).
Populated by goal_video_sync_service from Google Drive during live matches.

Revision ID: ff3a4b5c6d7e
Revises: ee2f3a4b5c6d
Create Date: 2026-04-19
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "ff3a4b5c6d7e"
down_revision: Union[str, None] = "ee2f3a4b5c6d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "game_events",
        sa.Column("video_url", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("game_events", "video_url")
