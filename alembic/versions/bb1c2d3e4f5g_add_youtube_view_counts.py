"""Add YouTube view_count tracking columns to games and media_videos.

Revision ID: bb1c2d3e4f5g
Revises: aa9b8c7d6e5f
Create Date: 2026-04-17
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "bb1c2d3e4f5g"
down_revision: Union[str, None] = "aa9b8c7d6e5f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("youtube_live_view_count", sa.Integer(), nullable=True))
    op.add_column("games", sa.Column("video_review_view_count", sa.Integer(), nullable=True))
    op.add_column("games", sa.Column("youtube_stats_updated_at", sa.DateTime(timezone=True), nullable=True))

    op.add_column("media_videos", sa.Column("view_count", sa.Integer(), nullable=True))
    op.add_column("media_videos", sa.Column("stats_updated_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("media_videos", "stats_updated_at")
    op.drop_column("media_videos", "view_count")

    op.drop_column("games", "youtube_stats_updated_at")
    op.drop_column("games", "video_review_view_count")
    op.drop_column("games", "youtube_live_view_count")
