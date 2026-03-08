"""add media_videos table

Revision ID: m1v2d3e4o5s6
Revises: c0d1e2f3g4h5
Create Date: 2026-03-08 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "m1v2d3e4o5s6"
down_revision = "c0d1e2f3g4h5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "media_videos",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("youtube_id", sa.String(20), nullable=False),
        sa.Column("sort_order", sa.Integer, nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime),
        sa.Column("updated_at", sa.DateTime),
    )

    # Seed the 4 existing hardcoded videos
    op.execute(
        """
        INSERT INTO media_videos (title, youtube_id, sort_order, is_active, created_at, updated_at)
        VALUES
            ('Премьер-лиганың 26-турының үздік голдары', '-LxnCdR-pxI', 0, true, NOW(), NOW()),
            ('Премьер-лиганың 22-турының үздік голдары', 'HcY3luVxyzo', 1, true, NOW(), NOW()),
            ('Премьер-лиганың 21-турының үздік голдары', 'K_6ov7ERSuE', 2, true, NOW(), NOW()),
            ('Премьер-лиганың 20-турының үздік голдары', 'hSb0s6kj_JA', 3, true, NOW(), NOW())
        """
    )


def downgrade() -> None:
    op.drop_table("media_videos")
