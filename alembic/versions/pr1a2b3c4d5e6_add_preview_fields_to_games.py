"""add AI preview fields to games

Revision ID: pr1a2b3c4d5e6
Revises: za0f1g2h3i4j5
Create Date: 2026-03-12 18:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = "pr1a2b3c4d5e6"
down_revision = "za0f1g2h3i4j5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("games", sa.Column("preview_ru", sa.Text(), nullable=True))
    op.add_column("games", sa.Column("preview_kz", sa.Text(), nullable=True))
    op.add_column("games", sa.Column("preview_generated_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "preview_generated_at")
    op.drop_column("games", "preview_kz")
    op.drop_column("games", "preview_ru")
