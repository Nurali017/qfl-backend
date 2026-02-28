"""add half1_started_at and half2_started_at to games

Revision ID: z5a6b7c8d9e0
Revises: z4a5b6c7d8e9
Create Date: 2026-02-28 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "z5a6b7c8d9e0"
down_revision = "z4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("games", sa.Column("half1_started_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("games", sa.Column("half2_started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "half2_started_at")
    op.drop_column("games", "half1_started_at")
