"""add is_free_entry to games

Revision ID: z7c8d9e0f1g2
Revises: z6b7c8d9e0f1
Create Date: 2026-03-03 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "z7c8d9e0f1g2"
down_revision = "z6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "games",
        sa.Column("is_free_entry", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("games", "is_free_entry")
