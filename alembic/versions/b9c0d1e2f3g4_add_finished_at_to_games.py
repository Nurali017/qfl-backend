"""add finished_at to games

Revision ID: b9c0d1e2f3g4
Revises: a8b9c0d1e2f3
Create Date: 2026-03-06 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "b9c0d1e2f3g4"
down_revision = "a8b9c0d1e2f3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("games", sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE games
            SET finished_at = COALESCE(finished_at, updated_at)
            WHERE status IN ('finished', 'technical_defeat')
              AND finished_at IS NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_column("games", "finished_at")
