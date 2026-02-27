"""drop player age column — computed from birthday

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-27 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "b2c3d4e5f6g7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("players", "age")


def downgrade() -> None:
    op.add_column(
        "players",
        sa.Column("age", sa.Integer(), nullable=True),
    )
    # Backfill age from birthday
    op.execute("""
        UPDATE players
        SET age = EXTRACT(YEAR FROM age(birthday))::int
        WHERE birthday IS NOT NULL
    """)
