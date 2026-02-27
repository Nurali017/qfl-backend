"""drop is_live and is_technical columns from games

Migrate data first: ensure status column is authoritative,
then drop the redundant boolean columns.

Revision ID: aa1b2c3d4e5f
Revises: z3a4b5c6d7e8
Create Date: 2026-02-26 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "aa1b2c3d4e5f"
down_revision = "z3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Backfill status from legacy booleans for any rows still in "created"
    op.execute("""
        UPDATE games SET status = 'live'
        WHERE is_live = true AND status = 'created'
    """)
    op.execute("""
        UPDATE games SET status = 'technical_defeat'
        WHERE is_technical = true AND status = 'created'
    """)
    op.execute("""
        UPDATE games SET status = 'finished'
        WHERE status = 'created'
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL
    """)

    # 2. Drop the redundant columns
    op.drop_column("games", "is_live")
    op.drop_column("games", "is_technical")


def downgrade() -> None:
    # Re-add columns with defaults
    op.add_column(
        "games",
        sa.Column("is_live", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.add_column(
        "games",
        sa.Column("is_technical", sa.Boolean(), nullable=False, server_default="false"),
    )

    # Backfill from status
    op.execute("""
        UPDATE games SET is_live = true WHERE status = 'live'
    """)
    op.execute("""
        UPDATE games SET is_technical = true WHERE status = 'technical_defeat'
    """)
