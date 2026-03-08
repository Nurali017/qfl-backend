"""add score_table season_id index

Revision ID: p3q4r5s6t7u8
Revises: n2v3w4x5y6z7
Create Date: 2026-03-09 12:00:00.000000
"""

from alembic import op


revision = "p3q4r5s6t7u8"
down_revision = "n2v3w4x5y6z7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_score_table_season_id", "score_table", ["season_id"])


def downgrade() -> None:
    op.drop_index("ix_score_table_season_id", table_name="score_table")
