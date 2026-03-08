"""add score_table team_id index

Revision ID: n2v3w4x5y6z7
Revises: m1v2d3e4o5s6
Create Date: 2026-03-08 18:00:00.000000
"""

from alembic import op


revision = "n2v3w4x5y6z7"
down_revision = "m1v2d3e4o5s6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_score_table_team_id", "score_table", ["team_id"])


def downgrade() -> None:
    op.drop_index("ix_score_table_team_id", table_name="score_table")
