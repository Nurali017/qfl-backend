"""add final_stage_ids to seasons

Revision ID: b4c5d6e7f8g9
Revises: a1b2c3d4e5f6
Create Date: 2026-02-22 23:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b4c5d6e7f8g9"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("seasons", sa.Column("final_stage_ids", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("seasons", "final_stage_ids")
