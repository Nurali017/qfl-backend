"""add tour_sync_status table

Revision ID: zk7m8n9o0p1q2
Revises: zj6l7m8n9o0p1
Create Date: 2026-03-18 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "zk7m8n9o0p1q2"
down_revision = "zj6l7m8n9o0p1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tour_sync_status",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("tour", sa.Integer(), nullable=False),
        sa.Column("synced_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("season_id", "tour", name="uq_tour_sync_status"),
    )
    op.create_index("ix_tour_sync_status_season", "tour_sync_status", ["season_id"])


def downgrade() -> None:
    op.drop_index("ix_tour_sync_status_season", table_name="tour_sync_status")
    op.drop_table("tour_sync_status")
