"""add fcms_roster_sync_logs table

Revision ID: zo1p2q3r4s5t6
Revises: zn0o1p2q3r4s5
Create Date: 2026-03-20 18:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = "zo1p2q3r4s5t6"
down_revision: Union[str, None] = "zn0o1p2q3r4s5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "fcms_roster_sync_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("competition_name", sa.String(200), nullable=False),
        sa.Column("competition_id", sa.Integer(), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="running"),
        sa.Column("teams_synced", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_auto_updates", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_new_players", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_auto_deactivated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_deregistered", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("results", JSONB(), nullable=True),
        sa.Column("resolved_items", JSONB(), nullable=False, server_default="{}"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("triggered_by", sa.String(100), nullable=False, server_default="celery_beat"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_fcms_roster_sync_logs_season_id", "fcms_roster_sync_logs", ["season_id"])
    op.create_index("ix_fcms_roster_sync_logs_started_at", "fcms_roster_sync_logs", ["started_at"])

    op.add_column("players", sa.Column("fcms_person_id", sa.Integer(), nullable=True))
    op.create_index("ix_players_fcms_person_id", "players", ["fcms_person_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_players_fcms_person_id", table_name="players")
    op.drop_column("players", "fcms_person_id")

    op.drop_index("ix_fcms_roster_sync_logs_started_at", table_name="fcms_roster_sync_logs")
    op.drop_index("ix_fcms_roster_sync_logs_season_id", table_name="fcms_roster_sync_logs")
    op.drop_table("fcms_roster_sync_logs")
