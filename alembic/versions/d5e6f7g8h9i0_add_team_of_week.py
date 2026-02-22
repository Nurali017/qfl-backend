"""add_team_of_week

Revision ID: d5e6f7g8h9i0
Revises: c4d5e6f7g8h9
Create Date: 2026-02-22
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = "d5e6f7g8h9i0"
down_revision: Union[str, None] = "c4d5e6f7g8h9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "team_of_week",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("tour_key", sa.String(50), nullable=False),
        sa.Column("locale", sa.String(5), nullable=False, server_default="ru"),
        sa.Column("scheme", sa.String(20), nullable=True),
        sa.Column("payload", JSONB(), nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("season_id", "tour_key", "locale", name="uq_team_of_week_season_tour_locale"),
    )
    op.create_index("ix_team_of_week_season_id", "team_of_week", ["season_id"])


def downgrade() -> None:
    op.drop_index("ix_team_of_week_season_id", "team_of_week")
    op.drop_table("team_of_week")
