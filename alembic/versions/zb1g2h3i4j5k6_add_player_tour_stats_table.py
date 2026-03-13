"""add player_tour_stats table

Revision ID: zb1g2h3i4j5k6
Revises: pr1a2b3c4d5e6
Create Date: 2026-03-12 18:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "zb1g2h3i4j5k6"
down_revision: Union[str, None] = "pr1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "player_tour_stats",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("player_id", sa.BigInteger(), nullable=False),
        sa.Column("season_id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=True),
        sa.Column("tour", sa.Integer(), nullable=False),
        sa.Column("games_played", sa.Integer(), nullable=True),
        sa.Column("time_on_field_total", sa.Integer(), nullable=True),
        sa.Column("goal", sa.Integer(), nullable=True),
        sa.Column("goal_pass", sa.Integer(), nullable=True),
        sa.Column("shot", sa.Integer(), nullable=True),
        sa.Column("passes", sa.Integer(), nullable=True),
        sa.Column("pass_ratio", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("xg", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("duel", sa.Integer(), nullable=True),
        sa.Column("tackle", sa.Integer(), nullable=True),
        sa.Column("yellow_cards", sa.Integer(), nullable=True),
        sa.Column("red_cards", sa.Integer(), nullable=True),
        sa.Column("extra_stats", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("player_id", "season_id", "tour", name="uq_player_tour_stats"),
    )
    op.create_index("ix_player_tour_stats_player_id", "player_tour_stats", ["player_id"])
    op.create_index("ix_player_tour_stats_season_id", "player_tour_stats", ["season_id"])
    op.create_index("ix_player_tour_stats_team_id", "player_tour_stats", ["team_id"])
    op.create_index("ix_player_tour_stats_season_tour", "player_tour_stats", ["season_id", "tour"])


def downgrade() -> None:
    op.drop_index("ix_player_tour_stats_season_tour", table_name="player_tour_stats")
    op.drop_index("ix_player_tour_stats_team_id", table_name="player_tour_stats")
    op.drop_index("ix_player_tour_stats_season_id", table_name="player_tour_stats")
    op.drop_index("ix_player_tour_stats_player_id", table_name="player_tour_stats")
    op.drop_table("player_tour_stats")
