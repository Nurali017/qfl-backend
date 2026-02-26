"""replace cup draw sessions/pairs with simple cup_draws table

Revision ID: a4b5c6d7e8f9
Revises: z3a4b5c6d7e8
Create Date: 2026-02-26 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "a4b5c6d7e8f9"
down_revision = "z3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop old tables (pairs first due to FK)
    op.drop_index("ix_cup_draw_pairs_session_is_published", table_name="cup_draw_pairs")
    op.drop_index("ix_cup_draw_pairs_session_id", table_name="cup_draw_pairs")
    op.drop_table("cup_draw_pairs")

    op.drop_index("ix_cup_draw_sessions_season_id", table_name="cup_draw_sessions")
    op.drop_table("cup_draw_sessions")

    # Create new simplified table
    op.create_table(
        "cup_draws",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("round_key", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="draft"),
        sa.Column("pairs", sa.JSON(), nullable=True),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("admin_users.id"), nullable=True),
        sa.Column("published_by", sa.Integer(), sa.ForeignKey("admin_users.id"), nullable=True),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("season_id", "round_key", name="uq_cup_draw_season_round"),
    )
    op.create_index("ix_cup_draws_season_id", "cup_draws", ["season_id"])


def downgrade() -> None:
    op.drop_index("ix_cup_draws_season_id", table_name="cup_draws")
    op.drop_table("cup_draws")

    # Recreate old tables
    op.create_table(
        "cup_draw_sessions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("round_key", sa.String(length=20), nullable=False),
        sa.Column("is_live", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_finalized", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("required_pairs", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("admin_users.id"), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finalized_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_cup_draw_sessions_season_id", "cup_draw_sessions", ["season_id"])

    op.create_table(
        "cup_draw_pairs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("cup_draw_sessions.id"), nullable=False),
        sa.Column("reveal_order", sa.Integer(), nullable=False),
        sa.Column("team1_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("team2_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("home_team_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=True),
        sa.Column("leg_mode", sa.String(length=10), nullable=False),
        sa.Column("is_published", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("revealed_by", sa.Integer(), sa.ForeignKey("admin_users.id"), nullable=True),
        sa.Column("published_by", sa.Integer(), sa.ForeignKey("admin_users.id"), nullable=True),
        sa.Column("revealed_at", sa.DateTime(), nullable=True),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("generated_game_ids", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("session_id", "reveal_order", name="uq_cup_draw_pair_session_reveal_order"),
    )
    op.create_index("ix_cup_draw_pairs_session_id", "cup_draw_pairs", ["session_id"])
    op.create_index("ix_cup_draw_pairs_session_is_published", "cup_draw_pairs", ["session_id", "is_published"])
