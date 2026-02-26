"""add cup draw sessions and pairs tables

Revision ID: z3a4b5c6d7e8
Revises: y2z3a4b5c6d7
Create Date: 2026-02-25 19:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "z3a4b5c6d7e8"
down_revision = "y2z3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
    op.create_index(
        "ix_cup_draw_sessions_season_id",
        "cup_draw_sessions",
        ["season_id"],
    )

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
        sa.UniqueConstraint(
            "session_id",
            "reveal_order",
            name="uq_cup_draw_pair_session_reveal_order",
        ),
    )
    op.create_index(
        "ix_cup_draw_pairs_session_id",
        "cup_draw_pairs",
        ["session_id"],
    )
    op.create_index(
        "ix_cup_draw_pairs_session_is_published",
        "cup_draw_pairs",
        ["session_id", "is_published"],
    )


def downgrade() -> None:
    op.drop_index("ix_cup_draw_pairs_session_is_published", table_name="cup_draw_pairs")
    op.drop_index("ix_cup_draw_pairs_session_id", table_name="cup_draw_pairs")
    op.drop_table("cup_draw_pairs")

    op.drop_index("ix_cup_draw_sessions_season_id", table_name="cup_draw_sessions")
    op.drop_table("cup_draw_sessions")
