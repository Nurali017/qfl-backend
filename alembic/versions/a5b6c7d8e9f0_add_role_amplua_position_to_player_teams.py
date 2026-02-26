"""add role, amplua, position fields to player_teams

Revision ID: a5b6c7d8e9f0
Revises: z3a4b5c6d7e8
Create Date: 2026-02-27 10:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a5b6c7d8e9f0"
down_revision = "b5c6d7e8f9g0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("player_teams", sa.Column("role", sa.Integer(), nullable=True, server_default="1"))
    op.add_column("player_teams", sa.Column("amplua", sa.Integer(), nullable=True))
    op.add_column("player_teams", sa.Column("position_ru", sa.String(200), nullable=True))
    op.add_column("player_teams", sa.Column("position_kz", sa.String(200), nullable=True))
    op.add_column("player_teams", sa.Column("position_en", sa.String(200), nullable=True))

    # Replace unique constraint: add role as part of the key
    op.drop_constraint("uq_player_team_season", "player_teams")
    op.create_unique_constraint(
        "uq_player_team_season_role",
        "player_teams",
        ["player_id", "team_id", "season_id", "role"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_player_team_season_role", "player_teams")
    op.create_unique_constraint(
        "uq_player_team_season", "player_teams", ["player_id", "team_id", "season_id"]
    )
    op.drop_column("player_teams", "position_en")
    op.drop_column("player_teams", "position_kz")
    op.drop_column("player_teams", "position_ru")
    op.drop_column("player_teams", "amplua")
    op.drop_column("player_teams", "role")
