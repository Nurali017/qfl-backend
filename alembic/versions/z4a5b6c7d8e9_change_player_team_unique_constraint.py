"""change player_team unique constraint: drop role from (player, team, season, role)

Revision ID: z4a5b6c7d8e9
Revises: z3a4b5c6d7e8
Create Date: 2026-02-27 12:00:00.000000
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "z4a5b6c7d8e9"
down_revision = "z3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_player_team_season_role", "player_teams", type_="unique")
    op.create_unique_constraint("uq_player_team_season", "player_teams", ["player_id", "team_id", "season_id"])


def downgrade() -> None:
    op.drop_constraint("uq_player_team_season", "player_teams", type_="unique")
    op.create_unique_constraint("uq_player_team_season_role", "player_teams", ["player_id", "team_id", "season_id", "role"])
