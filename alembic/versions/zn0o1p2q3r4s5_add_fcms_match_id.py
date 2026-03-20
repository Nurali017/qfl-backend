"""add fcms_match_id, fcms_protocol_synced_at to games and fcms_team_id to teams

Revision ID: zn0o1p2q3r4s5
Revises: zm9n0o1p2q3r4
Create Date: 2026-03-20 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "zn0o1p2q3r4s5"
down_revision: Union[str, None] = "zm9n0o1p2q3r4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("fcms_match_id", sa.Integer(), nullable=True))
    op.add_column("games", sa.Column("fcms_protocol_synced_at", sa.DateTime(timezone=True), nullable=True))
    op.create_unique_constraint("uq_games_fcms_match_id", "games", ["fcms_match_id"])
    op.create_index("ix_games_fcms_match_id", "games", ["fcms_match_id"])
    op.add_column("teams", sa.Column("fcms_team_id", sa.Integer(), nullable=True))
    op.create_unique_constraint("uq_teams_fcms_team_id", "teams", ["fcms_team_id"])
    op.create_index("ix_teams_fcms_team_id", "teams", ["fcms_team_id"])
    op.add_column("seasons", sa.Column("fcms_group_id", sa.Integer(), nullable=True))
    op.create_unique_constraint("uq_seasons_fcms_group_id", "seasons", ["fcms_group_id"])
    op.create_index("ix_seasons_fcms_group_id", "seasons", ["fcms_group_id"])
    # Seed FCMS group IDs for 2026 seasons
    op.execute(sa.text("UPDATE seasons SET fcms_group_id = 10733 WHERE id = 200"))  # Премьер-Лига 2026
    op.execute(sa.text("UPDATE seasons SET fcms_group_id = 10688 WHERE id = 201"))  # Суперкубок 2026
    op.execute(sa.text("UPDATE seasons SET fcms_group_id = 11081 WHERE id = 203"))  # Вторая Лига 2026
    op.execute(sa.text("UPDATE seasons SET fcms_group_id = 11036 WHERE id = 204"))  # Первая Лига 2026


def downgrade() -> None:
    op.drop_index("ix_seasons_fcms_group_id", table_name="seasons")
    op.drop_constraint("uq_seasons_fcms_group_id", "seasons", type_="unique")
    op.drop_column("seasons", "fcms_group_id")
    op.drop_index("ix_teams_fcms_team_id", table_name="teams")
    op.drop_constraint("uq_teams_fcms_team_id", "teams", type_="unique")
    op.drop_column("teams", "fcms_team_id")
    op.drop_index("ix_games_fcms_match_id", table_name="games")
    op.drop_constraint("uq_games_fcms_match_id", "games", type_="unique")
    op.drop_column("games", "fcms_protocol_synced_at")
    op.drop_column("games", "fcms_match_id")
