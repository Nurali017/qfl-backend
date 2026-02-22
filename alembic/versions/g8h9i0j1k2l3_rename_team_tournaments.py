"""rename_team_tournaments_to_season_participants

Revision ID: g8h9i0j1k2l3
Revises: f7g8h9i0j1k2
Create Date: 2026-02-22
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "g8h9i0j1k2l3"
down_revision: Union[str, None] = "f7g8h9i0j1k2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("team_tournaments", "season_participants")
    op.execute(
        "ALTER TABLE season_participants "
        "RENAME CONSTRAINT uq_team_tournament_season TO uq_season_participant_season"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE season_participants "
        "RENAME CONSTRAINT uq_season_participant_season TO uq_team_tournament_season"
    )
    op.rename_table("season_participants", "team_tournaments")
