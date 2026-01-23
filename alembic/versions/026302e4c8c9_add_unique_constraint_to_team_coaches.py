"""add_unique_constraint_to_team_coaches

Revision ID: 026302e4c8c9
Revises: l0g1h2i3j4k5
Create Date: 2026-01-19 15:17:23.130042

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '026302e4c8c9'
down_revision: Union[str, None] = 'l0g1h2i3j4k5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove duplicate team_coaches entries
    # Keep only the first entry for each (team_id, coach_id, season_id, role) combination
    op.execute("""
        DELETE FROM team_coaches
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM team_coaches
            GROUP BY team_id, coach_id, season_id, role
        )
    """)

    # Add unique constraint
    op.create_unique_constraint(
        'uq_team_coaches_team_coach_season_role',
        'team_coaches',
        ['team_id', 'coach_id', 'season_id', 'role']
    )


def downgrade() -> None:
    # Remove unique constraint
    op.drop_constraint('uq_team_coaches_team_coach_season_role', 'team_coaches', type_='unique')
