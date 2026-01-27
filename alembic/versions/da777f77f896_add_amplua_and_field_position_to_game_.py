"""Add amplua and field_position to game_lineup

Revision ID: da777f77f896
Revises: 2f90768fd6b8
Create Date: 2026-01-24 22:50:13.416077

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'da777f77f896'
down_revision: Union[str, None] = '2f90768fd6b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns for match-specific player positions from SOTA
    op.add_column('game_lineups', sa.Column('amplua', sa.String(length=10), nullable=True))
    op.add_column('game_lineups', sa.Column('field_position', sa.String(length=5), nullable=True))


def downgrade() -> None:
    op.drop_column('game_lineups', 'field_position')
    op.drop_column('game_lineups', 'amplua')
