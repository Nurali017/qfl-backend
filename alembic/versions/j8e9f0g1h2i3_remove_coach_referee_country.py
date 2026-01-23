"""remove_coach_referee_country

Revision ID: j8e9f0g1h2i3
Revises: i7d8e9f0g1h2
Create Date: 2026-01-19 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'j8e9f0g1h2i3'
down_revision: Union[str, None] = 'i7d8e9f0g1h2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove deprecated country column from coaches and referees
    # Data is now stored in countries table via country_id FK
    op.drop_column('coaches', 'country')
    op.drop_column('referees', 'country')


def downgrade() -> None:
    # Re-add country columns (data will be lost)
    op.add_column('referees', sa.Column('country', sa.String(length=100), nullable=True))
    op.add_column('coaches', sa.Column('country', sa.String(length=100), nullable=True))
