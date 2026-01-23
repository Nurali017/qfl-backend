"""remove_player_country_name

Revision ID: i7d8e9f0g1h2
Revises: h6c7d8e9f0g1
Create Date: 2026-01-19 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'i7d8e9f0g1h2'
down_revision: Union[str, None] = 'h6c7d8e9f0g1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove deprecated country_name fields from players
    # Data is now stored in countries table via country_id FK
    op.drop_column('players', 'country_name')
    op.drop_column('players', 'country_name_kz')
    op.drop_column('players', 'country_name_en')


def downgrade() -> None:
    # Re-add country_name columns (data will be lost)
    op.add_column('players', sa.Column('country_name_en', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('country_name_kz', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('country_name', sa.String(length=100), nullable=True))
