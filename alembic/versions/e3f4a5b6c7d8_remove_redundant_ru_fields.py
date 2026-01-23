"""remove_redundant_ru_fields

Revision ID: e3f4a5b6c7d8
Revises: d2e3f4a5b6c7
Create Date: 2026-01-19 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e3f4a5b6c7d8'
down_revision: Union[str, None] = 'd2e3f4a5b6c7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove redundant _ru fields (name field already contains Russian as default)

    # Teams
    op.drop_column('teams', 'name_ru')
    op.drop_column('teams', 'city_ru')

    # Players
    op.drop_column('players', 'first_name_ru')
    op.drop_column('players', 'last_name_ru')
    op.drop_column('players', 'country_name_ru')
    op.drop_column('players', 'top_role_ru')

    # Tournaments
    op.drop_column('tournaments', 'name_ru')
    op.drop_column('tournaments', 'country_name_ru')

    # Seasons
    op.drop_column('seasons', 'name_ru')


def downgrade() -> None:
    # Seasons
    op.add_column('seasons', sa.Column('name_ru', sa.String(length=100), nullable=True))

    # Tournaments
    op.add_column('tournaments', sa.Column('country_name_ru', sa.String(length=100), nullable=True))
    op.add_column('tournaments', sa.Column('name_ru', sa.String(length=255), nullable=True))

    # Players
    op.add_column('players', sa.Column('top_role_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('country_name_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('last_name_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('first_name_ru', sa.String(length=100), nullable=True))

    # Teams
    op.add_column('teams', sa.Column('city_ru', sa.String(length=100), nullable=True))
    op.add_column('teams', sa.Column('name_ru', sa.String(length=255), nullable=True))
