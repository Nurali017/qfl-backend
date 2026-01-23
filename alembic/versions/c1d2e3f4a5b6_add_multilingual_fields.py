"""add_multilingual_fields

Revision ID: c1d2e3f4a5b6
Revises: b9c964b6f5d2
Create Date: 2026-01-18 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c1d2e3f4a5b6'
down_revision: Union[str, None] = 'b9c964b6f5d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Teams - add kz, ru fields (en already exists)
    op.add_column('teams', sa.Column('name_kz', sa.String(length=255), nullable=True))
    op.add_column('teams', sa.Column('name_ru', sa.String(length=255), nullable=True))
    op.add_column('teams', sa.Column('city_kz', sa.String(length=100), nullable=True))
    op.add_column('teams', sa.Column('city_ru', sa.String(length=100), nullable=True))

    # Players - add kz, ru, en fields
    op.add_column('players', sa.Column('first_name_kz', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('first_name_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('first_name_en', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('last_name_kz', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('last_name_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('last_name_en', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('country_name_kz', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('country_name_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('country_name_en', sa.String(length=100), nullable=True))

    # Tournaments - add kz, ru, en fields
    op.add_column('tournaments', sa.Column('name_kz', sa.String(length=255), nullable=True))
    op.add_column('tournaments', sa.Column('name_ru', sa.String(length=255), nullable=True))
    op.add_column('tournaments', sa.Column('name_en', sa.String(length=255), nullable=True))
    op.add_column('tournaments', sa.Column('country_name_kz', sa.String(length=100), nullable=True))
    op.add_column('tournaments', sa.Column('country_name_ru', sa.String(length=100), nullable=True))
    op.add_column('tournaments', sa.Column('country_name_en', sa.String(length=100), nullable=True))

    # Seasons - add kz, ru, en fields
    op.add_column('seasons', sa.Column('name_kz', sa.String(length=100), nullable=True))
    op.add_column('seasons', sa.Column('name_ru', sa.String(length=100), nullable=True))
    op.add_column('seasons', sa.Column('name_en', sa.String(length=100), nullable=True))

    # Stadiums - add ru field (kz, en already exist)
    op.add_column('stadiums', sa.Column('name_ru', sa.String(length=255), nullable=True))
    op.add_column('stadiums', sa.Column('city_ru', sa.String(length=100), nullable=True))

    # Referees - add ru field (kz, en already exist)
    op.add_column('referees', sa.Column('first_name_ru', sa.String(length=100), nullable=True))
    op.add_column('referees', sa.Column('last_name_ru', sa.String(length=100), nullable=True))

    # Coaches - add ru field (kz, en already exist)
    op.add_column('coaches', sa.Column('first_name_ru', sa.String(length=100), nullable=True))
    op.add_column('coaches', sa.Column('last_name_ru', sa.String(length=100), nullable=True))


def downgrade() -> None:
    # Coaches
    op.drop_column('coaches', 'last_name_ru')
    op.drop_column('coaches', 'first_name_ru')

    # Referees
    op.drop_column('referees', 'last_name_ru')
    op.drop_column('referees', 'first_name_ru')

    # Stadiums
    op.drop_column('stadiums', 'city_ru')
    op.drop_column('stadiums', 'name_ru')

    # Seasons
    op.drop_column('seasons', 'name_en')
    op.drop_column('seasons', 'name_ru')
    op.drop_column('seasons', 'name_kz')

    # Tournaments
    op.drop_column('tournaments', 'country_name_en')
    op.drop_column('tournaments', 'country_name_ru')
    op.drop_column('tournaments', 'country_name_kz')
    op.drop_column('tournaments', 'name_en')
    op.drop_column('tournaments', 'name_ru')
    op.drop_column('tournaments', 'name_kz')

    # Players
    op.drop_column('players', 'country_name_en')
    op.drop_column('players', 'country_name_ru')
    op.drop_column('players', 'country_name_kz')
    op.drop_column('players', 'last_name_en')
    op.drop_column('players', 'last_name_ru')
    op.drop_column('players', 'last_name_kz')
    op.drop_column('players', 'first_name_en')
    op.drop_column('players', 'first_name_ru')
    op.drop_column('players', 'first_name_kz')

    # Teams
    op.drop_column('teams', 'city_ru')
    op.drop_column('teams', 'city_kz')
    op.drop_column('teams', 'name_ru')
    op.drop_column('teams', 'name_kz')
