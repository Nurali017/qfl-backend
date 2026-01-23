"""add_country_model

Revision ID: h6c7d8e9f0g1
Revises: g5b6c7d8e9f0
Create Date: 2026-01-19 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'h6c7d8e9f0g1'
down_revision: Union[str, None] = 'g5b6c7d8e9f0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create countries table
    op.create_table(
        'countries',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('code', sa.String(length=2), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('name_kz', sa.String(length=100), nullable=True),
        sa.Column('name_en', sa.String(length=100), nullable=True),
        sa.Column('flag_url', sa.String(length=500), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('code', name='uq_countries_code')
    )
    op.create_index('ix_countries_code', 'countries', ['code'], unique=True)

    # 2. Add country_id to players
    op.add_column('players', sa.Column('country_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_players_country_id', 'players', 'countries',
        ['country_id'], ['id']
    )
    op.create_index('ix_players_country_id', 'players', ['country_id'])

    # 3. Add country_id to coaches
    op.add_column('coaches', sa.Column('country_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_coaches_country_id', 'coaches', 'countries',
        ['country_id'], ['id']
    )
    op.create_index('ix_coaches_country_id', 'coaches', ['country_id'])

    # 4. Add country_id to referees
    op.add_column('referees', sa.Column('country_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_referees_country_id', 'referees', 'countries',
        ['country_id'], ['id']
    )
    op.create_index('ix_referees_country_id', 'referees', ['country_id'])


def downgrade() -> None:
    # Remove from referees
    op.drop_index('ix_referees_country_id', table_name='referees')
    op.drop_constraint('fk_referees_country_id', 'referees', type_='foreignkey')
    op.drop_column('referees', 'country_id')

    # Remove from coaches
    op.drop_index('ix_coaches_country_id', table_name='coaches')
    op.drop_constraint('fk_coaches_country_id', 'coaches', type_='foreignkey')
    op.drop_column('coaches', 'country_id')

    # Remove from players
    op.drop_index('ix_players_country_id', table_name='players')
    op.drop_constraint('fk_players_country_id', 'players', type_='foreignkey')
    op.drop_column('players', 'country_id')

    # Drop countries table
    op.drop_index('ix_countries_code', table_name='countries')
    op.drop_table('countries')
