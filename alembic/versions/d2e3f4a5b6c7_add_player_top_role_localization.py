"""add_player_top_role_localization

Revision ID: d2e3f4a5b6c7
Revises: c1d2e3f4a5b6
Create Date: 2026-01-19 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd2e3f4a5b6c7'
down_revision: Union[str, None] = 'c1d2e3f4a5b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Players - add top_role localization fields
    op.add_column('players', sa.Column('top_role_kz', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('top_role_ru', sa.String(length=100), nullable=True))
    op.add_column('players', sa.Column('top_role_en', sa.String(length=100), nullable=True))


def downgrade() -> None:
    op.drop_column('players', 'top_role_en')
    op.drop_column('players', 'top_role_ru')
    op.drop_column('players', 'top_role_kz')
