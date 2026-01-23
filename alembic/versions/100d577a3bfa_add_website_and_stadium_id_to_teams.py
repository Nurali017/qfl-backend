"""add website and stadium_id to teams

Revision ID: 100d577a3bfa
Revises: 2c39bb3cf68c
Create Date: 2026-01-20 23:22:03.836245

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '100d577a3bfa'
down_revision: Union[str, None] = '2c39bb3cf68c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('teams', sa.Column('website', sa.String(length=500), nullable=True))
    op.add_column('teams', sa.Column('stadium_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_teams_stadium_id', 'teams', 'stadiums', ['stadium_id'], ['id'])


def downgrade() -> None:
    op.drop_constraint('fk_teams_stadium_id', 'teams', type_='foreignkey')
    op.drop_column('teams', 'stadium_id')
    op.drop_column('teams', 'website')
