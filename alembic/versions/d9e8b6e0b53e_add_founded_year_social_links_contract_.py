"""add_founded_year_social_links_contract_end

Revision ID: d9e8b6e0b53e
Revises: zv8w9x0y1z2a3
Create Date: 2026-04-09 22:32:31.646498

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd9e8b6e0b53e'
down_revision: Union[str, None] = 'zv8w9x0y1z2a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('clubs', sa.Column('founded_year', sa.Integer(), nullable=True))
    op.add_column('clubs', sa.Column('social_links', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('player_teams', sa.Column('contract_end_date', sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column('player_teams', 'contract_end_date')
    op.drop_column('clubs', 'social_links')
    op.drop_column('clubs', 'founded_year')
