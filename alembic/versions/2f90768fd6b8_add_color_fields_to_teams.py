"""add_color_fields_to_teams

Revision ID: 2f90768fd6b8
Revises: c63260038e49
Create Date: 2026-01-23 16:03:49.888941

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2f90768fd6b8'
down_revision: Union[str, None] = 'c63260038e49'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('teams', sa.Column('primary_color', sa.String(length=7), nullable=True, comment='Hex color code (e.g., #FF5733)'))
    op.add_column('teams', sa.Column('secondary_color', sa.String(length=7), nullable=True, comment='Hex color code'))
    op.add_column('teams', sa.Column('accent_color', sa.String(length=7), nullable=True, comment='Hex color code'))
    op.add_column('teams', sa.Column('colors_updated_at', sa.DateTime(), nullable=True, comment='Last time colors were extracted'))


def downgrade() -> None:
    op.drop_column('teams', 'colors_updated_at')
    op.drop_column('teams', 'accent_color')
    op.drop_column('teams', 'secondary_color')
    op.drop_column('teams', 'primary_color')
