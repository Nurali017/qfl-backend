"""add_slider_fields_to_news

Revision ID: 3a1140499c0c
Revises: 005
Create Date: 2026-01-15 14:14:17.334003

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '3a1140499c0c'
down_revision: Union[str, None] = '005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('news', sa.Column('is_slider', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('news', sa.Column('slider_order', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('news', 'slider_order')
    op.drop_column('news', 'is_slider')
