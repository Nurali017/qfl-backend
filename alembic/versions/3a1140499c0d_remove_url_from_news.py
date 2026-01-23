"""remove_url_from_news

Revision ID: 3a1140499c0d
Revises: 3a1140499c0c
Create Date: 2026-01-15 20:58:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '3a1140499c0d'
down_revision: Union[str, None] = '3a1140499c0c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column('news', 'url')


def downgrade() -> None:
    op.add_column('news', sa.Column('url', sa.String(500), nullable=True))
