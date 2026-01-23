"""Add unique index on pages (slug, language)

Revision ID: 005
Revises: 004
Create Date: 2025-01-15

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '005'
down_revision: Union[str, None] = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index('ix_pages_slug_language', 'pages', ['slug', 'language'], unique=True)


def downgrade() -> None:
    op.drop_index('ix_pages_slug_language')
