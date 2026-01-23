"""add article_type to news

Revision ID: 0a1a327a4a9e
Revises: 026302e4c8c9
Create Date: 2026-01-20 14:20:56.555511

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0a1a327a4a9e'
down_revision: Union[str, None] = '026302e4c8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enum type for article classification
    op.execute("CREATE TYPE article_type AS ENUM ('NEWS', 'ANALYTICS')")

    # Add article_type column to news table
    op.add_column('news',
        sa.Column('article_type',
                  postgresql.ENUM('NEWS', 'ANALYTICS', name='article_type', create_type=False),
                  nullable=True))

    # Create index for filtering performance
    op.create_index('ix_news_article_type', 'news', ['article_type'])


def downgrade() -> None:
    # Drop index
    op.drop_index('ix_news_article_type', table_name='news')

    # Drop article_type column
    op.drop_column('news', 'article_type')

    # Drop enum type
    op.execute('DROP TYPE IF EXISTS article_type')
