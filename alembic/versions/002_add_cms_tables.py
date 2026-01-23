"""Add CMS tables (pages, news)

Revision ID: 002
Revises: 001
Create Date: 2025-01-14

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create language enum with uppercase values (matching SQLAlchemy enum names)
    op.execute("CREATE TYPE language AS ENUM ('KZ', 'RU')")

    # Pages table
    op.create_table(
        'pages',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('slug', sa.String(length=100), nullable=False),
        sa.Column('language', postgresql.ENUM('KZ', 'RU', name='language', create_type=False), nullable=False),
        sa.Column('title', sa.String(length=255), nullable=False),
        sa.Column('content', sa.Text(), nullable=True),
        sa.Column('content_text', sa.Text(), nullable=True),
        sa.Column('url', sa.String(length=500), nullable=True),
        sa.Column('structured_data', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        comment='Static pages content in multiple languages'
    )
    op.create_index('ix_pages_slug', 'pages', ['slug'])
    op.create_index('ix_pages_language', 'pages', ['language'])

    # News table
    op.create_table(
        'news',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('language', postgresql.ENUM('KZ', 'RU', name='language', create_type=False), nullable=False),
        sa.Column('title', sa.String(length=500), nullable=False),
        sa.Column('excerpt', sa.Text(), nullable=True),
        sa.Column('content', sa.Text(), nullable=True),
        sa.Column('content_text', sa.Text(), nullable=True),
        sa.Column('url', sa.String(length=500), nullable=True),
        sa.Column('image_url', sa.String(length=500), nullable=True),
        sa.Column('category', sa.String(length=100), nullable=True),
        sa.Column('publish_date', sa.Date(), nullable=True),
        sa.Column('structured_data', postgresql.JSONB(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'language'),
        comment='News articles in multiple languages'
    )
    op.create_index('ix_news_language', 'news', ['language'])
    op.create_index('ix_news_category', 'news', ['category'])
    op.create_index('ix_news_publish_date', 'news', ['publish_date'])


def downgrade() -> None:
    op.drop_index('ix_news_publish_date')
    op.drop_index('ix_news_category')
    op.drop_index('ix_news_language')
    op.drop_table('news')

    op.drop_index('ix_pages_language')
    op.drop_index('ix_pages_slug')
    op.drop_table('pages')

    # Drop enum type
    op.execute('DROP TYPE IF EXISTS language')
