"""add source_id and source_url to news

Revision ID: o3j4k5l6m7n8
Revises: 05323ad34943
Create Date: 2026-01-20 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'o3j4k5l6m7n8'
down_revision: Union[str, None] = '05323ad34943'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add source_id column - original ID from kffleague.kz
    op.add_column('news',
        sa.Column('source_id', sa.Integer(), nullable=True))

    # Add source_url column - original URL from kffleague.kz
    op.add_column('news',
        sa.Column('source_url', sa.String(500), nullable=True))

    # Create index for deduplication checks
    op.create_index('ix_news_source_id', 'news', ['source_id'])

    # Create unique constraint on source_id + language to prevent duplicates
    op.create_unique_constraint(
        'uq_news_source_id_language',
        'news',
        ['source_id', 'language']
    )


def downgrade() -> None:
    # Drop unique constraint
    op.drop_constraint('uq_news_source_id_language', 'news', type_='unique')

    # Drop index
    op.drop_index('ix_news_source_id', table_name='news')

    # Drop columns
    op.drop_column('news', 'source_url')
    op.drop_column('news', 'source_id')
