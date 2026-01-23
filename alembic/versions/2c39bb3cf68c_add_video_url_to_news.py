"""add_video_url_to_news

Revision ID: 2c39bb3cf68c
Revises: o3j4k5l6m7n8
Create Date: 2026-01-20 22:54:18.577244

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '2c39bb3cf68c'
down_revision: Union[str, None] = 'o3j4k5l6m7n8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('news', sa.Column('video_url', sa.String(length=500), nullable=True))


def downgrade() -> None:
    op.drop_column('news', 'video_url')
