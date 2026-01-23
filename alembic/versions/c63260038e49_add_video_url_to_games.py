"""add_video_url_to_games

Revision ID: c63260038e49
Revises: 100d577a3bfa
Create Date: 2026-01-23 16:02:17.255398

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c63260038e49'
down_revision: Union[str, None] = '100d577a3bfa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('games', sa.Column('video_url', sa.String(length=500), nullable=True))


def downgrade() -> None:
    op.drop_column('games', 'video_url')
