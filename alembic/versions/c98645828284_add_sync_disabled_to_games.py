"""add sync_disabled to games

Revision ID: c98645828284
Revises: 11d0da07daf4
Create Date: 2026-02-27 22:02:26.082736

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c98645828284'
down_revision: Union[str, None] = '11d0da07daf4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'games',
        sa.Column('sync_disabled', sa.Boolean(), server_default='false', nullable=False),
    )


def downgrade() -> None:
    op.drop_column('games', 'sync_disabled')
