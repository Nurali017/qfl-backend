"""add_ticket_url_to_games

Revision ID: m1h2i3j4k5l6
Revises: l0g1h2i3j4k5
Create Date: 2026-01-20 12:00:00.000000

Adds ticket_url field to games table for match center ticket purchase links.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'm1h2i3j4k5l6'
down_revision: Union[str, None] = 'l0g1h2i3j4k5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('games', sa.Column('ticket_url', sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column('games', 'ticket_url')
