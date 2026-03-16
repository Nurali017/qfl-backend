"""add live_minute and live_half to games

Revision ID: zc2h3i4j5k6l7
Revises: zb1g2h3i4j5k6
Create Date: 2026-03-16 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "zc2h3i4j5k6l7"
down_revision: Union[str, None] = "sm2e3f4g5h6i7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("live_minute", sa.Integer(), nullable=True))
    op.add_column("games", sa.Column("live_half", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "live_half")
    op.drop_column("games", "live_minute")
