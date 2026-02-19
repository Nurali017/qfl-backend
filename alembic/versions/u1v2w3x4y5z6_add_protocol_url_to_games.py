"""add_protocol_url_to_games

Revision ID: u1v2w3x4y5z6
Revises: s7t8u9v0w1x2
Create Date: 2026-02-19 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "u1v2w3x4y5z6"
down_revision: Union[str, None] = "s7t8u9v0w1x2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("protocol_url", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "protocol_url")
