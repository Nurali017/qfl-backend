"""add joined_at and left_at to player_teams

Revision ID: z8d9e0f1g2h3
Revises: c1d2e3f4g5h6
Create Date: 2026-03-06 22:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "z8d9e0f1g2h3"
down_revision: Union[str, None] = "c1d2e3f4g5h6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("player_teams", sa.Column("joined_at", sa.Date(), nullable=True))
    op.add_column("player_teams", sa.Column("left_at", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("player_teams", "left_at")
    op.drop_column("player_teams", "joined_at")
