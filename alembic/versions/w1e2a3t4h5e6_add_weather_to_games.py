"""add weather columns to games

Revision ID: w1e2a3t4h5e6
Revises: a0b1c2d3e4f5g6
Create Date: 2026-03-11 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "w1e2a3t4h5e6"
down_revision: Union[str, None] = "a0b1c2d3e4f5g6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("weather_temp", sa.Integer(), nullable=True))
    op.add_column("games", sa.Column("weather_condition", sa.String(50), nullable=True))
    op.add_column("games", sa.Column("weather_fetched_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "weather_fetched_at")
    op.drop_column("games", "weather_condition")
    op.drop_column("games", "weather_temp")
