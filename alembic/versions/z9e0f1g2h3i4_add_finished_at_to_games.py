"""add finished_at to games

Revision ID: z9e0f1g2h3i4
Revises: z8d9e0f1g2h3
Create Date: 2026-03-09 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "z9e0f1g2h3i4"
down_revision: Union[str, None] = "p3q4r5s6t7u8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("finished_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "finished_at")
