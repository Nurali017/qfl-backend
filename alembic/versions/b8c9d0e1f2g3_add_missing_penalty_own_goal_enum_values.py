"""add missing penalty and own_goal enum values

Revision ID: b8c9d0e1f2g3
Revises: a7b8c9d0e1f2
Create Date: 2026-03-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = "b8c9d0e1f2g3"
down_revision: Union[str, None] = "a7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE gameeventtype ADD VALUE IF NOT EXISTS 'penalty'")
    op.execute("ALTER TYPE gameeventtype ADD VALUE IF NOT EXISTS 'own_goal'")


def downgrade() -> None:
    # PostgreSQL does not support removing values from an enum type
    pass
