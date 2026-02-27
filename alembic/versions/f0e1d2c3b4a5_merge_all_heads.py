"""merge all heads into single head

Revision ID: f0e1d2c3b4a5
Revises: aa1b2c3d4e5f, cc6d7e8f9g0h, d4e5f6g7h8i9, e5f6g7h8i9j0
Create Date: 2026-02-27 18:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "f0e1d2c3b4a5"
down_revision = ("aa1b2c3d4e5f", "d4e5f6g7h8i9", "e5f6g7h8i9j0")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
