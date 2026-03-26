"""add INTERVIEW to article_type enum

Revision ID: zq3r4s5t6u7v8
Revises: zp2q3r4s5t6u7
Create Date: 2026-03-26
"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "zq3r4s5t6u7v8"
down_revision: Union[str, None] = "zp2q3r4s5t6u7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE article_type ADD VALUE IF NOT EXISTS 'INTERVIEW'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values directly.
    # To fully downgrade, recreate the type and update the column.
    pass
