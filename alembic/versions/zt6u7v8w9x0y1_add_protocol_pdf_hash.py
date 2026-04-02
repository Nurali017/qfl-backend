"""Add protocol_pdf_hash to games for change detection.

Revision ID: zt6u7v8w9x0y1
Revises: zs5t6u7v8w9x0
Create Date: 2026-04-02
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "zt6u7v8w9x0y1"
down_revision: Union[str, None] = "zs5t6u7v8w9x0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("protocol_pdf_hash", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "protocol_pdf_hash")
