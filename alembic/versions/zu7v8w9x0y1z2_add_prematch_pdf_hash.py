"""Add prematch_pdf_hash to games for pre-match PDF dedup.

Revision ID: zu7v8w9x0y1z2
Revises: zt6u7v8w9x0y1
Create Date: 2026-04-03
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "zu7v8w9x0y1z2"
down_revision: Union[str, None] = "zt6u7v8w9x0y1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("prematch_pdf_hash", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "prematch_pdf_hash")
