"""add lineup embargo fields (telegram_sent_at, telegram_hash)

Revision ID: zp2q3r4s5t6u7
Revises: zo1p2q3r4s5t6
Create Date: 2026-03-22 01:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "zp2q3r4s5t6u7"
down_revision: Union[str, None] = "zo1p2q3r4s5t6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "games",
        sa.Column("lineup_telegram_sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "games",
        sa.Column("lineup_telegram_hash", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("games", "lineup_telegram_hash")
    op.drop_column("games", "lineup_telegram_sent_at")
