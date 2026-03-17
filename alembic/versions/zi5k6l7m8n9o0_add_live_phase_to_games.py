"""add live_phase to games

Revision ID: zi5k6l7m8n9o0
Revises: ze4j5k6l7m8n9
Create Date: 2026-03-18 14:30:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "zi5k6l7m8n9o0"
down_revision: Union[str, None] = "ze4j5k6l7m8n9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "games",
        sa.Column("live_phase", sa.String(length=20), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("games", "live_phase")
