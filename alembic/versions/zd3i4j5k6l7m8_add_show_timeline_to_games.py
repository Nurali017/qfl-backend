"""add show_timeline to games

Revision ID: zd3i4j5k6l7m8
Revises: zc2h3i4j5k6l7
Create Date: 2026-03-16 18:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "zd3i4j5k6l7m8"
down_revision: Union[str, None] = "zc2h3i4j5k6l7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "games",
        sa.Column("show_timeline", sa.Boolean(), nullable=False, server_default="true"),
    )


def downgrade() -> None:
    op.drop_column("games", "show_timeline")
