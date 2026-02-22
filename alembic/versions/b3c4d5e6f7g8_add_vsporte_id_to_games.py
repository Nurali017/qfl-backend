"""add_vsporte_id_to_games

Revision ID: b3c4d5e6f7g8
Revises: a2b3c4d5e6f7
Create Date: 2026-02-21

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "b3c4d5e6f7g8"
down_revision: Union[str, None] = "a2b3c4d5e6f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("games", sa.Column("vsporte_id", sa.String(100), nullable=True))
    op.create_index("ix_games_vsporte_id", "games", ["vsporte_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_games_vsporte_id", table_name="games")
    op.drop_column("games", "vsporte_id")
