"""Extend refereerole enum with match_commissioner and var_operator (FCMS roles).

Revision ID: ij8k9l0m1n2o
Revises: hi7j8k9l0m1n
Create Date: 2026-04-26

ALTER TYPE ... ADD VALUE must run outside a transaction block, hence
autocommit_block(). IF NOT EXISTS makes it idempotent.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "ij8k9l0m1n2o"
down_revision: Union[str, None] = "hi7j8k9l0m1n"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE refereerole ADD VALUE IF NOT EXISTS 'match_commissioner'")
        op.execute("ALTER TYPE refereerole ADD VALUE IF NOT EXISTS 'var_operator'")


def downgrade() -> None:
    # Postgres has no DROP VALUE; downgrade would require recreating the type.
    # Acceptable to leave values in place — they only become inert if not referenced.
    pass
