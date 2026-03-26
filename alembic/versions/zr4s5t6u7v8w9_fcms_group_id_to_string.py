"""change fcms_group_id from integer to string for multi-group support

Revision ID: zr4s5t6u7v8w9
Revises: zq3r4s5t6u7v8
Create Date: 2026-03-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "zr4s5t6u7v8w9"
down_revision: Union[str, None] = "zq3r4s5t6u7v8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop unique constraint and index on integer column
    op.drop_constraint("uq_seasons_fcms_group_id", "seasons", type_="unique")
    op.drop_index("ix_seasons_fcms_group_id", table_name="seasons")

    # Change column type from Integer to String
    op.alter_column(
        "seasons",
        "fcms_group_id",
        existing_type=sa.Integer(),
        type_=sa.String(100),
        existing_nullable=True,
        postgresql_using="fcms_group_id::text",
    )

    # Recreate index (no unique constraint — multiple groups share seasons)
    op.create_index("ix_seasons_fcms_group_id", "seasons", ["fcms_group_id"])

    # Fix Second League: old group 11081 doesn't exist, real groups are 11083 (SW) + 11084 (NE)
    op.execute(
        sa.text("UPDATE seasons SET fcms_group_id = '11083,11084' WHERE id = 203")
    )


def downgrade() -> None:
    # Revert Second League to single (invalid) group ID
    op.execute(
        sa.text("UPDATE seasons SET fcms_group_id = '11081' WHERE id = 203")
    )

    op.drop_index("ix_seasons_fcms_group_id", table_name="seasons")

    op.alter_column(
        "seasons",
        "fcms_group_id",
        existing_type=sa.String(100),
        type_=sa.Integer(),
        existing_nullable=True,
        postgresql_using="fcms_group_id::integer",
    )

    op.create_index("ix_seasons_fcms_group_id", "seasons", ["fcms_group_id"])
    op.create_unique_constraint("uq_seasons_fcms_group_id", "seasons", ["fcms_group_id"])
