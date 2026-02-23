"""add_is_visible_to_seasons

Revision ID: x1y2z3a4b5c6
Revises: w3x4y5z6a7b8
Create Date: 2026-02-24 01:40:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "x1y2z3a4b5c6"
down_revision: Union[str, None] = "w3x4y5z6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "seasons",
        sa.Column("is_visible", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )

    op.execute(
        """
        UPDATE seasons
        SET is_visible = false
        WHERE (
            date_start >= DATE '2024-01-01'
            AND date_start < DATE '2025-01-01'
        )
        OR (
            date_start IS NULL
            AND (
                COALESCE(name, '') ILIKE '%2024%'
                OR COALESCE(name_kz, '') ILIKE '%2024%'
                OR COALESCE(name_en, '') ILIKE '%2024%'
            )
        )
        """
    )


def downgrade() -> None:
    op.drop_column("seasons", "is_visible")
