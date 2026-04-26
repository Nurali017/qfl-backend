"""Add fcms_person_id to referees (and merge gh6i/gg5d heads).

Revision ID: hi7j8k9l0m1n
Revises: gh6i7j8k9l0m, gg5d6e7f8g9h
Create Date: 2026-04-26
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "hi7j8k9l0m1n"
down_revision: Union[str, Sequence[str], None] = ("gh6i7j8k9l0m", "gg5d6e7f8g9h")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("referees", sa.Column("fcms_person_id", sa.Integer(), nullable=True))
    op.create_index(
        "ix_referees_fcms_person_id",
        "referees",
        ["fcms_person_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_referees_fcms_person_id", table_name="referees")
    op.drop_column("referees", "fcms_person_id")
