"""Add fcms_competition_id to seasons for auto group discovery.

Revision ID: jk9l0m1n2o3p
Revises: ij8k9l0m1n2o
Create Date: 2026-04-29

Lets fcms_bulk_import resolve competition→groups dynamically so new rounds
(1/8, 1/4, final, etc.) get picked up without editing fcms_group_id by hand.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "jk9l0m1n2o3p"
down_revision: Union[str, None] = "ij8k9l0m1n2o"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "seasons",
        sa.Column("fcms_competition_id", sa.String(100), nullable=True),
    )
    op.create_index(
        "ix_seasons_fcms_competition_id", "seasons", ["fcms_competition_id"]
    )

    # Seed known mappings (season → CSV of FCMS competition IDs).
    # Some seasons map to multiple FCMS competitions (e.g. 2-Лига SW+NE).
    op.execute(sa.text("UPDATE seasons SET fcms_competition_id = '3517' WHERE id = 200"))
    op.execute(sa.text("UPDATE seasons SET fcms_competition_id = '3501' WHERE id = 201"))
    op.execute(sa.text("UPDATE seasons SET fcms_competition_id = '3598' WHERE id = 202"))
    op.execute(sa.text("UPDATE seasons SET fcms_competition_id = '3596,3597' WHERE id = 203"))
    op.execute(sa.text("UPDATE seasons SET fcms_competition_id = '3585' WHERE id = 204"))
    op.execute(sa.text("UPDATE seasons SET fcms_competition_id = '3674,3675' WHERE id = 205"))


def downgrade() -> None:
    op.drop_index("ix_seasons_fcms_competition_id", table_name="seasons")
    op.drop_column("seasons", "fcms_competition_id")
