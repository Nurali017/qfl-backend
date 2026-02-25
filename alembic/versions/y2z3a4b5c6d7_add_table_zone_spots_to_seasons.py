"""Add table zone spots configuration to seasons.

Revision ID: y2z3a4b5c6d7
Revises: x1y2z3a4b5c6
Create Date: 2026-02-25 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "y2z3a4b5c6d7"
down_revision = "x1y2z3a4b5c6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "seasons",
        sa.Column("champion_spots", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "seasons",
        sa.Column("euro_cup_spots", sa.Integer(), nullable=False, server_default="2"),
    )
    op.add_column(
        "seasons",
        sa.Column("relegation_spots", sa.Integer(), nullable=False, server_default="0"),
    )

    op.create_check_constraint(
        "ck_seasons_champion_spots_non_negative",
        "seasons",
        "champion_spots >= 0",
    )
    op.create_check_constraint(
        "ck_seasons_euro_cup_spots_non_negative",
        "seasons",
        "euro_cup_spots >= 0",
    )
    op.create_check_constraint(
        "ck_seasons_relegation_spots_non_negative",
        "seasons",
        "relegation_spots >= 0",
    )

    # Preserve current visuals for league tables.
    op.execute(
        sa.text(
            """
            UPDATE seasons
            SET champion_spots = 1,
                euro_cup_spots = 2
            WHERE has_table = true
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE seasons
            SET champion_spots = 0,
                euro_cup_spots = 0,
                relegation_spots = 0
            WHERE has_table = false
            """
        )
    )

    # Season-specific relegation rules.
    op.execute(sa.text("UPDATE seasons SET relegation_spots = 1 WHERE id = 61"))
    op.execute(sa.text("UPDATE seasons SET relegation_spots = 2 WHERE id = 200"))


def downgrade() -> None:
    op.drop_constraint("ck_seasons_relegation_spots_non_negative", "seasons", type_="check")
    op.drop_constraint("ck_seasons_euro_cup_spots_non_negative", "seasons", type_="check")
    op.drop_constraint("ck_seasons_champion_spots_non_negative", "seasons", type_="check")

    op.drop_column("seasons", "relegation_spots")
    op.drop_column("seasons", "euro_cup_spots")
    op.drop_column("seasons", "champion_spots")
