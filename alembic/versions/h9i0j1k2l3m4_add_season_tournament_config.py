"""add season tournament config columns

Revision ID: h9i0j1k2l3m4
Revises: g8h9i0j1k2l3
Create Date: 2026-02-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "h9i0j1k2l3m4"
down_revision: Union[str, None] = "g8h9i0j1k2l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add tournament config columns to seasons
    op.add_column("seasons", sa.Column("frontend_code", sa.String(20), nullable=True))
    op.add_column("seasons", sa.Column("tournament_type", sa.String(30), nullable=True))
    op.add_column("seasons", sa.Column("tournament_format", sa.String(30), nullable=True))
    op.add_column("seasons", sa.Column("has_table", sa.Boolean(), nullable=False, server_default="false"))
    op.add_column("seasons", sa.Column("has_bracket", sa.Boolean(), nullable=False, server_default="false"))
    op.add_column("seasons", sa.Column("sponsor_name", sa.String(200), nullable=True))
    op.add_column("seasons", sa.Column("sponsor_name_kz", sa.String(200), nullable=True))
    op.add_column("seasons", sa.Column("logo", sa.String(500), nullable=True))
    op.add_column("seasons", sa.Column("current_round", sa.Integer(), nullable=True))
    op.add_column("seasons", sa.Column("total_rounds", sa.Integer(), nullable=True))
    op.add_column("seasons", sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("seasons", sa.Column("colors", sa.JSON(), nullable=True))

    op.create_index("ix_seasons_frontend_code", "seasons", ["frontend_code"])

    # Populate data for known seasons
    seasons_table = sa.table(
        "seasons",
        sa.column("id", sa.Integer),
        sa.column("frontend_code", sa.String),
        sa.column("tournament_type", sa.String),
        sa.column("tournament_format", sa.String),
        sa.column("has_table", sa.Boolean),
        sa.column("has_bracket", sa.Boolean),
        sa.column("sponsor_name", sa.String),
        sa.column("sponsor_name_kz", sa.String),
        sa.column("logo", sa.String),
        sa.column("current_round", sa.Integer),
        sa.column("total_rounds", sa.Integer),
        sa.column("sort_order", sa.Integer),
        sa.column("colors", sa.JSON),
    )

    # Season 61 - Premier League
    op.execute(
        seasons_table.update()
        .where(seasons_table.c.id == 61)
        .values(
            frontend_code="pl",
            tournament_type="league",
            tournament_format="round_robin",
            has_table=True,
            has_bracket=False,
            sponsor_name="ПРЕМЬЕР-ЛИГА",
            sponsor_name_kz="ПРЕМЬЕР-ЛИГА",
            logo="/images/tournaments/pl.png",
            current_round=26,
            total_rounds=33,
            sort_order=1,
            colors={
                "primary": "30 77 140",
                "primaryLight": "42 95 163",
                "primaryDark": "22 58 107",
                "accent": "229 183 59",
                "accentSoft": "240 201 93",
            },
        )
    )

    # Season 85 - First League
    op.execute(
        seasons_table.update()
        .where(seasons_table.c.id == 85)
        .values(
            frontend_code="1l",
            tournament_type="league",
            tournament_format="round_robin",
            has_table=True,
            has_bracket=False,
            sponsor_name="БІРІНШІ ЛИГА",
            sponsor_name_kz="БІРІНШІ ЛИГА",
            logo="/images/tournaments/1l.png",
            sort_order=2,
            colors={
                "primary": "61 122 62",
                "primaryLight": "78 155 79",
                "primaryDark": "46 94 47",
                "accent": "123 198 125",
                "accentSoft": "163 217 164",
            },
        )
    )

    # Season 71 - Cup
    op.execute(
        seasons_table.update()
        .where(seasons_table.c.id == 71)
        .values(
            frontend_code="cup",
            tournament_type="cup",
            tournament_format="knockout",
            has_table=False,
            has_bracket=True,
            sponsor_name="OLIMPBET ҚАЗАҚСТАН КУБОГЫ",
            sponsor_name_kz="OLIMPBET ҚАЗАҚСТАН КУБОГЫ",
            logo="/images/tournaments/cup.png",
            sort_order=3,
            colors={
                "primary": "74 26 43",
                "primaryLight": "107 45 66",
                "primaryDark": "53 18 31",
                "accent": "139 58 85",
                "accentSoft": "181 102 126",
            },
        )
    )

    # Season 80 - Second League (will become unified after merge)
    op.execute(
        seasons_table.update()
        .where(seasons_table.c.id == 80)
        .values(
            frontend_code="2l",
            tournament_type="league",
            tournament_format="round_robin",
            has_table=True,
            has_bracket=False,
            sponsor_name="ЕКІНШІ ЛИГА",
            sponsor_name_kz="ЕКІНШІ ЛИГА",
            logo="/images/tournaments/2l.png",
            sort_order=4,
            colors={
                "primary": "168 106 43",
                "primaryLight": "196 132 61",
                "primaryDark": "127 79 32",
                "accent": "212 168 92",
                "accentSoft": "229 200 138",
            },
        )
    )

    # Season 84 - Women's League
    op.execute(
        seasons_table.update()
        .where(seasons_table.c.id == 84)
        .values(
            frontend_code="el",
            tournament_type="league",
            tournament_format="round_robin",
            has_table=True,
            has_bracket=False,
            sponsor_name="ӘЙЕЛДЕР ЛИГАСЫ",
            sponsor_name_kz="ӘЙЕЛДЕР ЛИГАСЫ",
            logo="/images/tournaments/el.png",
            sort_order=5,
            colors={
                "primary": "107 79 160",
                "primaryLight": "133 102 184",
                "primaryDark": "80 59 120",
                "accent": "160 126 214",
                "accentSoft": "196 168 232",
            },
        )
    )


def downgrade() -> None:
    op.drop_index("ix_seasons_frontend_code", table_name="seasons")
    op.drop_column("seasons", "colors")
    op.drop_column("seasons", "sort_order")
    op.drop_column("seasons", "total_rounds")
    op.drop_column("seasons", "current_round")
    op.drop_column("seasons", "logo")
    op.drop_column("seasons", "sponsor_name_kz")
    op.drop_column("seasons", "sponsor_name")
    op.drop_column("seasons", "has_bracket")
    op.drop_column("seasons", "has_table")
    op.drop_column("seasons", "tournament_format")
    op.drop_column("seasons", "tournament_type")
    op.drop_column("seasons", "frontend_code")
