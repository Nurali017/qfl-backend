"""remove_tournament

Revision ID: e6f7g8h9i0j1
Revises: d5e6f7g8h9i0
Create Date: 2026-02-22
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e6f7g8h9i0j1"
down_revision: Union[str, None] = "d5e6f7g8h9i0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add championship_id column (nullable initially)
    op.add_column(
        "seasons",
        sa.Column("championship_id", sa.Integer(), sa.ForeignKey("championships.id"), nullable=True),
    )

    # 2. Populate championship_id from tournaments.championship_id
    op.execute("""
        UPDATE seasons
        SET championship_id = tournaments.championship_id
        FROM tournaments
        WHERE seasons.tournament_id = tournaments.id
    """)

    # 2b. Remove orphan seasons that have no tournament mapping and no data
    op.execute("""
        DELETE FROM seasons
        WHERE championship_id IS NULL
          AND NOT EXISTS (SELECT 1 FROM games WHERE games.season_id = seasons.id)
          AND NOT EXISTS (SELECT 1 FROM score_table WHERE score_table.season_id = seasons.id)
    """)

    # 3. Make championship_id NOT NULL
    op.alter_column("seasons", "championship_id", nullable=False)

    # 4. Create index on championship_id
    op.create_index("ix_seasons_championship_id", "seasons", ["championship_id"])

    # 5. Drop FK constraint and column tournament_id from seasons
    op.drop_constraint("seasons_tournament_id_fkey", "seasons", type_="foreignkey")
    op.drop_column("seasons", "tournament_id")

    # 6. Drop tournaments table
    op.drop_table("tournaments")


def downgrade() -> None:
    # 1. Recreate tournaments table
    op.create_table(
        "tournaments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("name_kz", sa.String(255)),
        sa.Column("name_en", sa.String(255)),
        sa.Column("country_code", sa.String(10)),
        sa.Column("country_name", sa.String(100)),
        sa.Column("country_name_kz", sa.String(100)),
        sa.Column("country_name_en", sa.String(100)),
        sa.Column("championship_id", sa.Integer(), sa.ForeignKey("championships.id"), index=True),
        sa.Column("updated_at", sa.DateTime()),
    )

    # 2. Add tournament_id column to seasons
    op.add_column(
        "seasons",
        sa.Column("tournament_id", sa.Integer(), sa.ForeignKey("tournaments.id"), nullable=True),
    )

    # 3. Drop championship_id from seasons
    op.drop_index("ix_seasons_championship_id", "seasons")
    op.drop_constraint("seasons_championship_id_fkey", "seasons", type_="foreignkey")
    op.drop_column("seasons", "championship_id")
