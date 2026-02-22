"""add_legacy_migration_tables

Revision ID: y0z1a2b3c4d5
Revises: x9y0z1a2b3c4
Create Date: 2026-02-19
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


# revision identifiers, used by Alembic.
revision: str = "y0z1a2b3c4d5"
down_revision: Union[str, None] = "x9y0z1a2b3c4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── 1. New tables ──────────────────────────────────────────

    # championships
    op.create_table(
        "championships",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("name_kz", sa.String(255)),
        sa.Column("name_en", sa.String(255)),
        sa.Column("short_name", sa.String(50)),
        sa.Column("short_name_kz", sa.String(50)),
        sa.Column("short_name_en", sa.String(50)),
        sa.Column("slug", sa.String(100), unique=True),
        sa.Column("sota_ids", sa.Text()),
        sa.Column("sort_order", sa.Integer(), server_default="0"),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # cities
    op.create_table(
        "cities",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("name_kz", sa.String(255)),
        sa.Column("name_en", sa.String(255)),
        sa.Column("country_id", sa.Integer(), sa.ForeignKey("countries.id")),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # clubs
    op.create_table(
        "clubs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("name_kz", sa.String(255)),
        sa.Column("name_en", sa.String(255)),
        sa.Column("short_name", sa.String(50)),
        sa.Column("logo_url", sa.String(500)),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("cities.id")),
        sa.Column("stadium_id", sa.Integer(), sa.ForeignKey("stadiums.id")),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # stages
    op.create_table(
        "stages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("name_kz", sa.String(255)),
        sa.Column("name_en", sa.String(255)),
        sa.Column("stage_number", sa.Integer()),
        sa.Column("sort_order", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # playoff_brackets
    op.create_table(
        "playoff_brackets",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("round_name", sa.String(50), nullable=False),
        sa.Column("side", sa.String(10), server_default="left"),
        sa.Column("sort_order", sa.Integer(), server_default="1"),
        sa.Column("game_id", PGUUID(as_uuid=True), sa.ForeignKey("games.id")),
        sa.Column("is_visible", sa.Boolean(), server_default="true"),
        sa.Column("is_third_place", sa.Boolean(), server_default="false"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # team_tournaments
    op.create_table(
        "team_tournaments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("group_name", sa.String(50)),
        sa.Column("is_disqualified", sa.Boolean(), server_default="false"),
        sa.Column("fine_points", sa.Integer(), server_default="0"),
        sa.Column("stadium_id", sa.Integer(), sa.ForeignKey("stadiums.id")),
        sa.Column("sort_order", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("team_id", "season_id", name="uq_team_tournament_season"),
    )

    # partners
    op.create_table(
        "partners",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), unique=True, index=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("logo_url", sa.String(500)),
        sa.Column("website", sa.String(500)),
        sa.Column("championship_id", sa.Integer(), sa.ForeignKey("championships.id")),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id")),
        sa.Column("sort_order", sa.Integer(), server_default="0"),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("show_in_news", sa.Boolean(), server_default="false"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # ── 2. New columns on existing tables ──────────────────────

    # tournaments
    op.add_column("tournaments", sa.Column("championship_id", sa.Integer(), sa.ForeignKey("championships.id")))
    op.create_index("ix_tournaments_championship_id", "tournaments", ["championship_id"])

    # teams
    op.add_column("teams", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.add_column("teams", sa.Column("club_id", sa.Integer(), sa.ForeignKey("clubs.id")))
    op.create_index("ix_teams_legacy_id", "teams", ["legacy_id"])
    op.create_index("ix_teams_club_id", "teams", ["club_id"])

    # stadiums
    op.add_column("stadiums", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.add_column("stadiums", sa.Column("city_id", sa.Integer(), sa.ForeignKey("cities.id")))
    op.add_column("stadiums", sa.Column("address_kz", sa.String(500)))
    op.add_column("stadiums", sa.Column("address_en", sa.String(500)))
    op.add_column("stadiums", sa.Column("photo_url", sa.String(500)))
    op.create_index("ix_stadiums_legacy_id", "stadiums", ["legacy_id"])
    op.create_index("ix_stadiums_city_id", "stadiums", ["city_id"])

    # games
    op.add_column("games", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.add_column("games", sa.Column("stage_id", sa.Integer(), sa.ForeignKey("stages.id")))
    op.add_column("games", sa.Column("home_penalty_score", sa.Integer()))
    op.add_column("games", sa.Column("away_penalty_score", sa.Integer()))
    op.create_index("ix_games_legacy_id", "games", ["legacy_id"])
    op.create_index("ix_games_stage_id", "games", ["stage_id"])

    # players
    op.add_column("players", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.add_column("players", sa.Column("height", sa.Integer()))
    op.add_column("players", sa.Column("weight", sa.Integer()))
    op.add_column("players", sa.Column("gender", sa.String(10)))
    op.create_index("ix_players_legacy_id", "players", ["legacy_id"])

    # coaches
    op.add_column("coaches", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.create_index("ix_coaches_legacy_id", "coaches", ["legacy_id"])

    # referees
    op.add_column("referees", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.create_index("ix_referees_legacy_id", "referees", ["legacy_id"])

    # seasons
    op.add_column("seasons", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.create_index("ix_seasons_legacy_id", "seasons", ["legacy_id"])

    # countries
    op.add_column("countries", sa.Column("legacy_id", sa.Integer(), unique=True))
    op.create_index("ix_countries_legacy_id", "countries", ["legacy_id"])


def downgrade() -> None:
    # ── Drop new columns ───────────────────────────────────────

    # countries
    op.drop_index("ix_countries_legacy_id", "countries")
    op.drop_column("countries", "legacy_id")

    # seasons
    op.drop_index("ix_seasons_legacy_id", "seasons")
    op.drop_column("seasons", "legacy_id")

    # referees
    op.drop_index("ix_referees_legacy_id", "referees")
    op.drop_column("referees", "legacy_id")

    # coaches
    op.drop_index("ix_coaches_legacy_id", "coaches")
    op.drop_column("coaches", "legacy_id")

    # players
    op.drop_index("ix_players_legacy_id", "players")
    op.drop_column("players", "gender")
    op.drop_column("players", "weight")
    op.drop_column("players", "height")
    op.drop_column("players", "legacy_id")

    # games
    op.drop_index("ix_games_stage_id", "games")
    op.drop_index("ix_games_legacy_id", "games")
    op.drop_column("games", "away_penalty_score")
    op.drop_column("games", "home_penalty_score")
    op.drop_column("games", "stage_id")
    op.drop_column("games", "legacy_id")

    # stadiums
    op.drop_index("ix_stadiums_city_id", "stadiums")
    op.drop_index("ix_stadiums_legacy_id", "stadiums")
    op.drop_column("stadiums", "photo_url")
    op.drop_column("stadiums", "address_en")
    op.drop_column("stadiums", "address_kz")
    op.drop_column("stadiums", "city_id")
    op.drop_column("stadiums", "legacy_id")

    # teams
    op.drop_index("ix_teams_club_id", "teams")
    op.drop_index("ix_teams_legacy_id", "teams")
    op.drop_column("teams", "club_id")
    op.drop_column("teams", "legacy_id")

    # tournaments
    op.drop_index("ix_tournaments_championship_id", "tournaments")
    op.drop_column("tournaments", "championship_id")

    # ── Drop new tables ────────────────────────────────────────
    op.drop_table("partners")
    op.drop_table("team_tournaments")
    op.drop_table("playoff_brackets")
    op.drop_table("stages")
    op.drop_table("clubs")
    op.drop_table("cities")
    op.drop_table("championships")
