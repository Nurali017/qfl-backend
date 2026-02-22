"""migrate_games_to_bigint_and_add_sota_id

Revision ID: z1a2b3c4d5e6
Revises: y0z1a2b3c4d5
Create Date: 2026-02-20

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "z1a2b3c4d5e6"
down_revision: Union[str, None] = "y0z1a2b3c4d5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# All child tables that have a game_id FK to games.id (UUID)
CHILD_TABLES = [
    "game_events",
    "game_lineups",
    "game_player_stats",
    "game_team_stats",
    "game_referees",
    "playoff_brackets",
]


def upgrade() -> None:
    # 1) Create sequence and temporary mapping table.
    op.execute("CREATE SEQUENCE IF NOT EXISTS games_id_seq")
    op.create_table(
        "game_id_map",
        sa.Column("old_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("new_id", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("old_id"),
        sa.UniqueConstraint("new_id"),
    )
    op.execute(
        """
        INSERT INTO game_id_map (old_id, new_id)
        SELECT id, nextval('games_id_seq')
        FROM games
        ORDER BY id
        """
    )

    # 2) Add sota_id and preserve old UUID IDs as integration identifiers.
    op.add_column("games", sa.Column("sota_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.execute("UPDATE games SET sota_id = id")

    # 3) Add new bigint columns to games and all child tables.
    op.add_column("games", sa.Column("id_new", sa.BigInteger(), nullable=True))
    for table in CHILD_TABLES:
        op.add_column(table, sa.Column("game_id_new", sa.BigInteger(), nullable=True))

    # 4) Backfill bigint IDs.
    op.execute(
        """
        UPDATE games g
        SET id_new = m.new_id
        FROM game_id_map m
        WHERE g.id = m.old_id
        """
    )
    for table in CHILD_TABLES:
        op.execute(
            f"""
            UPDATE {table} t
            SET game_id_new = m.new_id
            FROM game_id_map m
            WHERE t.game_id = m.old_id
            """
        )

    # 5) Drop all foreign keys that reference games to avoid dependency issues.
    op.execute(
        """
        DO $$
        DECLARE r RECORD;
        BEGIN
          FOR r IN
            SELECT conrelid::regclass AS table_name, conname
            FROM pg_constraint
            WHERE contype = 'f' AND confrelid = 'games'::regclass
          LOOP
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.table_name, r.conname);
          END LOOP;
        END $$;
        """
    )

    # 6) Remove old UUID game_id columns and promote bigint columns in child tables.
    for table in CHILD_TABLES:
        op.drop_column(table, "game_id")
        op.alter_column(table, "game_id_new", new_column_name="game_id")
        # playoff_brackets.game_id is nullable
        nullable = table == "playoff_brackets"
        op.alter_column(table, "game_id", existing_type=sa.BigInteger(), nullable=nullable)

    # Drop old PK on games(id uuid), swap columns, and recreate bigint PK.
    op.execute(
        """
        DO $$
        DECLARE pk_name text;
        BEGIN
          SELECT conname INTO pk_name
          FROM pg_constraint
          WHERE conrelid = 'games'::regclass AND contype = 'p'
          LIMIT 1;
          IF pk_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE games DROP CONSTRAINT %I', pk_name);
          END IF;
        END $$;
        """
    )
    op.drop_column("games", "id")
    op.alter_column("games", "id_new", new_column_name="id")
    op.alter_column(
        "games",
        "id",
        existing_type=sa.BigInteger(),
        nullable=False,
        server_default=sa.text("nextval('games_id_seq'::regclass)"),
    )
    op.execute("ALTER SEQUENCE games_id_seq OWNED BY games.id")
    op.create_primary_key("pk_games", "games", ["id"])

    # 7) Recreate FK constraints.
    op.create_foreign_key(
        "fk_game_events_game_id_games",
        "game_events",
        "games",
        ["game_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_lineups_game_id_games",
        "game_lineups",
        "games",
        ["game_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_player_stats_game_id_games",
        "game_player_stats",
        "games",
        ["game_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_team_stats_game_id_games",
        "game_team_stats",
        "games",
        ["game_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_referees_game_id_games",
        "game_referees",
        "games",
        ["game_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_playoff_brackets_game_id_games",
        "playoff_brackets",
        "games",
        ["game_id"],
        ["id"],
    )

    # 8) Recreate unique constraints and indexes that involve game_id.
    op.create_unique_constraint(
        "uq_game_lineup_player",
        "game_lineups",
        ["game_id", "player_id"],
    )
    op.create_unique_constraint(
        "uq_game_player_stats",
        "game_player_stats",
        ["game_id", "player_id"],
    )
    op.create_unique_constraint(
        "uq_game_team_stats",
        "game_team_stats",
        ["game_id", "team_id"],
    )

    op.create_index("ix_game_events_game_id", "game_events", ["game_id"], unique=False)
    op.create_index("ix_game_lineups_game_id", "game_lineups", ["game_id"], unique=False)
    op.create_index("ix_game_player_stats_game_id", "game_player_stats", ["game_id"], unique=False)
    op.create_index("ix_game_referees_game_id", "game_referees", ["game_id"], unique=False)

    # Recreate indexes from n2i3j4k5l6m7 (match center performance)
    op.create_index("ix_games_season_date", "games", ["season_id", "date"], unique=False)

    # Add unique index for sota_id.
    op.create_index("ix_games_sota_id", "games", ["sota_id"], unique=True)

    # 9) Clean temporary mapping.
    op.drop_table("game_id_map")

    # 10) Sanity checks: ensure no orphans in game FK tables.
    op.execute(
        """
        DO $$
        DECLARE c bigint;
        BEGIN
          SELECT COUNT(*) INTO c FROM game_events ge LEFT JOIN games g ON g.id = ge.game_id WHERE g.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_events: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_lineups gl LEFT JOIN games g ON g.id = gl.game_id WHERE g.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_lineups: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_player_stats gps LEFT JOIN games g ON g.id = gps.game_id WHERE g.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_player_stats: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_team_stats gts LEFT JOIN games g ON g.id = gts.game_id WHERE g.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_team_stats: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_referees gr LEFT JOIN games g ON g.id = gr.game_id WHERE g.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_referees: %', c; END IF;

          SELECT COUNT(*) INTO c FROM playoff_brackets pb LEFT JOIN games g ON g.id = pb.game_id WHERE pb.game_id IS NOT NULL AND g.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in playoff_brackets: %', c; END IF;
        END $$;
        """
    )


def downgrade() -> None:
    raise NotImplementedError(
        "Downgrade is not supported for games UUID->BIGINT migration because it is irreversible."
    )
