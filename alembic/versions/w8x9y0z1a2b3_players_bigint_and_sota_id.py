"""migrate_players_to_bigint_and_add_sota_id

Revision ID: w8x9y0z1a2b3
Revises: v2w3x4y5z6a7
Create Date: 2026-02-19

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "w8x9y0z1a2b3"
down_revision: Union[str, None] = "v2w3x4y5z6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Create sequence and temporary mapping table.
    op.execute("CREATE SEQUENCE IF NOT EXISTS players_id_seq")
    op.create_table(
        "player_id_map",
        sa.Column("old_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("new_id", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("old_id"),
        sa.UniqueConstraint("new_id"),
    )
    op.execute(
        """
        INSERT INTO player_id_map (old_id, new_id)
        SELECT id, nextval('players_id_seq')
        FROM players
        ORDER BY id
        """
    )

    # 2) Add sota_id and preserve old UUID IDs as integration identifiers.
    op.add_column("players", sa.Column("sota_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.execute("UPDATE players SET sota_id = id")

    # 3) Add new bigint columns.
    op.add_column("players", sa.Column("id_new", sa.BigInteger(), nullable=True))
    op.add_column("player_teams", sa.Column("player_id_new", sa.BigInteger(), nullable=True))
    op.add_column("player_season_stats", sa.Column("player_id_new", sa.BigInteger(), nullable=True))
    op.add_column("game_player_stats", sa.Column("player_id_new", sa.BigInteger(), nullable=True))
    op.add_column("game_lineups", sa.Column("player_id_new", sa.BigInteger(), nullable=True))
    op.add_column("game_events", sa.Column("player_id_new", sa.BigInteger(), nullable=True))
    op.add_column("game_events", sa.Column("player2_id_new", sa.BigInteger(), nullable=True))
    op.add_column("game_events", sa.Column("assist_player_id_new", sa.BigInteger(), nullable=True))

    # 4) Backfill bigint IDs.
    op.execute(
        """
        UPDATE players p
        SET id_new = m.new_id
        FROM player_id_map m
        WHERE p.id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE player_teams pt
        SET player_id_new = m.new_id
        FROM player_id_map m
        WHERE pt.player_id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE player_season_stats pss
        SET player_id_new = m.new_id
        FROM player_id_map m
        WHERE pss.player_id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE game_player_stats gps
        SET player_id_new = m.new_id
        FROM player_id_map m
        WHERE gps.player_id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE game_lineups gl
        SET player_id_new = m.new_id
        FROM player_id_map m
        WHERE gl.player_id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE game_events ge
        SET player_id_new = m.new_id
        FROM player_id_map m
        WHERE ge.player_id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE game_events ge
        SET player2_id_new = m.new_id
        FROM player_id_map m
        WHERE ge.player2_id = m.old_id
        """
    )
    op.execute(
        """
        UPDATE game_events ge
        SET assist_player_id_new = m.new_id
        FROM player_id_map m
        WHERE ge.assist_player_id = m.old_id
        """
    )

    # 5) Drop all foreign keys that reference players to avoid dependency issues.
    op.execute(
        """
        DO $$
        DECLARE r RECORD;
        BEGIN
          FOR r IN
            SELECT conrelid::regclass AS table_name, conname
            FROM pg_constraint
            WHERE contype = 'f' AND confrelid = 'players'::regclass
          LOOP
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.table_name, r.conname);
          END LOOP;
        END $$;
        """
    )

    # 6) Remove old UUID columns and promote bigint columns.
    op.drop_column("player_teams", "player_id")
    op.alter_column("player_teams", "player_id_new", new_column_name="player_id")
    op.alter_column("player_teams", "player_id", existing_type=sa.BigInteger(), nullable=False)

    op.drop_column("player_season_stats", "player_id")
    op.alter_column("player_season_stats", "player_id_new", new_column_name="player_id")
    op.alter_column("player_season_stats", "player_id", existing_type=sa.BigInteger(), nullable=False)

    op.drop_column("game_player_stats", "player_id")
    op.alter_column("game_player_stats", "player_id_new", new_column_name="player_id")
    op.alter_column("game_player_stats", "player_id", existing_type=sa.BigInteger(), nullable=False)

    op.drop_column("game_lineups", "player_id")
    op.alter_column("game_lineups", "player_id_new", new_column_name="player_id")
    op.alter_column("game_lineups", "player_id", existing_type=sa.BigInteger(), nullable=False)

    op.drop_column("game_events", "player_id")
    op.alter_column("game_events", "player_id_new", new_column_name="player_id")
    op.drop_column("game_events", "player2_id")
    op.alter_column("game_events", "player2_id_new", new_column_name="player2_id")
    op.drop_column("game_events", "assist_player_id")
    op.alter_column("game_events", "assist_player_id_new", new_column_name="assist_player_id")

    # Drop old PK on players(id uuid), swap columns, and recreate bigint PK.
    op.execute(
        """
        DO $$
        DECLARE pk_name text;
        BEGIN
          SELECT conname INTO pk_name
          FROM pg_constraint
          WHERE conrelid = 'players'::regclass AND contype = 'p'
          LIMIT 1;
          IF pk_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE players DROP CONSTRAINT %I', pk_name);
          END IF;
        END $$;
        """
    )
    op.drop_column("players", "id")
    op.alter_column("players", "id_new", new_column_name="id")
    op.alter_column(
        "players",
        "id",
        existing_type=sa.BigInteger(),
        nullable=False,
        server_default=sa.text("nextval('players_id_seq'::regclass)"),
    )
    op.execute("ALTER SEQUENCE players_id_seq OWNED BY players.id")
    op.create_primary_key("pk_players", "players", ["id"])

    # 7) Recreate FK constraints and indexes.
    op.create_foreign_key(
        "fk_player_teams_player_id_players",
        "player_teams",
        "players",
        ["player_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_player_season_stats_player_id_players",
        "player_season_stats",
        "players",
        ["player_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_player_stats_player_id_players",
        "game_player_stats",
        "players",
        ["player_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_lineups_player_id_players",
        "game_lineups",
        "players",
        ["player_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_events_player_id_players",
        "game_events",
        "players",
        ["player_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_events_player2_id_players",
        "game_events",
        "players",
        ["player2_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_game_events_assist_player_id_players",
        "game_events",
        "players",
        ["assist_player_id"],
        ["id"],
    )

    # Recreate key unique constraints and indexes that involve player_id.
    op.create_unique_constraint(
        "uq_player_team_season",
        "player_teams",
        ["player_id", "team_id", "season_id"],
    )
    op.create_unique_constraint(
        "uq_player_season_stats",
        "player_season_stats",
        ["player_id", "season_id"],
    )
    op.create_unique_constraint(
        "uq_game_player_stats",
        "game_player_stats",
        ["game_id", "player_id"],
    )
    op.create_unique_constraint(
        "uq_game_lineup_player",
        "game_lineups",
        ["game_id", "player_id"],
    )

    op.create_index("ix_player_season_stats_player_id", "player_season_stats", ["player_id"], unique=False)
    op.create_index("ix_game_player_stats_player_id", "game_player_stats", ["player_id"], unique=False)
    op.create_index("ix_game_lineups_player_id", "game_lineups", ["player_id"], unique=False)

    # Add unique index for sota_id.
    op.create_index("ix_players_sota_id", "players", ["sota_id"], unique=True)

    # 8) Clean temporary mapping.
    op.drop_table("player_id_map")

    # 9) Sanity checks: ensure no orphans in player FK tables.
    op.execute(
        """
        DO $$
        DECLARE c bigint;
        BEGIN
          SELECT COUNT(*) INTO c FROM player_teams pt LEFT JOIN players p ON p.id = pt.player_id WHERE p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in player_teams: %', c; END IF;

          SELECT COUNT(*) INTO c FROM player_season_stats pss LEFT JOIN players p ON p.id = pss.player_id WHERE p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in player_season_stats: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_player_stats gps LEFT JOIN players p ON p.id = gps.player_id WHERE p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_player_stats: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_lineups gl LEFT JOIN players p ON p.id = gl.player_id WHERE p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_lineups: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_events ge LEFT JOIN players p ON p.id = ge.player_id WHERE ge.player_id IS NOT NULL AND p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_events.player_id: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_events ge LEFT JOIN players p ON p.id = ge.player2_id WHERE ge.player2_id IS NOT NULL AND p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_events.player2_id: %', c; END IF;

          SELECT COUNT(*) INTO c FROM game_events ge LEFT JOIN players p ON p.id = ge.assist_player_id WHERE ge.assist_player_id IS NOT NULL AND p.id IS NULL;
          IF c > 0 THEN RAISE EXCEPTION 'Orphan rows in game_events.assist_player_id: %', c; END IF;
        END $$;
        """
    )


def downgrade() -> None:
    raise NotImplementedError(
        "Downgrade is not supported for players UUID->BIGINT migration because it is irreversible."
    )
