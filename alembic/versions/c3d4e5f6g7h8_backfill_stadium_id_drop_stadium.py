"""backfill stadium_id from stadium string, then drop stadium column

Match game.stadium strings against stadiums table (name, name_kz, name_ru, name_en)
to set stadium_id, then drop the redundant text column.

Revision ID: c3d4e5f6g7h8
Revises: b2c3d4e5f6g7
Create Date: 2026-02-27 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "c3d4e5f6g7h8"
down_revision = "b2c3d4e5f6g7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Backfill stadium_id from stadium string matches
    op.execute("""
        UPDATE games g
        SET stadium_id = s.id
        FROM stadiums s
        WHERE g.stadium IS NOT NULL
          AND g.stadium_id IS NULL
          AND (
            LOWER(TRIM(g.stadium)) = LOWER(TRIM(s.name))
            OR LOWER(TRIM(g.stadium)) = LOWER(TRIM(s.name_kz))
            OR LOWER(TRIM(g.stadium)) = LOWER(TRIM(s.name_ru))
            OR LOWER(TRIM(g.stadium)) = LOWER(TRIM(s.name_en))
          )
    """)

    # 2. Check for unmatched stadium strings — abort if any exist
    conn = op.get_bind()
    unmatched = conn.execute(sa.text("""
        SELECT DISTINCT stadium
        FROM games
        WHERE stadium IS NOT NULL AND stadium_id IS NULL
        ORDER BY stadium
    """))
    rows = unmatched.fetchall()
    if rows:
        names = [r[0] for r in rows]
        raise RuntimeError(
            f"Cannot drop stadium column: {len(names)} unmatched stadium strings "
            f"have no corresponding stadiums.name match: {names}. "
            f"Add missing stadiums or manually set stadium_id before re-running."
        )

    # 3. Drop the legacy column (safe — all non-null values matched)
    op.drop_column("games", "stadium")


def downgrade() -> None:
    # Re-add the stadium text column
    op.add_column(
        "games",
        sa.Column("stadium", sa.String(255), nullable=True),
    )

    # Backfill from stadiums.name
    op.execute("""
        UPDATE games g
        SET stadium = s.name
        FROM stadiums s
        WHERE g.stadium_id = s.id
    """)
