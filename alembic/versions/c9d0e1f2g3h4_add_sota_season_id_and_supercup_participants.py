"""Add sota_season_id to seasons and supercup participants

Revision ID: c9d0e1f2g3h4
Revises: b8c9d0e1f2g3
Create Date: 2026-03-03
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c9d0e1f2g3h4"
down_revision: Union[str, None] = "b8c9d0e1f2g3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Manually created seasons (IDs don't come from SOTA)
MANUAL_SEASON_IDS = (200, 201)

# Supercup 2026 participants
SUPERCUP_SEASON_ID = 201
SUPERCUP_TEAM_IDS = (13, 90)  # Кайрат, Тобыл


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Add sota_season_id column (idempotent)
    existing = {
        row[0]
        for row in conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'seasons'"
            )
        )
    }
    if "sota_season_id" not in existing:
        op.add_column("seasons", sa.Column("sota_season_id", sa.Integer(), nullable=True))
    # Create index if it doesn't exist
    idx_exists = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE indexname = 'ix_seasons_sota_season_id'")
    ).fetchone()
    if not idx_exists:
        op.create_index("ix_seasons_sota_season_id", "seasons", ["sota_season_id"])

    # 2. For SOTA-synced seasons: sota_season_id = id (backwards compatible)
    conn.execute(
        sa.text(
            "UPDATE seasons SET sota_season_id = id "
            "WHERE id NOT IN (200, 201)"
        )
    )

    # 3. Delete SOTA-created PL 2026 duplicate (season 173, empty)
    #    and assign its SOTA ID to our manual season 200
    conn.execute(
        sa.text("UPDATE seasons SET sota_season_id = 173 WHERE id = 200")
    )
    conn.execute(
        sa.text("DELETE FROM seasons WHERE id = 173")
    )

    # 4. Add season_participants for Supercup 2026
    for team_id in SUPERCUP_TEAM_IDS:
        conn.execute(
            sa.text(
                "INSERT INTO season_participants (team_id, season_id) "
                "VALUES (:team_id, :season_id) "
                "ON CONFLICT (team_id, season_id) DO NOTHING"
            ),
            {"team_id": team_id, "season_id": SUPERCUP_SEASON_ID},
        )


def downgrade() -> None:
    conn = op.get_bind()

    # Remove supercup participants
    for team_id in SUPERCUP_TEAM_IDS:
        conn.execute(
            sa.text(
                "DELETE FROM season_participants "
                "WHERE season_id = :season_id AND team_id = :team_id"
            ),
            {"season_id": SUPERCUP_SEASON_ID, "team_id": team_id},
        )

    # Drop column
    op.drop_index("ix_seasons_sota_season_id", table_name="seasons")
    op.drop_column("seasons", "sota_season_id")
