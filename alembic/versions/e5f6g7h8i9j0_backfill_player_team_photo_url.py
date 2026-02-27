"""backfill player_teams.photo_url from players.photo_url

When photo_url was added to player_teams (gap_analysis migration), existing rows
received NULL. This migration copies Player.photo_url → PlayerTeam.photo_url for
all contracts that have no contract-specific photo but the player has one.

Revision ID: e5f6g7h8i9j0
Revises: a5b6c7d8e9f0
Create Date: 2026-02-27 12:00:00.000000
"""

from alembic import op


revision = "e5f6g7h8i9j0"
down_revision = "a5b6c7d8e9f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE player_teams
        SET photo_url = players.photo_url
        FROM players
        WHERE player_teams.player_id = players.id
          AND player_teams.photo_url IS NULL
          AND players.photo_url IS NOT NULL
    """)


def downgrade() -> None:
    # Cannot safely undo — would require knowing which values were backfilled vs set manually.
    pass
