"""merge second league seasons 80+81+157 into 80

Revision ID: i0j1k2l3m4n5
Revises: h9i0j1k2l3m4
Create Date: 2026-02-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "i0j1k2l3m4n5"
down_revision: Union[str, None] = "h9i0j1k2l3m4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Step 1: Mark existing season 80 participants as group A
    conn.execute(
        sa.text(
            "UPDATE season_participants SET group_name = 'A' "
            "WHERE season_id = 80 AND (group_name IS NULL OR group_name = '')"
        )
    )

    # Step 2: Move season 81 participants to season 80 as group B
    # First, find team_ids that already exist in season 80
    existing_teams_80 = conn.execute(
        sa.text("SELECT team_id FROM season_participants WHERE season_id = 80")
    ).fetchall()
    existing_team_ids = {row[0] for row in existing_teams_80}

    # Get participants from season 81
    participants_81 = conn.execute(
        sa.text("SELECT id, team_id FROM season_participants WHERE season_id = 81")
    ).fetchall()

    for row in participants_81:
        pid, team_id = row
        if team_id in existing_team_ids:
            # Conflict: team already in season 80, delete the duplicate from 81
            conn.execute(
                sa.text("DELETE FROM season_participants WHERE id = :pid"),
                {"pid": pid},
            )
        else:
            # Move to season 80 as group B
            conn.execute(
                sa.text(
                    "UPDATE season_participants SET season_id = 80, group_name = 'B' "
                    "WHERE id = :pid"
                ),
                {"pid": pid},
            )
            existing_team_ids.add(team_id)

    # Step 3: Move season 157 (Final) participants to season 80
    participants_157 = conn.execute(
        sa.text("SELECT id, team_id FROM season_participants WHERE season_id = 157")
    ).fetchall()

    for row in participants_157:
        pid, team_id = row
        if team_id in existing_team_ids:
            conn.execute(
                sa.text("DELETE FROM season_participants WHERE id = :pid"),
                {"pid": pid},
            )
        else:
            conn.execute(
                sa.text(
                    "UPDATE season_participants SET season_id = 80 "
                    "WHERE id = :pid"
                ),
                {"pid": pid},
            )
            existing_team_ids.add(team_id)

    # Step 4: Move stages from 81 and 157 to season 80
    conn.execute(
        sa.text("UPDATE stages SET season_id = 80 WHERE season_id IN (81, 157)")
    )

    # Step 5: Move games from 81 and 157 to season 80
    conn.execute(
        sa.text("UPDATE games SET season_id = 80 WHERE season_id IN (81, 157)")
    )

    # Step 6: Delete aggregated data (will be recalculated)
    conn.execute(
        sa.text("DELETE FROM score_table WHERE season_id IN (81, 157)")
    )
    conn.execute(
        sa.text("DELETE FROM team_season_stats WHERE season_id IN (81, 157)")
    )
    conn.execute(
        sa.text("DELETE FROM player_season_stats WHERE season_id IN (81, 157)")
    )

    # Step 7: Move remaining FK references from 81/157 to 80
    conn.execute(
        sa.text("UPDATE player_teams SET season_id = 80 WHERE season_id IN (81, 157)")
    )
    conn.execute(
        sa.text("UPDATE team_coaches SET season_id = 80 WHERE season_id IN (81, 157)")
    )
    conn.execute(
        sa.text("UPDATE playoff_brackets SET season_id = 80 WHERE season_id IN (81, 157)")
    )
    conn.execute(
        sa.text("UPDATE partners SET season_id = 80 WHERE season_id IN (81, 157)")
    )
    conn.execute(
        sa.text("UPDATE team_of_week SET season_id = 80 WHERE season_id IN (81, 157)")
    )

    # Step 8: Delete seasons 81 and 157
    conn.execute(
        sa.text("DELETE FROM seasons WHERE id IN (81, 157)")
    )


def downgrade() -> None:
    # This migration is not reversible - the data merge is a one-way operation.
    # To undo, restore from database backup.
    pass
