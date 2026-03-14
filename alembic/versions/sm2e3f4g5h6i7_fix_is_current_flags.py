"""Fix is_current flags for Cup 2026, 1L 2026, 2L 2026 seasons.

Mark new seasons as current and unmark old conflicting ones so that
_pick_current_season() in the front-map endpoint returns the 2026 seasons.

Revision ID: sm2e3f4g5h6i7
Revises: sl1d2e3r4i5x6
Create Date: 2026-03-14 22:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "sm2e3f4g5h6i7"
down_revision = "sl1d2e3r4i5x6"
branch_labels = None
depends_on = None

# New 2026 season IDs
CUP_SEASON_ID = 202
LEAGUE2_SEASON_ID = 203
LEAGUE1_SEASON_ID = 204

# Old season IDs that have is_current=true or NULL dates causing conflicts
OLD_CONFLICTING_IDS = (1, 71, 80, 81, 85, 157)


def upgrade() -> None:
    bind = op.get_bind()

    # Mark new 2026 seasons as current
    for sid in (CUP_SEASON_ID, LEAGUE2_SEASON_ID, LEAGUE1_SEASON_ID):
        bind.execute(
            sa.text("UPDATE seasons SET is_current = true WHERE id = :sid"),
            {"sid": sid},
        )

    # Unmark old seasons that conflict
    ids_list = ", ".join(str(i) for i in OLD_CONFLICTING_IDS)
    bind.execute(
        sa.text(
            f"UPDATE seasons SET is_current = false "
            f"WHERE id IN ({ids_list}) AND is_current = true"
        ),
    )

    # Seed score_table for league seasons so tables show teams even with 0 games
    for sid in (LEAGUE2_SEASON_ID, LEAGUE1_SEASON_ID):
        bind.execute(
            sa.text("""
                INSERT INTO score_table
                    (season_id, team_id, position,
                     games_played, wins, draws, losses,
                     goals_scored, goals_conceded, goal_difference,
                     points, updated_at)
                SELECT :sid, sp.team_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY sp.group_name
                           ORDER BY sp.team_id
                       ),
                       0, 0, 0, 0, 0, 0, 0, 0, NOW()
                FROM season_participants sp
                WHERE sp.season_id = :sid
            """),
            {"sid": sid},
        )


def downgrade() -> None:
    bind = op.get_bind()

    # Remove score_table entries for league seasons
    for sid in (LEAGUE2_SEASON_ID, LEAGUE1_SEASON_ID):
        bind.execute(
            sa.text("DELETE FROM score_table WHERE season_id = :sid"),
            {"sid": sid},
        )

    # Restore old seasons as current
    ids_list = ", ".join(str(i) for i in OLD_CONFLICTING_IDS)
    bind.execute(
        sa.text(
            f"UPDATE seasons SET is_current = true "
            f"WHERE id IN ({ids_list})"
        ),
    )

    # Remove current flag from new seasons
    for sid in (CUP_SEASON_ID, LEAGUE2_SEASON_ID, LEAGUE1_SEASON_ID):
        bind.execute(
            sa.text("UPDATE seasons SET is_current = false WHERE id = :sid"),
            {"sid": sid},
        )
