"""rename stats columns to SOTA keys

Revision ID: c61c9ab55ff7
Revises: z9e0f1g2h3i4
Create Date: 2026-03-12

PostgreSQL ALTER COLUMN RENAME is an instant metadata-only operation — no data is rewritten.
"""
from alembic import op

revision = "c61c9ab55ff7"
down_revision = "z9e0f1g2h3i4"
branch_labels = None
depends_on = None


# PlayerSeasonStats renames: (old_column, new_column)
PLAYER_RENAMES = [
    ("goals", "goal"),
    ("assists", "goal_pass"),
    ("minutes_played", "time_on_field_total"),
    ("shots", "shot"),
    ("pass_accuracy", "pass_ratio"),
    ("key_passes", "key_pass"),
    ("duels", "duel"),
    ("duels_won", "duel_success"),
]

# TeamSeasonStats renames: (old_column, new_column)
TEAM_RENAMES = [
    ("wins", "win"),
    ("draws", "draw"),
    ("losses", "match_loss"),
    ("goals_scored", "goal"),
    ("shots", "shot"),
    ("possession_avg", "possession_percent_average"),
    ("pass_accuracy_avg", "pass_ratio"),
    ("fouls", "foul"),
    ("corners", "corner"),
    ("offsides", "offside"),
]

# Index renames: (old_index_name, new_index_name)
INDEX_RENAMES = [
    ("ix_player_season_stats_goals", "ix_player_season_stats_goal"),
    ("ix_player_season_stats_assists", "ix_player_season_stats_goal_pass"),
    ("ix_player_season_stats_minutes", "ix_player_season_stats_time_on_field_total"),
    ("ix_team_season_stats_goals_scored", "ix_team_season_stats_goal"),
]


def upgrade() -> None:
    for old, new in PLAYER_RENAMES:
        op.alter_column("player_season_stats", old, new_column_name=new)

    for old, new in TEAM_RENAMES:
        op.alter_column("team_season_stats", old, new_column_name=new)

    for old, new in INDEX_RENAMES:
        op.execute(f'ALTER INDEX IF EXISTS "{old}" RENAME TO "{new}"')


def downgrade() -> None:
    for old, new in PLAYER_RENAMES:
        op.alter_column("player_season_stats", new, new_column_name=old)

    for old, new in TEAM_RENAMES:
        op.alter_column("team_season_stats", new, new_column_name=old)

    for old, new in INDEX_RENAMES:
        op.execute(f'ALTER INDEX IF EXISTS "{new}" RENAME TO "{old}"')
