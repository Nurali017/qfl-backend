"""add_missing_indexes

Revision ID: b5c6d7e8f9g0
Revises: a4b5c6d7e8f9
Create Date: 2026-02-26

Adds missing FK indexes and composite indexes across the schema.
Also cleans up duplicate/redundant indexes on the games table.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b5c6d7e8f9g0'
down_revision: Union[str, None] = 'a4b5c6d7e8f9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── CRITICAL: games table ──────────────────────────────────────
    # FK indexes for team columns (used in every team-specific query)
    op.create_index('ix_games_home_team_id', 'games', ['home_team_id'])
    op.create_index('ix_games_away_team_id', 'games', ['away_team_id'])

    # Composite (season_id, date, time) for ORDER BY date, time after season filter.
    # This makes the existing (season_id, date) composites redundant.
    op.create_index(
        'ix_games_season_date_time', 'games',
        ['season_id', 'date', 'time']
    )

    # Drop redundant (season_id, date) composites — covered by new 3-column index
    op.drop_index('ix_games_season_date', table_name='games')
    op.execute("DROP INDEX IF EXISTS idx_games_season_date")

    # Drop redundant full is_live index — keep only the partial idx_games_is_live
    op.drop_index('ix_games_is_live', table_name='games')

    # ── HIGH: season_participants ──────────────────────────────────
    # Unique constraint (team_id, season_id) has team_id as leading column,
    # so queries filtering by season_id alone need a separate index.
    op.create_index(
        'ix_season_participants_season_id', 'season_participants',
        ['season_id']
    )

    # ── HIGH: stages ───────────────────────────────────────────────
    op.create_index('ix_stages_season_id', 'stages', ['season_id'])

    # ── HIGH: game_events ──────────────────────────────────────────
    # For filtering goals/assists by event_type per game
    op.create_index(
        'ix_game_events_game_type', 'game_events',
        ['game_id', 'event_type']
    )

    # ── MEDIUM: team_coaches ───────────────────────────────────────
    # Queries always filter by (team_id, season_id)
    op.create_index(
        'ix_team_coaches_team_season', 'team_coaches',
        ['team_id', 'season_id']
    )

    # ── MEDIUM: news ───────────────────────────────────────────────
    # Main listing query: WHERE language = ? ORDER BY publish_date DESC
    op.create_index(
        'ix_news_language_publish_date', 'news',
        ['language', 'publish_date']
    )

    # ── LOW: partners ──────────────────────────────────────────────
    op.create_index('ix_partners_championship_id', 'partners', ['championship_id'])
    op.create_index('ix_partners_season_id', 'partners', ['season_id'])

    # ── LOW: clubs ─────────────────────────────────────────────────
    op.create_index('ix_clubs_city_id', 'clubs', ['city_id'])
    op.create_index('ix_clubs_stadium_id', 'clubs', ['stadium_id'])


def downgrade() -> None:
    # LOW
    op.drop_index('ix_clubs_stadium_id', table_name='clubs')
    op.drop_index('ix_clubs_city_id', table_name='clubs')
    op.drop_index('ix_partners_season_id', table_name='partners')
    op.drop_index('ix_partners_championship_id', table_name='partners')

    # MEDIUM
    op.drop_index('ix_news_language_publish_date', table_name='news')
    op.drop_index('ix_team_coaches_team_season', table_name='team_coaches')

    # HIGH
    op.drop_index('ix_game_events_game_type', table_name='game_events')
    op.drop_index('ix_stages_season_id', table_name='stages')
    op.drop_index('ix_season_participants_season_id', table_name='season_participants')

    # Restore dropped indexes
    op.create_index('ix_games_is_live', 'games', ['is_live'])
    op.create_index('ix_games_season_date', 'games', ['season_id', 'date'])

    # CRITICAL
    op.drop_index('ix_games_season_date_time', table_name='games')
    op.drop_index('ix_games_away_team_id', table_name='games')
    op.drop_index('ix_games_home_team_id', table_name='games')
