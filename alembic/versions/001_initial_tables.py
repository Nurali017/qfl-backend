"""Initial tables

Revision ID: 001
Revises:
Create Date: 2025-01-14

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Tournaments
    op.create_table(
        'tournaments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('country_code', sa.String(length=10), nullable=True),
        sa.Column('country_name', sa.String(length=100), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Seasons
    op.create_table(
        'seasons',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('tournament_id', sa.Integer(), nullable=True),
        sa.Column('date_start', sa.Date(), nullable=True),
        sa.Column('date_end', sa.Date(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['tournament_id'], ['tournaments.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Teams
    op.create_table(
        'teams',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('name_en', sa.String(length=255), nullable=True),
        sa.Column('logo_url', sa.Text(), nullable=True),
        sa.Column('logo_updated_at', sa.DateTime(), nullable=True),
        sa.Column('city', sa.String(length=100), nullable=True),
        sa.Column('city_en', sa.String(length=100), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Players
    op.create_table(
        'players',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('first_name', sa.String(length=100), nullable=True),
        sa.Column('last_name', sa.String(length=100), nullable=True),
        sa.Column('birthday', sa.Date(), nullable=True),
        sa.Column('player_type', sa.String(length=50), nullable=True),
        sa.Column('country_name', sa.String(length=100), nullable=True),
        sa.Column('country_code', sa.String(length=10), nullable=True),
        sa.Column('photo_url', sa.Text(), nullable=True),
        sa.Column('age', sa.Integer(), nullable=True),
        sa.Column('top_role', sa.String(length=100), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Player Teams
    op.create_table(
        'player_teams',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('player_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=False),
        sa.Column('number', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['player_id'], ['players.id'], ),
        sa.ForeignKeyConstraint(['season_id'], ['seasons.id'], ),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('player_id', 'team_id', 'season_id', name='uq_player_team_season')
    )

    # Games
    op.create_table(
        'games',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('time', sa.Time(), nullable=True),
        sa.Column('tour', sa.Integer(), nullable=True),
        sa.Column('season_id', sa.Integer(), nullable=True),
        sa.Column('home_team_id', sa.Integer(), nullable=True),
        sa.Column('away_team_id', sa.Integer(), nullable=True),
        sa.Column('home_score', sa.Integer(), nullable=True),
        sa.Column('away_score', sa.Integer(), nullable=True),
        sa.Column('has_stats', sa.Boolean(), nullable=False, default=False),
        sa.Column('stadium', sa.String(length=255), nullable=True),
        sa.Column('visitors', sa.Integer(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['away_team_id'], ['teams.id'], ),
        sa.ForeignKeyConstraint(['home_team_id'], ['teams.id'], ),
        sa.ForeignKeyConstraint(['season_id'], ['seasons.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Game Team Stats
    op.create_table(
        'game_team_stats',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('game_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('possession', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('possession_percent', sa.Integer(), nullable=True),
        sa.Column('shots', sa.Integer(), nullable=True),
        sa.Column('shots_on_goal', sa.Integer(), nullable=True),
        sa.Column('passes', sa.Integer(), nullable=True),
        sa.Column('pass_accuracy', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('fouls', sa.Integer(), nullable=True),
        sa.Column('yellow_cards', sa.Integer(), nullable=True),
        sa.Column('red_cards', sa.Integer(), nullable=True),
        sa.Column('corners', sa.Integer(), nullable=True),
        sa.Column('offsides', sa.Integer(), nullable=True),
        sa.Column('extra_stats', postgresql.JSONB(), nullable=True),
        sa.ForeignKeyConstraint(['game_id'], ['games.id'], ),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('game_id', 'team_id', name='uq_game_team_stats')
    )

    # Game Player Stats
    op.create_table(
        'game_player_stats',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('game_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('player_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('minutes_played', sa.Integer(), nullable=True),
        sa.Column('started', sa.Boolean(), nullable=True),
        sa.Column('position', sa.String(length=20), nullable=True),
        sa.Column('goals', sa.Integer(), nullable=False, default=0),
        sa.Column('assists', sa.Integer(), nullable=False, default=0),
        sa.Column('shots', sa.Integer(), nullable=False, default=0),
        sa.Column('passes', sa.Integer(), nullable=False, default=0),
        sa.Column('pass_accuracy', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('yellow_cards', sa.Integer(), nullable=False, default=0),
        sa.Column('red_cards', sa.Integer(), nullable=False, default=0),
        sa.Column('extra_stats', postgresql.JSONB(), nullable=True),
        sa.ForeignKeyConstraint(['game_id'], ['games.id'], ),
        sa.ForeignKeyConstraint(['player_id'], ['players.id'], ),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('game_id', 'player_id', name='uq_game_player_stats')
    )

    # Score Table
    op.create_table(
        'score_table',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('position', sa.Integer(), nullable=True),
        sa.Column('games_played', sa.Integer(), nullable=True),
        sa.Column('wins', sa.Integer(), nullable=True),
        sa.Column('draws', sa.Integer(), nullable=True),
        sa.Column('losses', sa.Integer(), nullable=True),
        sa.Column('goals_scored', sa.Integer(), nullable=True),
        sa.Column('goals_conceded', sa.Integer(), nullable=True),
        sa.Column('goal_difference', sa.Integer(), nullable=True),
        sa.Column('points', sa.Integer(), nullable=True),
        sa.Column('form', sa.String(length=20), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['season_id'], ['seasons.id'], ),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('season_id', 'team_id', name='uq_score_table_season_team')
    )

    # Create indexes for better query performance
    op.create_index('ix_games_season_id', 'games', ['season_id'])
    op.create_index('ix_games_date', 'games', ['date'])
    op.create_index('ix_player_teams_season_id', 'player_teams', ['season_id'])
    op.create_index('ix_player_teams_team_id', 'player_teams', ['team_id'])
    op.create_index('ix_game_player_stats_game_id', 'game_player_stats', ['game_id'])
    op.create_index('ix_game_player_stats_player_id', 'game_player_stats', ['player_id'])


def downgrade() -> None:
    op.drop_index('ix_game_player_stats_player_id')
    op.drop_index('ix_game_player_stats_game_id')
    op.drop_index('ix_player_teams_team_id')
    op.drop_index('ix_player_teams_season_id')
    op.drop_index('ix_games_date')
    op.drop_index('ix_games_season_id')

    op.drop_table('score_table')
    op.drop_table('game_player_stats')
    op.drop_table('game_team_stats')
    op.drop_table('games')
    op.drop_table('player_teams')
    op.drop_table('players')
    op.drop_table('teams')
    op.drop_table('seasons')
    op.drop_table('tournaments')
