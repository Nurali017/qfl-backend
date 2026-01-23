"""Add player_season_stats table

Revision ID: 004
Revises: 003
Create Date: 2025-01-15

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: Union[str, None] = '003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'player_season_stats',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('player_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=True),
        # Basic stats
        sa.Column('games_played', sa.Integer(), nullable=True),
        sa.Column('games_starting', sa.Integer(), nullable=True),
        sa.Column('minutes_played', sa.Integer(), nullable=True),
        # Goals & Assists
        sa.Column('goals', sa.Integer(), nullable=True),
        sa.Column('assists', sa.Integer(), nullable=True),
        sa.Column('xg', sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column('xg_per_90', sa.Numeric(precision=4, scale=2), nullable=True),
        # Shots
        sa.Column('shots', sa.Integer(), nullable=True),
        sa.Column('shots_on_goal', sa.Integer(), nullable=True),
        # Passes
        sa.Column('passes', sa.Integer(), nullable=True),
        sa.Column('pass_accuracy', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('key_passes', sa.Integer(), nullable=True),
        # Duels
        sa.Column('duels', sa.Integer(), nullable=True),
        sa.Column('duels_won', sa.Integer(), nullable=True),
        # Discipline
        sa.Column('yellow_cards', sa.Integer(), nullable=True),
        sa.Column('red_cards', sa.Integer(), nullable=True),
        # Extra stats (50+ metrics from v2)
        sa.Column('extra_stats', postgresql.JSONB(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        # Constraints
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['player_id'], ['players.id']),
        sa.ForeignKeyConstraint(['season_id'], ['seasons.id']),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id']),
        sa.UniqueConstraint('player_id', 'season_id', name='uq_player_season_stats'),
    )
    op.create_index('ix_player_season_stats_player_id', 'player_season_stats', ['player_id'])
    op.create_index('ix_player_season_stats_season_id', 'player_season_stats', ['season_id'])
    op.create_index('ix_player_season_stats_team_id', 'player_season_stats', ['team_id'])


def downgrade() -> None:
    op.drop_index('ix_player_season_stats_team_id')
    op.drop_index('ix_player_season_stats_season_id')
    op.drop_index('ix_player_season_stats_player_id')
    op.drop_table('player_season_stats')
