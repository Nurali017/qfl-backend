"""Add team_season_stats table

Revision ID: 003
Revises: 002
Create Date: 2025-01-15

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '003'
down_revision: Union[str, None] = '002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'team_season_stats',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=False),
        # Basic stats
        sa.Column('games_played', sa.Integer(), nullable=True),
        sa.Column('wins', sa.Integer(), nullable=True),
        sa.Column('draws', sa.Integer(), nullable=True),
        sa.Column('losses', sa.Integer(), nullable=True),
        sa.Column('goals_scored', sa.Integer(), nullable=True),
        sa.Column('goals_conceded', sa.Integer(), nullable=True),
        sa.Column('points', sa.Integer(), nullable=True),
        # Detailed stats
        sa.Column('shots', sa.Integer(), nullable=True),
        sa.Column('shots_on_goal', sa.Integer(), nullable=True),
        sa.Column('possession_avg', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('passes', sa.Integer(), nullable=True),
        sa.Column('pass_accuracy_avg', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('fouls', sa.Integer(), nullable=True),
        sa.Column('yellow_cards', sa.Integer(), nullable=True),
        sa.Column('red_cards', sa.Integer(), nullable=True),
        sa.Column('corners', sa.Integer(), nullable=True),
        sa.Column('offsides', sa.Integer(), nullable=True),
        # Extra
        sa.Column('extra_stats', postgresql.JSONB(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        # Constraints
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id']),
        sa.ForeignKeyConstraint(['season_id'], ['seasons.id']),
        sa.UniqueConstraint('team_id', 'season_id', name='uq_team_season_stats'),
    )
    op.create_index('ix_team_season_stats_team_id', 'team_season_stats', ['team_id'])
    op.create_index('ix_team_season_stats_season_id', 'team_season_stats', ['season_id'])


def downgrade() -> None:
    op.drop_index('ix_team_season_stats_season_id')
    op.drop_index('ix_team_season_stats_team_id')
    op.drop_table('team_season_stats')
