"""remove_goals_assists_from_player_stats

Revision ID: k9f0g1h2i3j4
Revises: j8e9f0g1h2i3
Create Date: 2026-01-19 22:00:00.000000

Goals and assists are now calculated from game_events table
to maintain single source of truth and avoid data duplication.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'k9f0g1h2i3j4'
down_revision: Union[str, None] = 'j8e9f0g1h2i3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove goals and assists columns from game_player_stats
    # These values are now calculated from game_events table
    op.drop_index('ix_game_player_stats_goals', table_name='game_player_stats')
    op.drop_column('game_player_stats', 'goals')
    op.drop_column('game_player_stats', 'assists')


def downgrade() -> None:
    # Re-add goals and assists columns (data will be lost)
    op.add_column('game_player_stats', sa.Column('assists', sa.Integer(), nullable=True, server_default='0'))
    op.add_column('game_player_stats', sa.Column('goals', sa.Integer(), nullable=True, server_default='0'))
    op.create_index('ix_game_player_stats_goals', 'game_player_stats', ['goals'], unique=False)
