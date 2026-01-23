"""add_game_events_and_live_fields

Revision ID: f4a5b6c7d8e9
Revises: e3f4a5b6c7d8
Create Date: 2026-01-19 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'f4a5b6c7d8e9'
down_revision: Union[str, None] = 'e3f4a5b6c7d8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Define enum type for game events
gameeventtype_enum = postgresql.ENUM(
    'goal', 'assist', 'yellow_card', 'red_card', 'substitution',
    name='gameeventtype',
    create_type=False
)


def upgrade() -> None:
    # Create enum type first
    gameeventtype_enum.create(op.get_bind(), checkfirst=True)

    # Add new columns to games table for live tracking
    op.add_column('games', sa.Column('is_live', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('games', sa.Column('home_formation', sa.String(length=20), nullable=True))
    op.add_column('games', sa.Column('away_formation', sa.String(length=20), nullable=True))

    # Create game_events table
    op.create_table('game_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('game_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('half', sa.Integer(), nullable=False),
        sa.Column('minute', sa.Integer(), nullable=False),
        sa.Column('event_type', gameeventtype_enum, nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=True),
        sa.Column('team_name', sa.String(length=255), nullable=True),
        sa.Column('player_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('player_number', sa.Integer(), nullable=True),
        sa.Column('player_name', sa.String(length=255), nullable=True),
        sa.Column('player2_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('player2_number', sa.Integer(), nullable=True),
        sa.Column('player2_name', sa.String(length=255), nullable=True),
        sa.Column('player2_team_name', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['game_id'], ['games.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['player_id'], ['players.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['player2_id'], ['players.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for efficient querying
    op.create_index('ix_game_events_game_id', 'game_events', ['game_id'], unique=False)
    op.create_index('ix_game_events_game_minute', 'game_events', ['game_id', 'half', 'minute'], unique=False)

    # Create index on games.is_live for finding active games
    op.create_index('ix_games_is_live', 'games', ['is_live'], unique=False)


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_games_is_live', table_name='games')
    op.drop_index('ix_game_events_game_minute', table_name='game_events')
    op.drop_index('ix_game_events_game_id', table_name='game_events')

    # Drop game_events table
    op.drop_table('game_events')

    # Drop columns from games table
    op.drop_column('games', 'away_formation')
    op.drop_column('games', 'home_formation')
    op.drop_column('games', 'is_live')

    # Drop enum type
    gameeventtype_enum.drop(op.get_bind(), checkfirst=True)
