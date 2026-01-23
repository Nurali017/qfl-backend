"""add_stadium_referee_coach_lineup_models

Revision ID: b9c964b6f5d2
Revises: 2e98f48bc279
Create Date: 2026-01-18 00:21:55.777631

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b9c964b6f5d2'
down_revision: Union[str, None] = '2e98f48bc279'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Define enum types
coachrole_enum = postgresql.ENUM('head_coach', 'assistant', 'goalkeeper_coach', 'fitness_coach', 'other', name='coachrole', create_type=False)
refereerole_enum = postgresql.ENUM('main', 'first_assistant', 'second_assistant', 'fourth_referee', 'var_main', 'var_assistant', 'match_inspector', name='refereerole', create_type=False)
lineuptype_enum = postgresql.ENUM('starter', 'substitute', name='lineuptype', create_type=False)


def upgrade() -> None:
    # Create enum types first
    coachrole_enum.create(op.get_bind(), checkfirst=True)
    refereerole_enum.create(op.get_bind(), checkfirst=True)
    lineuptype_enum.create(op.get_bind(), checkfirst=True)

    # Create stadiums table
    op.create_table('stadiums',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('name_kz', sa.String(length=255), nullable=True),
        sa.Column('name_en', sa.String(length=255), nullable=True),
        sa.Column('city', sa.String(length=100), nullable=True),
        sa.Column('city_kz', sa.String(length=100), nullable=True),
        sa.Column('city_en', sa.String(length=100), nullable=True),
        sa.Column('capacity', sa.Integer(), nullable=True),
        sa.Column('address', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )

    # Create referees table
    op.create_table('referees',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('first_name', sa.String(length=100), nullable=False),
        sa.Column('last_name', sa.String(length=100), nullable=False),
        sa.Column('first_name_kz', sa.String(length=100), nullable=True),
        sa.Column('last_name_kz', sa.String(length=100), nullable=True),
        sa.Column('first_name_en', sa.String(length=100), nullable=True),
        sa.Column('last_name_en', sa.String(length=100), nullable=True),
        sa.Column('country', sa.String(length=100), nullable=True),
        sa.Column('photo_url', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )

    # Create coaches table
    op.create_table('coaches',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('first_name', sa.String(length=100), nullable=False),
        sa.Column('last_name', sa.String(length=100), nullable=False),
        sa.Column('first_name_kz', sa.String(length=100), nullable=True),
        sa.Column('last_name_kz', sa.String(length=100), nullable=True),
        sa.Column('first_name_en', sa.String(length=100), nullable=True),
        sa.Column('last_name_en', sa.String(length=100), nullable=True),
        sa.Column('photo_url', sa.String(length=500), nullable=True),
        sa.Column('country', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )

    # Create team_coaches table
    op.create_table('team_coaches',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('coach_id', sa.Integer(), nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=True),
        sa.Column('role', coachrole_enum, nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('start_date', sa.DateTime(), nullable=True),
        sa.Column('end_date', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['coach_id'], ['coaches.id']),
        sa.ForeignKeyConstraint(['season_id'], ['seasons.id']),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id']),
        sa.PrimaryKeyConstraint('id')
    )

    # Create game_referees table
    op.create_table('game_referees',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('game_id', sa.UUID(), nullable=False),
        sa.Column('referee_id', sa.Integer(), nullable=False),
        sa.Column('role', refereerole_enum, nullable=False),
        sa.ForeignKeyConstraint(['game_id'], ['games.id']),
        sa.ForeignKeyConstraint(['referee_id'], ['referees.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('game_id', 'referee_id', 'role', name='uq_game_referee_role')
    )
    op.create_index('ix_game_referees_game_id', 'game_referees', ['game_id'], unique=False)
    op.create_index('ix_game_referees_referee_id', 'game_referees', ['referee_id'], unique=False)

    # Create game_lineups table
    op.create_table('game_lineups',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('game_id', sa.UUID(), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.Column('player_id', sa.UUID(), nullable=False),
        sa.Column('lineup_type', lineuptype_enum, nullable=False),
        sa.Column('shirt_number', sa.Integer(), nullable=True),
        sa.Column('is_captain', sa.Boolean(), nullable=False, server_default='false'),
        sa.ForeignKeyConstraint(['game_id'], ['games.id']),
        sa.ForeignKeyConstraint(['player_id'], ['players.id']),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('game_id', 'player_id', name='uq_game_lineup_player')
    )
    op.create_index('ix_game_lineup_game_team', 'game_lineups', ['game_id', 'team_id'], unique=False)
    op.create_index('ix_game_lineups_game_id', 'game_lineups', ['game_id'], unique=False)
    op.create_index('ix_game_lineups_player_id', 'game_lineups', ['player_id'], unique=False)
    op.create_index('ix_game_lineups_team_id', 'game_lineups', ['team_id'], unique=False)

    # Add columns to games table
    op.add_column('games', sa.Column('has_lineup', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('games', sa.Column('stadium_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_games_stadium_id', 'games', 'stadiums', ['stadium_id'], ['id'])


def downgrade() -> None:
    # Drop foreign key and columns from games
    op.drop_constraint('fk_games_stadium_id', 'games', type_='foreignkey')
    op.drop_column('games', 'stadium_id')
    op.drop_column('games', 'has_lineup')

    # Drop game_lineups
    op.drop_index('ix_game_lineups_team_id', table_name='game_lineups')
    op.drop_index('ix_game_lineups_player_id', table_name='game_lineups')
    op.drop_index('ix_game_lineups_game_id', table_name='game_lineups')
    op.drop_index('ix_game_lineup_game_team', table_name='game_lineups')
    op.drop_table('game_lineups')
    op.execute("DROP TYPE lineuptype")

    # Drop game_referees
    op.drop_index('ix_game_referees_referee_id', table_name='game_referees')
    op.drop_index('ix_game_referees_game_id', table_name='game_referees')
    op.drop_table('game_referees')
    op.execute("DROP TYPE refereerole")

    # Drop team_coaches
    op.drop_table('team_coaches')
    op.execute("DROP TYPE coachrole")

    # Drop coaches
    op.drop_table('coaches')

    # Drop referees
    op.drop_table('referees')

    # Drop stadiums
    op.drop_table('stadiums')
