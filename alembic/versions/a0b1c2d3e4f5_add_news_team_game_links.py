"""add news_teams and news_games link tables

Revision ID: a0b1c2d3e4f5
Revises: z4a5b6c7d8e9
Create Date: 2026-02-28 12:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = 'a0b1c2d3e4f5'
down_revision = 'z4a5b6c7d8e9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'news_teams',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('translation_group_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('team_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['team_id'], ['teams.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('translation_group_id', 'team_id', name='uq_news_teams'),
    )
    op.create_index('ix_news_teams_group', 'news_teams', ['translation_group_id'])
    op.create_index('ix_news_teams_team', 'news_teams', ['team_id'])

    op.create_table(
        'news_games',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('translation_group_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('game_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['game_id'], ['games.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('translation_group_id', 'game_id', name='uq_news_games'),
    )
    op.create_index('ix_news_games_group', 'news_games', ['translation_group_id'])
    op.create_index('ix_news_games_game', 'news_games', ['game_id'])


def downgrade():
    op.drop_index('ix_news_games_game', table_name='news_games')
    op.drop_index('ix_news_games_group', table_name='news_games')
    op.drop_table('news_games')
    op.drop_index('ix_news_teams_team', table_name='news_teams')
    op.drop_index('ix_news_teams_group', table_name='news_teams')
    op.drop_table('news_teams')
