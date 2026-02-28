"""add broadcasters and game_broadcasters tables

Revision ID: f1e2d3c4b5a6
Revises: d8e9f0a1b2c3
Create Date: 2026-02-28 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = 'f1e2d3c4b5a6'
down_revision = 'd8e9f0a1b2c3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'broadcasters',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('logo_url', sa.Text(), nullable=True),
        sa.Column('type', sa.String(20), nullable=True),
        sa.Column('website', sa.String(500), nullable=True),
        sa.Column('sort_order', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_broadcasters_sort_order', 'broadcasters', ['sort_order'])

    op.create_table(
        'game_broadcasters',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('game_id', sa.BigInteger(), nullable=False),
        sa.Column('broadcaster_id', sa.Integer(), nullable=False),
        sa.Column('sort_order', sa.Integer(), nullable=False, server_default='0'),
        sa.ForeignKeyConstraint(['game_id'], ['games.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['broadcaster_id'], ['broadcasters.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('game_id', 'broadcaster_id', name='uq_game_broadcaster'),
    )
    op.create_index('ix_game_broadcasters_game_id', 'game_broadcasters', ['game_id'])


def downgrade():
    op.drop_index('ix_game_broadcasters_game_id', table_name='game_broadcasters')
    op.drop_table('game_broadcasters')
    op.drop_index('ix_broadcasters_sort_order', table_name='broadcasters')
    op.drop_table('broadcasters')
