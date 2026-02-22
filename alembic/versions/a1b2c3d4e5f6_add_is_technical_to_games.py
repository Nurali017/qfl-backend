"""add is_technical to games

Revision ID: a1b2c3d4e5f6
Revises: z1a2b3c4d5e6
Create Date: 2026-02-22 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'i0j1k2l3m4n5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('games', sa.Column('is_technical', sa.Boolean(), nullable=False, server_default=sa.text('false')))


def downgrade() -> None:
    op.drop_column('games', 'is_technical')
