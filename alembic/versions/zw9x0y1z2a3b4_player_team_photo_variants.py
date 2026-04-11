"""Add photo variant columns to player_teams (avatar, leaderboard, player_page).

Revision ID: zw9x0y1z2a3b4
Revises: zv8w9x0y1z2a3
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa

revision = "zw9x0y1z2a3b4"
down_revision = "zv8w9x0y1z2a3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("player_teams", sa.Column("photo_url_avatar", sa.String(), nullable=True))
    op.add_column("player_teams", sa.Column("photo_url_leaderboard", sa.String(), nullable=True))
    op.add_column("player_teams", sa.Column("photo_url_player_page", sa.String(), nullable=True))


def downgrade():
    op.drop_column("player_teams", "photo_url_player_page")
    op.drop_column("player_teams", "photo_url_leaderboard")
    op.drop_column("player_teams", "photo_url_avatar")
