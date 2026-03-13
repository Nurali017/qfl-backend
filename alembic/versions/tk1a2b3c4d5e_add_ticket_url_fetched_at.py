"""add ticket_url_fetched_at to games (merge heads)

Revision ID: tk1a2b3c4d5e
Revises: c61c9ab55ff7, ft2b3c4d5e6f7
Create Date: 2026-03-12 18:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "tk1a2b3c4d5e"
down_revision = ("c61c9ab55ff7", "ft2b3c4d5e6f7")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("games", sa.Column("ticket_url_fetched_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("games", "ticket_url_fetched_at")
