"""add source to game_events

Revision ID: c0d1e2f3g4h5
Revises: b9c0d1e2f3g4
Create Date: 2026-03-07 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "c0d1e2f3g4h5"
down_revision = ("13cbb481dd91", "z8d9e0f1g2h3")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "game_events",
        sa.Column("source", sa.String(16), nullable=False, server_default="sota"),
    )
    op.create_index(
        "ix_game_events_game_source", "game_events", ["game_id", "source"]
    )


def downgrade() -> None:
    op.drop_index("ix_game_events_game_source", table_name="game_events")
    op.drop_column("game_events", "source")
