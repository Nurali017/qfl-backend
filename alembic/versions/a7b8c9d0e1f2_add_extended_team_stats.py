"""add extended team stats columns

Revision ID: a7b8c9d0e1f2
Revises: z6b7c8d9e0f1
Create Date: 2026-02-28 13:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "a7b8c9d0e1f2"
down_revision = "z6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    existing = {
        row[0]
        for row in conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'game_team_stats'"
            )
        )
    }
    for col_name in ("shots_on_bar", "shots_blocked", "penalties", "saves"):
        if col_name not in existing:
            op.add_column(
                "game_team_stats", sa.Column(col_name, sa.Integer(), nullable=True)
            )


def downgrade() -> None:
    for col in ["saves", "penalties", "shots_blocked", "shots_on_bar"]:
        op.drop_column("game_team_stats", col)
