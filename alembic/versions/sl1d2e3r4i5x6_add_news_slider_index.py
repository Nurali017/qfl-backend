"""Add composite index for news slider query.

Revision ID: sl1d2e3r4i5x6
Revises: s1e2a3s4o5n6
Create Date: 2026-03-14 18:00:00.000000
"""

from alembic import op

revision = "sl1d2e3r4i5x6"
down_revision = "s1e2a3s4o5n6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_news_slider_query",
        "news",
        ["language", "is_slider", "slider_order", "publish_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_news_slider_query", table_name="news")
