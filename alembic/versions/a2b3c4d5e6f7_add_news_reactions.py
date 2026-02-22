"""add_news_reactions

Revision ID: a2b3c4d5e6f7
Revises: z1a2b3c4d5e6
Create Date: 2026-02-21

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a2b3c4d5e6f7"
down_revision: Union[str, None] = "z1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add counters to news table
    op.add_column("news", sa.Column("views_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("news", sa.Column("likes_count", sa.Integer(), nullable=False, server_default="0"))

    # Create news_likes table
    # Note: no FK to news because news PK is composite (id, language) and news.id
    # is not unique (same id exists for each language). Integrity enforced at app level.
    op.create_table(
        "news_likes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("news_id", sa.Integer(), nullable=False),
        sa.Column("client_ip", sa.String(45), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("news_id", "client_ip", name="uq_news_likes_news_id_client_ip"),
    )
    op.create_index("ix_news_likes_news_id", "news_likes", ["news_id"])


def downgrade() -> None:
    op.drop_index("ix_news_likes_news_id", table_name="news_likes")
    op.drop_table("news_likes")
    op.drop_column("news", "likes_count")
    op.drop_column("news", "views_count")
