"""Add admin auth tables and translation grouping for CMS content.

Revision ID: r6s7t8u9v0w1
Revises: q5r6s7t8u9v0
Create Date: 2026-02-06
"""
from __future__ import annotations

from typing import Sequence, Union
import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "r6s7t8u9v0w1"
down_revision: Union[str, None] = "5c694885749b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "admin_users",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_admin_users_email", "admin_users", ["email"], unique=True)

    op.create_table(
        "admin_sessions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.Column("user_agent", sa.String(length=512), nullable=True),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["user_id"], ["admin_users.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_admin_sessions_user_id", "admin_sessions", ["user_id"], unique=False)
    op.create_index("ix_admin_sessions_refresh_token_hash", "admin_sessions", ["refresh_token_hash"], unique=True)

    op.add_column("news", sa.Column("translation_group_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("news", sa.Column("created_by_admin_id", sa.Integer(), nullable=True))
    op.add_column("news", sa.Column("updated_by_admin_id", sa.Integer(), nullable=True))

    op.add_column("pages", sa.Column("translation_group_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("pages", sa.Column("created_by_admin_id", sa.Integer(), nullable=True))
    op.add_column("pages", sa.Column("updated_by_admin_id", sa.Integer(), nullable=True))

    conn = op.get_bind()

    # Backfill news: pair by source_id when available, otherwise create standalone group.
    news_rows = conn.execute(sa.text("SELECT id, language, source_id FROM news ORDER BY id ASC")).fetchall()
    source_group_map: dict[int, str] = {}
    for row in news_rows:
        row_id = row[0]
        row_language = row[1]
        source_id = row[2]
        if source_id is not None:
            group_id = source_group_map.setdefault(source_id, str(uuid.uuid4()))
        else:
            group_id = str(uuid.uuid4())

        conn.execute(
            sa.text("UPDATE news SET translation_group_id = :group_id WHERE id = :row_id AND language = :language"),
            {"group_id": group_id, "row_id": row_id, "language": row_language},
        )

    # Backfill pages: pair known RU/KZ slug groups, otherwise standalone group.
    slug_pair_keys = {
        "kontakty": "contacts",
        "baylanystar": "contacts",
        "dokumenty": "documents",
        "kuzhattar": "documents",
        "rukovodstvo": "leadership",
        "basshylyk": "leadership",
    }
    page_rows = conn.execute(sa.text("SELECT id, slug FROM pages ORDER BY id ASC")).fetchall()
    pair_group_map: dict[str, str] = {}
    for row in page_rows:
        row_id = row[0]
        slug = (row[1] or "").lower()
        pair_key = slug_pair_keys.get(slug)

        if pair_key:
            group_id = pair_group_map.setdefault(pair_key, str(uuid.uuid4()))
        else:
            group_id = str(uuid.uuid4())

        conn.execute(
            sa.text("UPDATE pages SET translation_group_id = :group_id WHERE id = :row_id"),
            {"group_id": group_id, "row_id": row_id},
        )

    op.alter_column("news", "translation_group_id", nullable=False)
    op.alter_column("pages", "translation_group_id", nullable=False)

    op.create_index("ix_news_translation_group_id", "news", ["translation_group_id"], unique=False)
    op.create_index("ix_pages_translation_group_id", "pages", ["translation_group_id"], unique=False)

    op.create_unique_constraint(
        "uq_news_translation_group_language",
        "news",
        ["translation_group_id", "language"],
    )
    op.create_unique_constraint(
        "uq_pages_translation_group_language",
        "pages",
        ["translation_group_id", "language"],
    )

    op.create_foreign_key(
        "fk_news_created_by_admin_id",
        "news",
        "admin_users",
        ["created_by_admin_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_news_updated_by_admin_id",
        "news",
        "admin_users",
        ["updated_by_admin_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_foreign_key(
        "fk_pages_created_by_admin_id",
        "pages",
        "admin_users",
        ["created_by_admin_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pages_updated_by_admin_id",
        "pages",
        "admin_users",
        ["updated_by_admin_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_pages_updated_by_admin_id", "pages", type_="foreignkey")
    op.drop_constraint("fk_pages_created_by_admin_id", "pages", type_="foreignkey")
    op.drop_constraint("fk_news_updated_by_admin_id", "news", type_="foreignkey")
    op.drop_constraint("fk_news_created_by_admin_id", "news", type_="foreignkey")

    op.drop_constraint("uq_pages_translation_group_language", "pages", type_="unique")
    op.drop_constraint("uq_news_translation_group_language", "news", type_="unique")

    op.drop_index("ix_pages_translation_group_id", table_name="pages")
    op.drop_index("ix_news_translation_group_id", table_name="news")

    op.drop_column("pages", "updated_by_admin_id")
    op.drop_column("pages", "created_by_admin_id")
    op.drop_column("pages", "translation_group_id")

    op.drop_column("news", "updated_by_admin_id")
    op.drop_column("news", "created_by_admin_id")
    op.drop_column("news", "translation_group_id")

    op.drop_index("ix_admin_sessions_refresh_token_hash", table_name="admin_sessions")
    op.drop_index("ix_admin_sessions_user_id", table_name="admin_sessions")
    op.drop_table("admin_sessions")

    op.drop_index("ix_admin_users_email", table_name="admin_users")
    op.drop_table("admin_users")
