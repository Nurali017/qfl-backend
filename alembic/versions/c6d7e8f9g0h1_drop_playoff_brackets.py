"""drop playoff_brackets table

Revision ID: c6d7e8f9g0h1
Revises: b4c5d6e7f8g9
Create Date: 2026-02-23 02:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c6d7e8f9g0h1"
down_revision = "b4c5d6e7f8g9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "playoff_brackets" in inspector.get_table_names():
        op.drop_table("playoff_brackets")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "playoff_brackets" in inspector.get_table_names():
        return

    op.create_table(
        "playoff_brackets",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("legacy_id", sa.Integer(), nullable=True, unique=True, index=True),
        sa.Column("season_id", sa.Integer(), sa.ForeignKey("seasons.id"), nullable=False),
        sa.Column("round_name", sa.String(length=50), nullable=False),
        sa.Column("side", sa.String(length=10), nullable=False, server_default="left"),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "game_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            sa.ForeignKey("games.id"),
            nullable=True,
        ),
        sa.Column("is_visible", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("is_third_place", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

