"""Add game_live_inputs table for live-stream ingest endpoints.

Revision ID: dd1e2f3a4b5c
Revises: cc7d8e9f0a1b
Create Date: 2026-04-19
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "dd1e2f3a4b5c"
down_revision: Union[str, None] = "cc7d8e9f0a1b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "game_live_inputs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("game_id", sa.BigInteger(), sa.ForeignKey("games.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("provider", sa.String(length=32), nullable=False, server_default="cloudflare"),
        sa.Column("provider_input_uid", sa.String(length=100), nullable=False, unique=True),
        sa.Column("srt_url", sa.Text(), nullable=False),
        sa.Column("srt_passphrase", sa.Text(), nullable=True),
        sa.Column("srt_stream_id", sa.Text(), nullable=True),
        sa.Column("rtmp_url", sa.Text(), nullable=False),
        sa.Column("rtmp_stream_key", sa.Text(), nullable=False),
        sa.Column("playback_hls_url", sa.Text(), nullable=False),
        sa.Column("playback_dash_url", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("pending", "live", "ended", "failed", name="game_live_input_status"),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_game_live_inputs_game_id", "game_live_inputs", ["game_id"])
    op.create_index("ix_game_live_inputs_provider_input_uid", "game_live_inputs", ["provider_input_uid"])
    op.create_index("ix_game_live_inputs_status", "game_live_inputs", ["status"])


def downgrade() -> None:
    op.drop_index("ix_game_live_inputs_status", table_name="game_live_inputs")
    op.drop_index("ix_game_live_inputs_provider_input_uid", table_name="game_live_inputs")
    op.drop_index("ix_game_live_inputs_game_id", table_name="game_live_inputs")
    op.drop_table("game_live_inputs")
    sa.Enum(name="game_live_input_status").drop(op.get_bind(), checkfirst=True)
