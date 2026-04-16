"""remove video_url from games and migrate review/replay links

Revision ID: aa9b8c7d6e5f
Revises: zx0y1z2a3b4c5
Create Date: 2026-04-16 00:00:00.000000
"""

from __future__ import annotations

import logging
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

from app.utils.youtube import extract_youtube_id


revision: str = "aa9b8c7d6e5f"
down_revision: Union[str, None] = "zx0y1z2a3b4c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger(__name__)


def upgrade() -> None:
    bind = op.get_bind()
    games = sa.table(
        "games",
        sa.column("id", sa.BigInteger()),
        sa.column("video_url", sa.String(length=500)),
        sa.column("youtube_live_url", sa.String(length=500)),
        sa.column("video_review_url", sa.String(length=500)),
    )

    rows = bind.execute(
        sa.select(
            games.c.id,
            games.c.video_url,
            games.c.youtube_live_url,
            games.c.video_review_url,
        )
    ).mappings().all()

    for row in rows:
        video_url = (row["video_url"] or "").strip()
        if not video_url:
            continue

        live_url = (row["youtube_live_url"] or "").strip()
        review_url = (row["video_review_url"] or "").strip()

        video_id = extract_youtube_id(video_url)
        live_id = extract_youtube_id(live_url) if live_url else None

        same_as_live = bool(live_url) and (
            (video_id is not None and live_id is not None and video_id == live_id)
            or video_url == live_url
        )

        if same_as_live:
            bind.execute(
                games.update()
                .where(games.c.id == row["id"])
                .values(video_url=None)
            )
            continue

        if review_url and review_url != video_url:
            logger.warning(
                "games.video_review_url collision for game_id=%s; replacing %s with %s",
                row["id"],
                review_url,
                video_url,
            )

        bind.execute(
            games.update()
            .where(games.c.id == row["id"])
            .values(video_review_url=video_url, video_url=None)
        )

    op.drop_column("games", "video_url")


def downgrade() -> None:
    bind = op.get_bind()
    op.add_column("games", sa.Column("video_url", sa.String(length=500), nullable=True))
    bind.execute(
        sa.text(
            """
            UPDATE games
            SET video_url = video_review_url
            WHERE video_review_url IS NOT NULL
            """
        )
    )
