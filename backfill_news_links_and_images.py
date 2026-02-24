"""Backfill news links and media URLs.

Default mode is dry-run. Use --apply to persist updates.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import News
from app.utils.news_html_normalizer import (
    normalize_news_html_content,
    normalize_news_media_url,
)


def _sample_value(value: str | None, limit: int = 220) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


async def run(*, apply: bool, limit: int | None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "dry_run": not apply,
        "rows_scanned": 0,
        "rows_changed": 0,
        "image_url_updates": 0,
        "content_updates": 0,
        "links_normalized": 0,
        "external_links_updated": 0,
        "unsafe_links_removed": 0,
        "src_normalized": 0,
        "unsafe_src_removed": 0,
        "player_links_rewritten": 0,
        "team_links_rewritten": 0,
        "examples": [],
    }

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(News).order_by(News.id.asc(), News.language.asc()))
        rows = result.scalars().all()
        if limit is not None:
            rows = rows[:limit]

        for row in rows:
            summary["rows_scanned"] += 1

            normalized_image_url = normalize_news_media_url(
                row.image_url,
                source_url=row.source_url,
            )
            normalized_content = await normalize_news_html_content(
                row.content,
                source_url=row.source_url,
                db=session,
            )

            image_changed = normalized_image_url != row.image_url
            content_changed = normalized_content.content != row.content
            row_changed = image_changed or content_changed

            if image_changed:
                summary["image_url_updates"] += 1
            if content_changed:
                summary["content_updates"] += 1

            summary["links_normalized"] += normalized_content.links_normalized
            summary["external_links_updated"] += normalized_content.external_links_updated
            summary["unsafe_links_removed"] += normalized_content.unsafe_links_removed
            summary["src_normalized"] += normalized_content.src_normalized
            summary["unsafe_src_removed"] += normalized_content.unsafe_src_removed
            summary["player_links_rewritten"] += normalized_content.player_links_rewritten
            summary["team_links_rewritten"] += normalized_content.team_links_rewritten

            if row_changed:
                summary["rows_changed"] += 1

                if len(summary["examples"]) < 20:
                    summary["examples"].append(
                        {
                            "id": row.id,
                            "language": row.language.value if row.language else None,
                            "image_url_before": row.image_url,
                            "image_url_after": normalized_image_url,
                            "content_before_sample": _sample_value(row.content),
                            "content_after_sample": _sample_value(normalized_content.content),
                            "player_links_rewritten": normalized_content.player_links_rewritten,
                            "team_links_rewritten": normalized_content.team_links_rewritten,
                        }
                    )

                if apply:
                    row.image_url = normalized_image_url
                    row.content = normalized_content.content

        if apply and summary["rows_changed"] > 0:
            await session.commit()

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize news content links/images and rewrite player/team URLs to internal routes "
            "when mapping is unambiguous."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates. Without this flag, the script runs in dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of rows to process.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = asyncio.run(run(apply=args.apply, limit=args.limit))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

