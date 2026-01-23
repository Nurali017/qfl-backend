#!/usr/bin/env python3
"""
Fix news articles with very short content by using content_text.

Usage:
    python scripts/fix_short_content.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models import News


def text_to_html(text: str) -> str:
    """Convert plain text to HTML paragraphs."""
    if not text:
        return ""

    # Split by double newlines or single newlines
    paragraphs = text.replace('\r\n', '\n').split('\n\n')
    if len(paragraphs) == 1:
        # Try splitting by single newlines if no double newlines
        paragraphs = text.split('\n')

    # Filter empty paragraphs and wrap in <p> tags
    html_parts = []
    for p in paragraphs:
        p = p.strip()
        if p and len(p) > 10:  # Skip very short lines
            html_parts.append(f"<p>{p}</p>")

    return "\n".join(html_parts)


async def fix_short_content(db: AsyncSession) -> dict:
    """Fix articles with short content using content_text."""
    stats = {"checked": 0, "fixed": 0, "errors": 0}

    # Get articles with short content (less than 200 chars) but have content_text
    result = await db.execute(
        select(News)
        .where(
            func.length(News.content) < 200,
            func.length(News.content_text) > 200
        )
    )
    articles = result.scalars().all()

    print(f"Found {len(articles)} articles with short content to fix")

    for news in articles:
        try:
            stats["checked"] += 1

            # Convert content_text to HTML
            new_content = text_to_html(news.content_text)

            if new_content and len(new_content) > len(news.content or ""):
                news.content = new_content
                stats["fixed"] += 1
                print(f"  Fixed ID {news.id}: {news.title[:50]}...")

        except Exception as e:
            print(f"  Error fixing news id={news.id}: {e}")
            stats["errors"] += 1

    await db.commit()
    return stats


async def main():
    print("=" * 60)
    print("Fix Short Content")
    print("=" * 60)

    async with AsyncSessionLocal() as db:
        stats = await fix_short_content(db)

    print()
    print("=" * 60)
    print("Completed!")
    print(f"  Checked: {stats['checked']}")
    print(f"  Fixed: {stats['fixed']}")
    print(f"  Errors: {stats['errors']}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
