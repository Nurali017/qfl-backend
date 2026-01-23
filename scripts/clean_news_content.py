#!/usr/bin/env python3
"""
Clean HTML content in news table.

Usage:
    python scripts/clean_news_content.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models import News
from app.utils.html_cleaner import clean_news_content


async def clean_all_news(db: AsyncSession) -> dict:
    """Clean content field for all news articles."""
    stats = {"processed": 0, "cleaned": 0, "errors": 0}

    # Get all news
    result = await db.execute(select(News))
    all_news = result.scalars().all()

    print(f"Found {len(all_news)} news articles to process")

    for news in all_news:
        try:
            if not news.content:
                stats["processed"] += 1
                continue

            cleaned = clean_news_content(news.content)

            if cleaned != news.content:
                news.content = cleaned
                stats["cleaned"] += 1

            stats["processed"] += 1

            if stats["processed"] % 50 == 0:
                await db.commit()
                print(f"  Processed: {stats['processed']}/{len(all_news)}")

        except Exception as e:
            print(f"  Error cleaning news id={news.id}: {e}")
            stats["errors"] += 1

    await db.commit()
    return stats


async def main():
    print("=" * 60)
    print("News Content Cleaner")
    print("=" * 60)

    async with AsyncSessionLocal() as db:
        stats = await clean_all_news(db)

    print()
    print("=" * 60)
    print("Completed!")
    print(f"  Processed: {stats['processed']}")
    print(f"  Cleaned: {stats['cleaned']}")
    print(f"  Errors: {stats['errors']}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
