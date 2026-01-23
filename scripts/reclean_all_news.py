#!/usr/bin/env python3
"""
Re-clean all news content: restore from JSON, clean HTML, remove dates.

Usage:
    python scripts/reclean_all_news.py
"""

import asyncio
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models import News
from app.utils.html_cleaner import clean_news_content


OUTPUT_DIR = Path("/Users/nuralisagyndykuly/Documents/qfl/news/output")


def load_original_content() -> dict:
    """Load original content from JSON files."""
    content_map = {}

    for filename, lang in [("news_kz.json", "KZ"), ("news_ru.json", "RU")]:
        filepath = OUTPUT_DIR / filename
        if filepath.exists():
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    key = (item["id"], lang)
                    content_map[key] = {
                        "content": item.get("content"),
                        "content_text": item.get("content_text"),
                    }

    return content_map


def text_to_html(text: str) -> str:
    """Convert plain text to HTML paragraphs, filtering out dates."""
    if not text:
        return ""

    date_pattern = re.compile(r'^\d{2}\.\d{2}\.\d{4}$')

    paragraphs = text.replace('\r\n', '\n').split('\n\n')
    if len(paragraphs) == 1:
        paragraphs = text.split('\n')

    html_parts = []
    for p in paragraphs:
        p = p.strip()
        # Skip empty, short, or date-only lines
        if not p or len(p) < 10 or date_pattern.match(p):
            continue
        html_parts.append(f"<p>{p}</p>")

    return "\n".join(html_parts)


async def reclean_all_news(db: AsyncSession) -> dict:
    """Re-clean all news articles."""
    stats = {"processed": 0, "cleaned": 0, "used_text": 0, "errors": 0}

    # Load original content
    print("Loading original content from JSON...")
    original = load_original_content()
    print(f"Loaded {len(original)} articles from JSON")

    # Process each key from original content
    print(f"Processing articles...")

    for key, orig in original.items():
        news_id, lang_str = key
        try:
            # Get the specific news item
            from app.models import Language
            lang_enum = Language.KZ if lang_str == "KZ" else Language.RU

            result = await db.execute(
                select(News).where(News.id == news_id, News.language == lang_enum)
            )
            news = result.scalar_one_or_none()

            if not news:
                continue

            # Start with original HTML content
            orig_content = orig.get("content", "")
            orig_text = orig.get("content_text", "")

            # Clean the HTML
            cleaned = clean_news_content(orig_content) if orig_content else None

            # If cleaned content is too short, use content_text
            if not cleaned or len(cleaned) < 100:
                if orig_text and len(orig_text) > 100:
                    cleaned = text_to_html(orig_text)
                    stats["used_text"] += 1

            if cleaned:
                news.content = cleaned
                stats["cleaned"] += 1

            stats["processed"] += 1

            if stats["processed"] % 100 == 0:
                await db.commit()
                print(f"  Processed: {stats['processed']}/{len(original)}")

        except Exception as e:
            await db.rollback()
            print(f"  Error processing news id={news_id}, lang={lang_str}: {e}")
            stats["errors"] += 1

    await db.commit()
    return stats


async def main():
    print("=" * 60)
    print("Re-clean All News Content")
    print("=" * 60)

    async with AsyncSessionLocal() as db:
        stats = await reclean_all_news(db)

    print()
    print("=" * 60)
    print("Completed!")
    print(f"  Processed: {stats['processed']}")
    print(f"  Cleaned: {stats['cleaned']}")
    print(f"  Used content_text: {stats['used_text']}")
    print(f"  Errors: {stats['errors']}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
