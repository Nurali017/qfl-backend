#!/usr/bin/env python3
"""
Import CMS data from JSON files into PostgreSQL database.

Usage:
    python scripts/import_cms_data.py

Files expected in output/ directory:
    - contacts_kz.json, contacts_ru.json
    - documents_kz.json, documents_ru.json
    - leadership_kz.json, leadership_ru.json
    - news_kz.json, news_ru.json
"""

import asyncio
import json
import re
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.database import AsyncSessionLocal
from app.models import Page, News


OUTPUT_DIR = Path("/Users/nuralisagyndykuly/Documents/qfl/news/output")

# Use uppercase strings since SQLAlchemy uses enum NAMES (KZ, RU)
LANG_KZ = "KZ"
LANG_RU = "RU"


def parse_date_from_excerpt(excerpt: str) -> datetime | None:
    """Extract date from news excerpt like 'ПРЕМЬЕР-ЛИГА 01.01.2026 ...'"""
    if not excerpt:
        return None
    # Pattern: DD.MM.YYYY
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', excerpt)
    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            return None
    return None


def extract_category_from_excerpt(excerpt: str) -> str | None:
    """Extract category from news excerpt."""
    if not excerpt:
        return None
    # Categories are usually at the beginning in uppercase
    categories = ["ПРЕМЬЕР-ЛИГА", "ПЕРВАЯ ЛИГА", "КУБОК", "ВТОРАЯ ЛИГА", "ЖЕНСКАЯ ЛИГА"]
    for cat in categories:
        if cat in excerpt.upper():
            return cat
    return None


# Mapping from category names (cyrillic) to tournament IDs
CATEGORY_TO_TOURNAMENT_ID = {
    "ПРЕМЬЕР-ЛИГА": "pl",
    "ПЕРВАЯ ЛИГА": "1l",
    "БІРІНШІ ЛИГА": "1l",
    "КУБОК": "cup",
    "ВТОРАЯ ЛИГА": "2l",
    "ЕКІНШІ ЛИГА": "2l",
    "ЖЕНСКАЯ ЛИГА": "el",
    "ӘЙЕЛДЕР ЛИГАСЫ": "el",
}


def get_tournament_id_from_category(category: str | None) -> str | None:
    """Get tournament_id from category name."""
    if not category:
        return None
    return CATEGORY_TO_TOURNAMENT_ID.get(category.upper())


def get_tournament_id_from_excerpt(excerpt: str | None) -> str | None:
    """Get tournament_id from excerpt text by matching patterns."""
    if not excerpt:
        return None
    
    excerpt_upper = excerpt.upper()
    
    # Order matters - check more specific patterns first
    patterns = [
        ("КУБОК", "cup"),
        ("ПЕРВАЯ ЛИГА", "1l"),
        ("БІРІНШІ ЛИГА", "1l"),
        ("ВТОРАЯ ЛИГА", "2l"),
        ("ЕКІНШІ ЛИГА", "2l"),
        ("ЖЕНСКАЯ", "el"),
        ("ӘЙЕЛДЕР", "el"),
        ("ПРЕМЬЕР", "pl"),
    ]
    
    for pattern, tid in patterns:
        if pattern in excerpt_upper:
            return tid
    
    return None


def extract_image_from_content(content: str) -> str | None:
    """Extract main image URL from content HTML."""
    if not content:
        return None
    # Look for background-image URL in style
    match = re.search(r"background:\s*url\(['\"]?(https?://[^'\")\s]+)['\"]?\)", content)
    if match:
        return match.group(1)
    # Look for img src
    match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', content)
    if match:
        return match.group(1)
    return None


async def import_pages(db: AsyncSession) -> dict:
    """Import static pages (contacts, documents, leadership)."""
    stats = {"imported": 0, "updated": 0, "errors": 0}

    page_files = [
        ("contacts_kz.json", LANG_KZ),
        ("contacts_ru.json", LANG_RU),
        ("documents_kz.json", LANG_KZ),
        ("documents_ru.json", LANG_RU),
        ("leadership_kz.json", LANG_KZ),
        ("leadership_ru.json", LANG_RU),
    ]

    for filename, lang in page_files:
        filepath = OUTPUT_DIR / filename
        if not filepath.exists():
            print(f"  Warning: {filename} not found, skipping")
            stats["errors"] += 1
            continue

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            stmt = insert(Page).values(
                slug=data["slug"],
                language=lang,
                title=data["title"],
                content=data.get("content"),
                content_text=data.get("content_text"),
                url=data.get("url"),
                structured_data=data.get("structured_data"),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["slug", "language"],
                set_={
                    "title": stmt.excluded.title,
                    "content": stmt.excluded.content,
                    "content_text": stmt.excluded.content_text,
                    "url": stmt.excluded.url,
                    "structured_data": stmt.excluded.structured_data,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await db.execute(stmt)
            stats["imported"] += 1
            print(f"  Imported: {filename} ({data['slug']})")

        except Exception as e:
            print(f"  Error importing {filename}: {e}")
            stats["errors"] += 1

    await db.commit()
    return stats


async def import_news(db: AsyncSession) -> dict:
    """Import news articles."""
    stats = {"imported": 0, "updated": 0, "errors": 0}

    news_files = [
        ("news_kz.json", LANG_KZ),
        ("news_ru.json", LANG_RU),
    ]

    for filename, lang in news_files:
        filepath = OUTPUT_DIR / filename
        if not filepath.exists():
            print(f"  Warning: {filename} not found, skipping")
            stats["errors"] += 1
            continue

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                news_list = json.load(f)

            print(f"  Processing {filename}: {len(news_list)} articles")

            batch_size = 100
            for i in range(0, len(news_list), batch_size):
                batch = news_list[i:i + batch_size]

                for item in batch:
                    try:
                        excerpt = item.get("excerpt", "")
                        content = item.get("content", "")

                        # Use direct fields from JSON if available, fallback to extraction
                        image_url = item.get("image_url") or extract_image_from_content(content)
                        category = item.get("category") or extract_category_from_excerpt(excerpt)
                        # Try to get tournament_id from category first, then from excerpt
                        tournament_id = get_tournament_id_from_category(category) or get_tournament_id_from_excerpt(excerpt)

                        # Parse date - use 'date' field if available
                        publish_date = None
                        if item.get("date"):
                            try:
                                # Try parsing DD.MM.YYYY format
                                publish_date = datetime.strptime(item["date"], "%d.%m.%Y")
                            except ValueError:
                                pass
                        if not publish_date:
                            publish_date = parse_date_from_excerpt(excerpt)

                        stmt = insert(News).values(
                            id=item["id"],
                            language=lang,
                            title=item["title"],
                            excerpt=excerpt,
                            content=content,
                            content_text=item.get("content_text"),
                            url=item.get("url"),
                            image_url=image_url,
                            category=category,
                            tournament_id=tournament_id,
                            publish_date=publish_date,
                            structured_data=item.get("structured_data"),
                            created_at=datetime.utcnow(),
                            updated_at=datetime.utcnow(),
                        )
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["id", "language"],
                            set_={
                                "title": stmt.excluded.title,
                                "excerpt": stmt.excluded.excerpt,
                                "content": stmt.excluded.content,
                                "content_text": stmt.excluded.content_text,
                                "url": stmt.excluded.url,
                                "image_url": stmt.excluded.image_url,
                                "category": stmt.excluded.category,
                                "tournament_id": stmt.excluded.tournament_id,
                                "publish_date": stmt.excluded.publish_date,
                                "structured_data": stmt.excluded.structured_data,
                                "updated_at": stmt.excluded.updated_at,
                            },
                        )
                        await db.execute(stmt)
                        stats["imported"] += 1

                    except Exception as e:
                        print(f"    Error importing news id={item.get('id')}: {e}")
                        stats["errors"] += 1

                await db.commit()
                print(f"    Processed {min(i + batch_size, len(news_list))}/{len(news_list)}")

        except Exception as e:
            print(f"  Error processing {filename}: {e}")
            stats["errors"] += 1

    return stats


async def main():
    """Main import function."""
    print("=" * 60)
    print("CMS Data Import")
    print("=" * 60)
    print(f"Source directory: {OUTPUT_DIR}")
    print()

    if not OUTPUT_DIR.exists():
        print(f"Error: Output directory not found: {OUTPUT_DIR}")
        sys.exit(1)

    async with AsyncSessionLocal() as db:
        # Import pages
        print("Importing pages...")
        page_stats = await import_pages(db)
        print(f"  Pages: {page_stats['imported']} imported, {page_stats['errors']} errors")
        print()

        # Import news
        print("Importing news...")
        news_stats = await import_news(db)
        print(f"  News: {news_stats['imported']} imported, {news_stats['errors']} errors")
        print()

    print("=" * 60)
    print("Import completed!")
    print(f"  Total pages: {page_stats['imported']}")
    print(f"  Total news: {news_stats['imported']}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
