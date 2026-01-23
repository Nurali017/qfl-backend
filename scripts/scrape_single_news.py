#!/usr/bin/env python3
"""
Scrape a single news article from kffleague.kz and save to DB.

This script demonstrates the full workflow:
1. Parse news page (title, content, images, date, category)
2. Download all images (main + content images)
3. Upload images to MinIO
4. Replace image URLs in HTML content with MinIO URLs
5. Save to PostgreSQL database

Usage:
    # Test with news ID 1 (Russian version)
    python scripts/scrape_single_news.py --news-id 1 --language ru

    # Dry run (no DB changes)
    python scripts/scrape_single_news.py --news-id 1 --language ru --dry-run

    # Both languages
    python scripts/scrape_single_news.py --news-id 1 --both-languages
"""

import argparse
import asyncio
import io
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from sqlalchemy import select

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models.news import News, ArticleType
from app.models.page import Language
from app.minio_client import get_minio_client, get_public_url, init_minio
from app.config import get_settings
from app.utils.html_cleaner import clean_news_content

settings = get_settings()

BASE_URL = "https://kffleague.kz"


@dataclass
class ParsedNews:
    """Parsed news data from kffleague.kz"""
    source_id: int
    source_url: str
    language: str  # 'ru' or 'kz'
    title: str
    category: str | None
    publish_date: datetime | None
    main_image_url: str | None
    content_html: str | None
    content_images: list[str]  # List of image URLs in content
    video_url: str | None = None  # YouTube embed URL


class NewsParser:
    """Parser for kffleague.kz news articles"""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

    async def close(self):
        await self.http_client.aclose()

    async def parse_news_page(self, news_id: int, language: str) -> ParsedNews | None:
        """Parse a single news page using Playwright."""
        url = f"{BASE_URL}/{language}/news/{news_id}"
        print(f"  Parsing: {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()

            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)

                # Check if page exists (not 404)
                title_el = await page.query_selector("h1")
                if not title_el:
                    print(f"  ERROR: Page not found or no title (404?)")
                    await browser.close()
                    return None

                # Extract data using JavaScript
                data = await page.evaluate("""
                    () => {
                        // Title
                        const title = document.querySelector('h1')?.textContent?.trim() || '';

                        // Category (badge)
                        const category = document.querySelector('.badge')?.textContent?.trim() || null;

                        // Date - look for pattern DD.MM.YYYY
                        let publishDate = null;
                        const dateMatch = document.body.innerHTML.match(/(\\d{2}\\.\\d{2}\\.\\d{4})/);
                        if (dateMatch) {
                            publishDate = dateMatch[1];
                        }

                        // Main image - from hero/header section or og:image
                        let mainImage = null;
                        const heroImg = document.querySelector('header img, .hero img, [class*="hero"] img');
                        if (heroImg) {
                            mainImage = heroImg.src;
                        } else {
                            const ogImage = document.querySelector('meta[property="og:image"]');
                            if (ogImage) {
                                mainImage = ogImage.content;
                            }
                        }

                        // Content HTML
                        const contentEl = document.querySelector('.news-content');
                        const contentHtml = contentEl ? contentEl.innerHTML : null;

                        // All images in content
                        const contentImages = [];
                        if (contentEl) {
                            contentEl.querySelectorAll('img').forEach(img => {
                                if (img.src && img.src.includes('/upload/')) {
                                    contentImages.push(img.src);
                                }
                            });
                        }

                        // YouTube video URL (from iframe src or data-src)
                        let videoUrl = null;
                        const ytIframe = document.querySelector('iframe[src*="youtube"], iframe[data-src*="youtube"]');
                        if (ytIframe) {
                            videoUrl = ytIframe.src || ytIframe.getAttribute('data-src');
                        }

                        return {
                            title,
                            category,
                            publishDate,
                            mainImage,
                            contentHtml,
                            contentImages,
                            videoUrl
                        };
                    }
                """)

                await browser.close()

                if not data['title']:
                    print(f"  ERROR: Could not extract title")
                    return None

                # Parse date
                publish_date = None
                if data['publishDate']:
                    try:
                        publish_date = datetime.strptime(data['publishDate'], '%d.%m.%Y')
                    except ValueError:
                        pass

                return ParsedNews(
                    source_id=news_id,
                    source_url=url,
                    language=language,
                    title=data['title'],
                    category=data['category'],
                    publish_date=publish_date,
                    main_image_url=data['mainImage'],
                    content_html=data['contentHtml'],
                    content_images=data['contentImages'] or [],
                    video_url=data.get('videoUrl')
                )

            except Exception as e:
                print(f"  ERROR: {e}")
                await browser.close()
                return None

    async def download_image(self, url: str) -> bytes | None:
        """Download image from URL."""
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"    Download failed: {url[:60]}... - {e}")
            return None

    async def upload_to_minio(self, image_data: bytes, news_id: int, filename: str) -> str | None:
        """Upload image to MinIO and return public URL."""
        try:
            client = get_minio_client()
            bucket = settings.minio_bucket

            # Path: news/{news_id}/{filename}
            object_name = f"news/{news_id}/{filename}"
            data = io.BytesIO(image_data)

            # Determine content type
            content_type = "image/webp"
            if filename.endswith(".jpg") or filename.endswith(".jpeg"):
                content_type = "image/jpeg"
            elif filename.endswith(".png"):
                content_type = "image/png"

            client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=len(image_data),
                content_type=content_type,
                metadata={
                    "news-id": str(news_id),
                    "original-filename": filename,
                }
            )

            return get_public_url(object_name)

        except Exception as e:
            print(f"    MinIO upload failed: {e}")
            return None

    def replace_image_urls(self, html: str, url_mapping: dict[str, str]) -> str:
        """Replace original image URLs with MinIO URLs in HTML content."""
        if not html:
            return html

        result = html
        for original_url, new_url in url_mapping.items():
            result = result.replace(original_url, new_url)

        return result

    def get_filename_from_url(self, url: str, index: int) -> str:
        """Extract filename from URL or generate one."""
        # Try to get original filename
        parts = url.split('/')
        if parts:
            original_name = parts[-1]
            # Remove query params
            original_name = original_name.split('?')[0]
            if '.' in original_name:
                return original_name

        # Generate filename
        return f"content_{index}.webp"

    async def process_news(self, parsed: ParsedNews, db_news_id: int) -> tuple[str | None, str | None]:
        """
        Process news: download images, upload to MinIO, replace URLs.
        Returns (processed_content_html, main_image_url)
        """
        url_mapping = {}
        main_image_minio_url = None

        # 1. Process main image
        if parsed.main_image_url:
            print(f"    Downloading main image...")
            image_data = await self.download_image(parsed.main_image_url)
            if image_data:
                filename = "main.webp"
                if self.dry_run:
                    print(f"    [DRY] Would upload main image as {filename}")
                    main_image_minio_url = f"[MINIO]/news/{db_news_id}/{filename}"
                else:
                    main_image_minio_url = await self.upload_to_minio(image_data, db_news_id, filename)
                    if main_image_minio_url:
                        print(f"    Uploaded: {main_image_minio_url}")

        # 2. Process content images
        if parsed.content_images:
            print(f"    Processing {len(parsed.content_images)} content images...")
            for i, img_url in enumerate(parsed.content_images, 1):
                image_data = await self.download_image(img_url)
                if image_data:
                    filename = f"content_{i}.webp"
                    if self.dry_run:
                        print(f"    [DRY] Would upload content image {i} as {filename}")
                        url_mapping[img_url] = f"[MINIO]/news/{db_news_id}/{filename}"
                    else:
                        new_url = await self.upload_to_minio(image_data, db_news_id, filename)
                        if new_url:
                            url_mapping[img_url] = new_url
                            print(f"    Uploaded content image {i}: {new_url}")

        # 3. Replace URLs in content
        processed_content = parsed.content_html
        if url_mapping and processed_content:
            processed_content = self.replace_image_urls(processed_content, url_mapping)

        # 4. Clean HTML
        if processed_content:
            processed_content = clean_news_content(processed_content)

        return processed_content, main_image_minio_url

    def determine_article_type(self, category: str | None, title: str) -> ArticleType:
        """Determine article type based on category and title."""
        # Analytics keywords
        analytics_keywords = ['анализ', 'аналитика', 'статистика', 'обзор', 'итоги']

        title_lower = title.lower()
        for keyword in analytics_keywords:
            if keyword in title_lower:
                return ArticleType.ANALYTICS

        return ArticleType.NEWS

    async def save_to_db(self, parsed: ParsedNews, content_html: str | None, main_image_url: str | None) -> News | None:
        """Save news to database."""
        async with AsyncSessionLocal() as session:
            # Check if already exists
            lang_enum = Language.KZ if parsed.language == 'kz' else Language.RU

            existing = await session.execute(
                select(News).where(
                    News.source_id == parsed.source_id,
                    News.language == lang_enum
                )
            )
            existing_news = existing.scalar_one_or_none()

            if existing_news:
                print(f"    News already exists (ID: {existing_news.id}), updating...")
                existing_news.title = parsed.title
                existing_news.category = parsed.category
                existing_news.publish_date = parsed.publish_date
                existing_news.content = content_html
                existing_news.content_text = BeautifulSoup(content_html or '', 'html.parser').get_text() if content_html else None
                existing_news.image_url = main_image_url
                existing_news.video_url = parsed.video_url
                existing_news.article_type = self.determine_article_type(parsed.category, parsed.title)
                existing_news.source_url = parsed.source_url

                if not self.dry_run:
                    await session.commit()
                    await session.refresh(existing_news)
                return existing_news

            # Create new
            # Get excerpt from content
            excerpt = None
            if content_html:
                text = BeautifulSoup(content_html, 'html.parser').get_text()
                if text:
                    excerpt = text[:300].rsplit(' ', 1)[0] + '...' if len(text) > 300 else text

            news = News(
                source_id=parsed.source_id,
                source_url=parsed.source_url,
                language=lang_enum,
                title=parsed.title,
                excerpt=excerpt,
                content=content_html,
                content_text=BeautifulSoup(content_html or '', 'html.parser').get_text() if content_html else None,
                image_url=main_image_url,
                video_url=parsed.video_url,
                category=parsed.category,
                article_type=self.determine_article_type(parsed.category, parsed.title),
                publish_date=parsed.publish_date,
                is_slider=False,
            )

            if not self.dry_run:
                session.add(news)
                await session.commit()
                await session.refresh(news)
                print(f"    Created news with ID: {news.id}")
            else:
                print(f"    [DRY] Would create news")
                news.id = -1  # Placeholder for dry run

            return news


async def main():
    parser = argparse.ArgumentParser(description="Scrape a single news from kffleague.kz")
    parser.add_argument("--news-id", type=int, required=True, help="News ID from kffleague.kz")
    parser.add_argument("--language", choices=['ru', 'kz'], default='ru', help="Language version")
    parser.add_argument("--both-languages", action="store_true", help="Parse both RU and KZ versions")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")
    args = parser.parse_args()

    print("=" * 70)
    print("KFF League News Scraper - Single Article")
    print("=" * 70)
    if args.dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    print()

    # Initialize MinIO
    if not args.dry_run:
        print("[0/4] Initializing MinIO...")
        await init_minio()

    scraper = NewsParser(dry_run=args.dry_run)

    languages = ['ru', 'kz'] if args.both_languages else [args.language]

    for lang in languages:
        print(f"\n[1/4] Parsing news ID {args.news_id} ({lang.upper()})...")

        # Parse
        parsed = await scraper.parse_news_page(args.news_id, lang)
        if not parsed:
            print(f"  FAILED: Could not parse news")
            continue

        print(f"  Title: {parsed.title[:60]}...")
        print(f"  Category: {parsed.category}")
        print(f"  Date: {parsed.publish_date}")
        print(f"  Main image: {'Yes' if parsed.main_image_url else 'No'}")
        print(f"  Content images: {len(parsed.content_images)}")
        print(f"  Video URL: {parsed.video_url or 'No'}")

        # Determine DB news ID (for MinIO path)
        # In real scenario, we'd first create the DB record to get the ID
        # For now, use source_id as temporary ID, will be updated after save
        temp_news_id = parsed.source_id * 1000 + (1 if lang == 'ru' else 2)  # Unique per language

        print(f"\n[2/4] Processing images...")
        content_html, main_image_url = await scraper.process_news(parsed, temp_news_id)

        print(f"\n[3/4] Saving to database...")
        news = await scraper.save_to_db(parsed, content_html, main_image_url)

        if news:
            print(f"\n[4/4] Summary ({lang.upper()}):")
            print(f"  News ID: {news.id}")
            print(f"  Source ID: {news.source_id}")
            print(f"  Title: {news.title[:50]}...")
            print(f"  Category: {news.category}")
            print(f"  Image URL: {news.image_url}")
            print(f"  Video URL: {news.video_url}")
            print(f"  Content length: {len(news.content or '')} chars")

    await scraper.close()
    print("\n" + "=" * 70)
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
