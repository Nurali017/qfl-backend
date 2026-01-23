"""
One-time migration script to classify all existing news articles.

Usage:
    # Dry run (no database changes)
    python scripts/classify_existing_news.py --dry-run

    # Classify all news
    python scripts/classify_existing_news.py

    # Classify only Russian news
    python scripts/classify_existing_news.py --language ru

    # Limit to 100 articles (for testing)
    python scripts/classify_existing_news.py --limit 100
"""
import asyncio
import argparse
import logging
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.news import News, ArticleType
from app.services.news_classifier import NewsClassifierService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/classification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def classify_news(
    dry_run: bool = False,
    language: str | None = None,
    limit: int | None = None,
    batch_size: int = 20
):
    """
    Classify all unclassified news articles.

    Args:
        dry_run: If True, don't update database
        language: Filter by language (kz or ru)
        limit: Maximum number of articles to process
        batch_size: Number of articles to classify concurrently
    """
    logger.info("=" * 60)
    logger.info("NEWS CLASSIFICATION SCRIPT")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"Language filter: {language or 'all'}")
    logger.info(f"Limit: {limit or 'none'}")
    logger.info(f"Batch size: {batch_size}")
    logger.info("=" * 60)

    # Initialize service
    classifier = NewsClassifierService()

    # Get database session
    async with AsyncSessionLocal() as db:
        try:
            # Query unclassified news
            query = select(News).where(News.article_type.is_(None))

            if language:
                from app.models.page import Language
                lang_enum = Language.KZ if language == "kz" else Language.RU
                query = query.where(News.language == lang_enum)

            if limit:
                query = query.limit(limit)

            result = await db.execute(query)
            news_items = result.scalars().all()

            total = len(news_items)
            logger.info(f"Found {total} unclassified news articles")

            if total == 0:
                logger.info("No articles to classify. Exiting.")
                return

            # Statistics
            stats = {
                ArticleType.NEWS: 0,
                ArticleType.ANALYTICS: 0,
                "errors": 0
            }

            # Process in batches
            for i in range(0, total, batch_size):
                batch = news_items[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total + batch_size - 1) // batch_size

                logger.info(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch)} articles)")

                # Classify batch
                classifications = await classifier.classify_batch(batch)

                # Update database (if not dry-run)
                if not dry_run:
                    for news_id, article_type in classifications.items():
                        stmt = update(News).where(News.id == news_id).values(article_type=article_type)
                        await db.execute(stmt)
                        stats[article_type] += 1

                    await db.commit()
                    logger.info(f"✓ Batch {batch_num} committed to database")
                else:
                    for article_type in classifications.values():
                        stats[article_type] += 1
                    logger.info(f"✓ Batch {batch_num} classified (dry-run, not saved)")

                # Log sample results
                for news in batch[:3]:  # Show first 3 of each batch
                    classification = classifications.get(news.id, "ERROR")
                    logger.debug(
                        f"  [{classification}] {news.id}: {news.title[:60]}..."
                    )

                # Progress update
                processed = min(i + batch_size, total)
                progress_pct = (processed / total) * 100
                logger.info(f"Progress: {processed}/{total} ({progress_pct:.1f}%)")

            # Final statistics
            logger.info("\n" + "=" * 60)
            logger.info("CLASSIFICATION COMPLETE")
            logger.info(f"Total processed: {total}")
            logger.info(f"  - NEWS: {stats[ArticleType.NEWS]} ({stats[ArticleType.NEWS]/total*100:.1f}%)")
            logger.info(f"  - ANALYTICS: {stats[ArticleType.ANALYTICS]} ({stats[ArticleType.ANALYTICS]/total*100:.1f}%)")
            logger.info(f"  - Errors: {stats['errors']}")

            if dry_run:
                logger.info("\n⚠️  DRY RUN MODE - No changes saved to database")
            else:
                logger.info("\n✓ All classifications saved to database")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            await db.rollback()
            raise


def main():
    parser = argparse.ArgumentParser(description="Classify existing news articles using AI")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument("--language", choices=["kz", "ru"], help="Filter by language")
    parser.add_argument("--limit", type=int, help="Limit number of articles to process")
    parser.add_argument("--batch-size", type=int, default=20, help="Batch size for concurrent processing")

    args = parser.parse_args()

    asyncio.run(classify_news(
        dry_run=args.dry_run,
        language=args.language,
        limit=args.limit,
        batch_size=args.batch_size
    ))


if __name__ == "__main__":
    main()
