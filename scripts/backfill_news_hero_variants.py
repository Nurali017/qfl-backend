"""
Backfill 1200x630 ``-hero.webp`` variants for legacy news cover uploads.

The site started generating hero variants on upload as part of the Discover
SEO work — this one-off script catches old rows so all NewsArticle JSON-LD
entries can ship ``ImageObject`` with explicit dimensions.

Usage:
  docker exec qfl-backend python scripts/backfill_news_hero_variants.py --dry-run
  docker exec qfl-backend python scripts/backfill_news_hero_variants.py --limit 100
  docker exec qfl-backend python scripts/backfill_news_hero_variants.py --news-id 123
"""
import argparse
import asyncio
import io
import logging

from sqlalchemy import select

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.minio_client import get_minio_client
from app.models.news import News
from app.services.file_storage import (
    FileStorageService,
    _generate_news_hero,
    _news_hero_object_name,
    _run_sync,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
settings = get_settings()


def _extract_object_name_and_file_id(image_url: str | None) -> tuple[str, str] | None:
    """Pull ``(object_name, file_id)`` out of a stored ``image_url``.

    Stored values look like ``.../news_image/{news_id}/{file_id}.{ext}`` —
    we strip query strings and protocol prefix. Returns ``None`` for
    external URLs (e.g. legacy items still pointing at the source site).
    """
    if not image_url:
        return None
    idx = image_url.find("news_image/")
    if idx == -1:
        return None
    object_name = image_url[idx:].split("?", 1)[0]
    tail = object_name.rsplit("/", 1)[-1]
    file_id = tail.rsplit(".", 1)[0]
    if not file_id or file_id.endswith("-hero"):
        return None
    return object_name, file_id


async def _process_one(
    client,
    bucket: str,
    news_id: int,
    image_url: str | None,
    dry_run: bool,
) -> str:
    parsed = _extract_object_name_and_file_id(image_url)
    if not parsed:
        return "no_object"
    object_name, file_id = parsed
    hero_object_name = _news_hero_object_name(news_id, file_id)

    try:
        await _run_sync(client.stat_object, bucket, hero_object_name)
        return "existed"
    except Exception:
        # not found → generate below
        pass

    if dry_run:
        logger.info("[DRY] would generate %s from %s", hero_object_name, object_name)
        return "generated"

    data = await FileStorageService.get_file(object_name)
    if data is None:
        logger.warning("Source missing for news %d: %s", news_id, object_name)
        return "error"

    raw_bytes, _ = data
    try:
        hero_bytes = await _run_sync(_generate_news_hero, raw_bytes)
    except Exception:
        logger.exception("Hero generation failed for news %d", news_id)
        return "error"

    await _run_sync(
        client.put_object,
        bucket_name=bucket,
        object_name=hero_object_name,
        data=io.BytesIO(hero_bytes),
        length=len(hero_bytes),
        content_type="image/webp",
        metadata={"category": "news_image_hero", "parent-file-id": file_id},
    )
    logger.info("Generated %s (%d bytes)", hero_object_name, len(hero_bytes))
    return "generated"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--news-id", type=int, default=None)
    args = parser.parse_args()

    async with AsyncSessionLocal() as db:
        stmt = select(News.id, News.image_url).where(News.image_url.is_not(None))
        if args.news_id is not None:
            stmt = stmt.where(News.id == args.news_id)
        stmt = stmt.order_by(News.id)
        if args.limit is not None:
            stmt = stmt.limit(args.limit)
        rows = (await db.execute(stmt)).all()

    logger.info("Found %d candidate news rows", len(rows))

    client = get_minio_client()
    bucket = settings.minio_bucket
    counters = {"generated": 0, "existed": 0, "no_object": 0, "error": 0}

    for news_id, image_url in rows:
        try:
            outcome = await _process_one(
                client, bucket, news_id, image_url, args.dry_run
            )
        except Exception:
            logger.exception("Unhandled error for news %d", news_id)
            outcome = "error"
        counters[outcome] = counters.get(outcome, 0) + 1

    logger.info(
        "Done. generated=%d existed=%d no_object=%d errors=%d",
        counters["generated"],
        counters["existed"],
        counters["no_object"],
        counters["error"],
    )


if __name__ == "__main__":
    asyncio.run(main())
