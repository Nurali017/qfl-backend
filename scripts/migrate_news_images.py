"""
Migrate news images from local folder to MinIO storage.

Usage:
    python scripts/migrate_news_images.py

Images are read from: /Users/nuralisagyndykuly/Documents/qfl/news/output/images/{news_id}/
"""

import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.services.file_storage import FileStorageService
from app.models.news import News
from app.minio_client import init_minio, get_public_url


IMAGES_DIR = Path("/Users/nuralisagyndykuly/Documents/qfl/news/output/images")


def get_content_type(filename: str) -> str:
    """Get content type from filename extension."""
    ext = filename.lower().split(".")[-1]
    return {
        "webp": "image/webp",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
    }.get(ext, "application/octet-stream")


async def migrate_images():
    """Migrate all news images to MinIO and update database."""

    # Initialize MinIO
    await init_minio()

    if not IMAGES_DIR.exists():
        print(f"Error: Images directory not found: {IMAGES_DIR}")
        return

    # Get all news folders
    news_folders = [f for f in IMAGES_DIR.iterdir() if f.is_dir()]
    print(f"Found {len(news_folders)} news folders with images")

    uploaded = 0
    updated = 0
    errors = 0

    async with AsyncSessionLocal() as db:
        for folder in news_folders:
            news_id = folder.name

            # Get image files in folder
            images = list(folder.glob("*"))
            if not images:
                continue

            # Use first image (usually there's only one)
            image_path = images[0]

            try:
                # Read image file
                with open(image_path, "rb") as f:
                    file_data = f.read()

                content_type = get_content_type(image_path.name)

                # Upload to MinIO
                result = await FileStorageService.upload_file(
                    file_data=file_data,
                    filename=image_path.name,
                    content_type=content_type,
                    category="news_image",
                    metadata={"news-id": str(news_id)},
                )

                uploaded += 1
                new_url = result["url"]

                # Update news record in database
                stmt = (
                    update(News)
                    .where(News.id == int(news_id))
                    .values(image_url=new_url)
                )
                result_db = await db.execute(stmt)

                if result_db.rowcount > 0:
                    updated += result_db.rowcount
                    print(f"✓ News {news_id}: uploaded and updated ({image_path.name})")
                else:
                    print(f"⚠ News {news_id}: uploaded but no DB record found")

            except Exception as e:
                errors += 1
                print(f"✗ News {news_id}: error - {e}")

        # Commit all changes
        await db.commit()

    print("\n" + "=" * 50)
    print(f"Migration complete:")
    print(f"  Uploaded: {uploaded}")
    print(f"  DB records updated: {updated}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(migrate_images())
