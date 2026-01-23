"""
Download and migrate leadership photos to MinIO storage.

Usage:
    python scripts/migrate_leadership.py
"""

import asyncio
import sys
import re
import json
import ssl
from pathlib import Path
from urllib.request import urlopen, Request

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.file_storage import FileStorageService
from app.minio_client import init_minio


LEADERSHIP_JSON = Path("/Users/nuralisagyndykuly/Documents/qfl/news/output/leadership_ru.json")


async def migrate_leadership():
    """Download and migrate leadership photos to MinIO."""
    await init_minio()

    if not LEADERSHIP_JSON.exists():
        print(f"Error: Leadership JSON not found: {LEADERSHIP_JSON}")
        return

    with open(LEADERSHIP_JSON) as f:
        data = json.load(f)

    # Extract image URLs from content
    images = re.findall(r'src="(/upload/images/[^"]+)"', data.get("content", ""))
    print(f"Found {len(images)} leadership photos")

    uploaded = 0
    errors = 0

    for img_path in images:
        url = f"https://kffleague.kz{img_path}"
        filename = img_path.split("/")[-1]
        name = filename.rsplit(".", 1)[0]

        try:
            print(f"Downloading {url}...")
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urlopen(req, timeout=30, context=ctx) as response:
                file_data = response.read()

            ext = filename.split(".")[-1].lower()
            content_type = {
                "jpg": "image/jpeg",
                "jpeg": "image/jpeg",
                "png": "image/png",
                "webp": "image/webp",
            }.get(ext, "image/jpeg")

            result = await FileStorageService.upload_file(
                file_data=file_data,
                filename=filename,
                content_type=content_type,
                category="leadership",
                metadata={"person-name": name},
            )

            uploaded += 1
            print(f"✓ {name}: {result['url']}")

        except Exception as e:
            errors += 1
            print(f"✗ {name}: error - {e}")

    print("\n" + "=" * 50)
    print(f"Migration complete:")
    print(f"  Uploaded: {uploaded}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(migrate_leadership())
