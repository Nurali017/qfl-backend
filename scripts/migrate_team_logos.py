"""
Migrate team logos from local assets to MinIO storage.

Usage:
    python scripts/migrate_team_logos.py

Logos are read from: /Users/nuralisagyndykuly/Documents/qfl/assets/teams/
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.file_storage import FileStorageService
from app.minio_client import init_minio


LOGOS_DIR = Path("/Users/nuralisagyndykuly/Documents/qfl/assets/teams")


def get_content_type(filename: str) -> str:
    """Get content type from filename extension."""
    ext = filename.lower().split(".")[-1]
    return {
        "webp": "image/webp",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "svg": "image/svg+xml",
    }.get(ext, "application/octet-stream")


async def migrate_logos():
    """Migrate all team logos to MinIO."""
    await init_minio()

    if not LOGOS_DIR.exists():
        print(f"Error: Logos directory not found: {LOGOS_DIR}")
        return

    logos = list(LOGOS_DIR.glob("*.*"))
    print(f"Found {len(logos)} team logos")

    uploaded = 0
    errors = 0

    for logo_path in logos:
        team_name = logo_path.stem  # filename without extension

        try:
            with open(logo_path, "rb") as f:
                file_data = f.read()

            content_type = get_content_type(logo_path.name)

            result = await FileStorageService.upload_team_logo(
                file_data=file_data,
                team_name=team_name,
                content_type=content_type,
            )

            uploaded += 1
            print(f"✓ {team_name}: {result['url']}")

        except Exception as e:
            errors += 1
            print(f"✗ {team_name}: error - {e}")

    print("\n" + "=" * 50)
    print(f"Migration complete:")
    print(f"  Uploaded: {uploaded}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(migrate_logos())
