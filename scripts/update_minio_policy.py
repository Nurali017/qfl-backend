#!/usr/bin/env python3
"""Update MinIO bucket policy to allow public access to player_photos."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.minio_client import init_minio
import asyncio


async def main():
    """Update MinIO bucket policy."""
    print("Updating MinIO bucket policy...")
    try:
        await init_minio()
        print("✓ Policy updated successfully!")
        print("✓ player_photos/* is now publicly accessible")
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
