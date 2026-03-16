"""One-time script to remove backgrounds from existing player/coach photos in MinIO.

Downloads each photo, removes background via rembg (u2netp model),
resizes to 800x1200, and re-uploads as transparent WebP.

Usage:
    # Dry run (default) — shows what would be done
    python -m scripts.remove_bg_existing_photos

    # Actually apply
    python -m scripts.remove_bg_existing_photos --apply
"""

import argparse
import io
import sys
import time
from pathlib import PurePosixPath

import numpy as np
from PIL import Image
from rembg import new_session, remove

# ── Bootstrap app config ──
sys.path.insert(0, str(PurePosixPath(__file__).parent.parent))

from app.config import get_settings  # noqa: E402
from app.minio_client import get_minio_client  # noqa: E402

settings = get_settings()

MAX_SIZE = (800, 1200)
WEBP_QUALITY = 85
MIN_SIZE = 5 * 1024  # 5 KB — skip tiny/placeholder files

CATEGORIES = ["player_photos", "coach_photos"]


def has_transparency(data: bytes) -> bool:
    """Check if an image already has meaningful transparency."""
    try:
        img = Image.open(io.BytesIO(data))
        if img.mode not in ("RGBA", "LA", "PA"):
            return False
        alpha = np.array(img.split()[-1])
        # If >5% of pixels are transparent, consider it already processed
        transparent_ratio = np.count_nonzero(alpha < 128) / alpha.size
        return transparent_ratio > 0.05
    except Exception:
        return False


def remove_bg_and_optimize(data: bytes, session) -> bytes | None:
    """Remove background, resize, and convert to transparent WebP.

    Returns None if quality gate fails.
    """
    img = Image.open(io.BytesIO(data))

    # Remove background
    result = remove(img, session=session)
    result = result.convert("RGBA")

    # Quality gate
    alpha = np.array(result.split()[-1])
    opaque_ratio = np.count_nonzero(alpha > 128) / alpha.size

    if opaque_ratio < 0.10 or opaque_ratio > 0.95:
        return None

    # Resize
    result.thumbnail(MAX_SIZE, Image.LANCZOS)

    buf = io.BytesIO()
    result.save(buf, format="WEBP", quality=WEBP_QUALITY)
    return buf.getvalue()


def run(apply: bool = False):
    client = get_minio_client()
    bucket = settings.minio_bucket

    # Load model once
    print("Loading rembg model (u2netp)...")
    session = new_session("u2netp")
    print("Model loaded.\n")

    processed = 0
    skipped_small = 0
    skipped_transparent = 0
    skipped_quality = 0
    failed = 0
    total_before = 0
    total_after = 0

    for category in CATEGORIES:
        prefix = f"{category}/"
        print(f"{'='*60}")
        print(f"Scanning {prefix} ...")

        objects = list(client.list_objects(bucket, prefix=prefix))
        count = len([o for o in objects if not o.is_dir])
        print(f"Found {count} files\n")

        for idx, obj in enumerate(objects, 1):
            if obj.is_dir:
                continue

            size = obj.size or 0
            name = obj.object_name

            # Skip tiny files
            if size < MIN_SIZE:
                skipped_small += 1
                continue

            if not apply:
                print(f"  [{idx}/{count}] {name} ({size/1024:.0f} KB) → would process")
                processed += 1
                continue

            t0 = time.time()

            # Download
            try:
                response = client.get_object(bucket, name)
                data = response.read()
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"  [{idx}/{count}] {name} ✗ download failed: {e}")
                failed += 1
                continue

            # Skip already transparent
            if has_transparency(data):
                print(f"  [{idx}/{count}] {name} — already transparent, skipping")
                skipped_transparent += 1
                continue

            total_before += len(data)

            # Remove background
            try:
                result = remove_bg_and_optimize(data, session)
            except Exception as e:
                print(f"  [{idx}/{count}] {name} ✗ processing failed: {e}")
                failed += 1
                continue

            if result is None:
                print(f"  [{idx}/{count}] {name} — quality gate failed, skipping")
                skipped_quality += 1
                continue

            new_size = len(result)
            total_after += new_size

            # Upload back (same object name)
            try:
                client.put_object(
                    bucket_name=bucket,
                    object_name=name,
                    data=io.BytesIO(result),
                    length=new_size,
                    content_type="image/webp",
                )
            except Exception as e:
                print(f"  [{idx}/{count}] {name} ✗ upload failed: {e}")
                failed += 1
                continue

            elapsed = time.time() - t0
            print(
                f"  [{idx}/{count}] {name} ✓ "
                f"{len(data)/1024:.0f} KB → {new_size/1024:.0f} KB "
                f"({elapsed:.1f}s)"
            )
            processed += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"{'DRY RUN' if not apply else 'DONE'}:")
    print(f"  Processed:              {processed}")
    print(f"  Skipped (< 5 KB):       {skipped_small}")
    print(f"  Skipped (transparent):  {skipped_transparent}")
    print(f"  Skipped (quality gate): {skipped_quality}")
    print(f"  Failed:                 {failed}")
    if apply and total_before > 0:
        print(f"  Before: {total_before / (1024*1024):.1f} MB")
        print(f"  After:  {total_after / (1024*1024):.1f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove backgrounds from existing player/coach photos in MinIO"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually apply changes (default: dry run)"
    )
    args = parser.parse_args()

    run(apply=args.apply)
