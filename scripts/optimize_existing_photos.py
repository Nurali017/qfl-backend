"""One-time script to optimize existing player/coach photos in MinIO.

Resizes oversized images to max 800x1200 and converts to WebP.
Updates photo_url in `players`, `player_teams`, and `coaches` tables
when the file extension changes (e.g. .JPG → .webp).

Usage:
    # Dry run (default) — shows what would be done
    python -m scripts.optimize_existing_photos

    # Actually optimize
    python -m scripts.optimize_existing_photos --apply
"""

import argparse
import io
import sys
from pathlib import PurePosixPath

from PIL import Image

# ── Bootstrap app config ──
sys.path.insert(0, str(PurePosixPath(__file__).parent.parent))

from app.config import get_settings  # noqa: E402
from app.minio_client import get_minio_client  # noqa: E402

settings = get_settings()

MAX_SIZE = (800, 1200)
WEBP_QUALITY = 85
SIZE_THRESHOLD = 500 * 1024  # 500 KB

CATEGORIES = ["player_photos", "coach_photos"]


def optimize_image(data: bytes) -> bytes:
    """Resize and convert to WebP."""
    img = Image.open(io.BytesIO(data))

    if img.mode in ("RGBA", "LA", "P"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "P":
            img = img.convert("RGBA")
        bg.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
        img = bg
    elif img.mode != "RGB":
        img = img.convert("RGB")

    img.thumbnail(MAX_SIZE, Image.LANCZOS)

    buf = io.BytesIO()
    img.save(buf, format="WEBP", quality=WEBP_QUALITY)
    return buf.getvalue()


def get_webp_object_name(object_name: str) -> str:
    """Replace extension with .webp."""
    base, _, _ = object_name.rpartition(".")
    return f"{base}.webp" if base else f"{object_name}.webp"


def run(apply: bool = False):
    client = get_minio_client()
    bucket = settings.minio_bucket

    total_before = 0
    total_after = 0
    optimized_count = 0
    skipped_count = 0
    db_updates: list[tuple[str, str]] = []  # (old_object_name, new_object_name)

    for category in CATEGORIES:
        prefix = f"{category}/"
        print(f"\n{'='*60}")
        print(f"Scanning {prefix} ...")

        for obj in client.list_objects(bucket, prefix=prefix):
            if obj.is_dir:
                continue

            size = obj.size or 0
            name = obj.object_name

            if size < SIZE_THRESHOLD:
                skipped_count += 1
                continue

            size_mb = size / (1024 * 1024)
            print(f"\n  {name}  ({size_mb:.1f} MB)")
            total_before += size

            if not apply:
                new_name = get_webp_object_name(name)
                print(f"    → would optimize to {new_name}")
                if new_name != name:
                    db_updates.append((name, new_name))
                optimized_count += 1
                continue

            # Download
            try:
                response = client.get_object(bucket, name)
                data = response.read()
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"    ✗ download failed: {e}")
                continue

            # Optimize
            try:
                webp_data = optimize_image(data)
            except Exception as e:
                print(f"    ✗ optimization failed: {e}")
                continue

            new_name = get_webp_object_name(name)
            new_size = len(webp_data)
            total_after += new_size

            # Upload optimized
            try:
                client.put_object(
                    bucket_name=bucket,
                    object_name=new_name,
                    data=io.BytesIO(webp_data),
                    length=new_size,
                    content_type="image/webp",
                )
            except Exception as e:
                print(f"    ✗ upload failed: {e}")
                continue

            # Delete old file if name changed
            if new_name != name:
                try:
                    client.remove_object(bucket, name)
                except Exception:
                    print(f"    ⚠ could not delete old file {name}")
                db_updates.append((name, new_name))

            ratio = (1 - new_size / size) * 100
            print(f"    ✓ {size_mb:.1f} MB → {new_size/1024:.0f} KB  (-{ratio:.0f}%)")
            optimized_count += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"{'DRY RUN' if not apply else 'DONE'}:")
    print(f"  Optimized: {optimized_count}")
    print(f"  Skipped (< {SIZE_THRESHOLD//1024} KB): {skipped_count}")
    if apply and total_before > 0:
        print(f"  Before: {total_before / (1024*1024):.1f} MB")
        print(f"  After:  {total_after / (1024*1024):.1f} MB")
        print(f"  Saved:  {(total_before - total_after) / (1024*1024):.1f} MB")

    # DB updates needed
    if db_updates:
        print(f"\n  DB updates needed ({len(db_updates)} rows):")
        print("  Run these SQL statements to update photo_url references:\n")
        for old, new in db_updates:
            print(f"    UPDATE players SET photo_url = '{new}' WHERE photo_url = '{old}';")
            print(f"    UPDATE player_teams SET photo_url = '{new}' WHERE photo_url = '{old}';")
            print(f"    UPDATE coaches SET photo_url = '{new}' WHERE photo_url = '{old}';")
        print()

        if apply:
            _apply_db_updates(db_updates)


def _apply_db_updates(updates: list[tuple[str, str]]):
    """Apply photo_url updates directly via SQLAlchemy."""
    try:
        from sqlalchemy import create_engine, text

        sync_url = settings.database_url.replace("+asyncpg", "")
        engine = create_engine(sync_url)

        with engine.begin() as conn:
            for old_name, new_name in updates:
                for table in ("players", "player_teams", "coaches"):
                    result = conn.execute(
                        text(f"UPDATE {table} SET photo_url = :new WHERE photo_url = :old"),
                        {"old": old_name, "new": new_name},
                    )
                    if result.rowcount > 0:
                        print(f"    Updated {result.rowcount} row(s) in {table}: {old_name} → {new_name}")

        engine.dispose()
        print("  DB updates applied successfully.")
    except Exception as e:
        print(f"  ✗ DB update failed: {e}")
        print("  Run the SQL statements above manually.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimize existing photos in MinIO")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes (default: dry run)")
    args = parser.parse_args()

    run(apply=args.apply)
