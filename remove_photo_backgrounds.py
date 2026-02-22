#!/usr/bin/env python3
"""
Remove backgrounds from player photos using rembg (U2-Net neural network).

Downloads photos from MinIO, removes background, saves as WebP with transparent
alpha channel, and uploads back. Originals are backed up to player_photos_originals/.

Prerequisites:
    pip install rembg[cpu] Pillow

Usage:
    cd backend
    python3 remove_photo_backgrounds.py              # dry-run (default)
    python3 remove_photo_backgrounds.py --apply       # process all
    python3 remove_photo_backgrounds.py --apply --player-id 123  # single player
    python3 remove_photo_backgrounds.py --apply --force  # re-process already done
    python3 remove_photo_backgrounds.py --apply --quality 90  # custom WebP quality
"""

import argparse
import io
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

# ---------------------------------------------------------------------------
# Connection constants (same as import_legacy_photos.py)
# ---------------------------------------------------------------------------
PG_HOST = "localhost"
PG_PORT = 5434
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "qfl_db"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "qfl-files"
MINIO_SECURE = False

BACKUP_PREFIX = "player_photos_originals"
PHOTO_PREFIX = "player_photos"
DEFAULT_WEBP_QUALITY = 85


def main():
    parser = argparse.ArgumentParser(
        description="Remove backgrounds from player photos using rembg"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run", action="store_true", default=True,
        help="Show what would be processed (default)",
    )
    group.add_argument(
        "--apply", action="store_true",
        help="Actually process photos",
    )
    parser.add_argument(
        "--player-id", type=int, default=None,
        help="Process only this player ID",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-process photos even if already done",
    )
    parser.add_argument(
        "--quality", type=int, default=DEFAULT_WEBP_QUALITY,
        help=f"WebP quality 1-100 (default: {DEFAULT_WEBP_QUALITY})",
    )
    args = parser.parse_args()
    apply = args.apply

    # ── 1. Query PostgreSQL for players with photos ──────────────────────
    print("Connecting to PostgreSQL...")
    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB,
    )
    cur = pg.cursor()

    sql = """
        SELECT id, first_name, last_name, photo_url
        FROM players
        WHERE photo_url IS NOT NULL
    """
    params = []
    if args.player_id:
        sql += " AND id = %s"
        params.append(args.player_id)
    sql += " ORDER BY id"

    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    players = [dict(zip(cols, row)) for row in cur.fetchall()]
    print(f"  Players with photos: {len(players)}")

    if not players:
        print("\nNo players with photos found.")
        pg.close()
        return

    # ── 2. Connect to MinIO, check metadata ──────────────────────────────
    from minio import Minio
    from minio.error import S3Error

    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    candidates = []
    already_processed = 0
    missing = 0

    for p in players:
        object_name = p["photo_url"]
        # photo_url in DB is the object name (e.g. player_photos/uuid.jpg)
        # FileUrlType resolves to full URL on ORM read, but raw SQL returns the stored value

        try:
            stat = client.stat_object(MINIO_BUCKET, object_name)
        except S3Error as e:
            if e.code == "NoSuchKey":
                missing += 1
                continue
            raise

        # Check if already processed via metadata
        meta = stat.metadata or {}
        if meta.get("x-amz-meta-bg-removed") == "true" and not args.force:
            already_processed += 1
            continue

        name = f"{p['first_name'] or ''} {p['last_name'] or ''}".strip()
        candidates.append({
            "player_id": p["id"],
            "player_name": name,
            "object_name": object_name,
            "content_type": stat.content_type,
            "size": stat.size,
        })

    print(f"  Already processed (skipped): {already_processed}")
    print(f"  Missing in MinIO: {missing}")
    print(f"  Candidates to process: {len(candidates)}")

    if not candidates:
        print("\nNothing to process.")
        pg.close()
        return

    # ── 3. Dry-run or Apply ──────────────────────────────────────────────
    if not apply:
        print(f"\n{'=' * 70}")
        print(f"DRY-RUN: {len(candidates)} photos would be processed")
        print(f"{'=' * 70}\n")

        total_size = sum(c["size"] for c in candidates)
        print(f"  Total input size: {total_size / 1024 / 1024:.1f} MB")
        print(f"  WebP quality: {args.quality}")
        print()

        for c in candidates:
            size_kb = c["size"] / 1024
            print(
                f"  Player #{c['player_id']:>5}  {c['player_name']:<30}  "
                f"{c['object_name']:<50}  {size_kb:.0f} KB"
            )

        print(f"\nRun with --apply to process.")
        pg.close()
        return

    # ── Apply mode ───────────────────────────────────────────────────────
    print("\nImporting rembg (this may take a moment on first run)...")
    try:
        from rembg import remove, new_session
    except ImportError:
        print(
            "ERROR: rembg is not installed.\n"
            "Install it with: pip install rembg[cpu] Pillow",
            file=sys.stderr,
        )
        pg.close()
        sys.exit(1)

    from PIL import Image

    print("Creating rembg session (loading U2-Net model)...")
    session = new_session("u2net")

    print(f"\n{'=' * 70}")
    print(f"PROCESSING {len(candidates)} photos...")
    print(f"{'=' * 70}\n")

    processed = 0
    errors = 0
    db_updates = 0
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for i, c in enumerate(candidates, 1):
        player_label = f"Player #{c['player_id']} ({c['player_name']})"
        try:
            # a. Download original from MinIO
            response = client.get_object(MINIO_BUCKET, c["object_name"])
            original_data = response.read()
            response.close()
            response.release_conn()

            # b. Back up original to player_photos_originals/
            original_filename = Path(c["object_name"]).name
            backup_name = f"{BACKUP_PREFIX}/{original_filename}"

            # Check if backup already exists (idempotent)
            backup_exists = False
            try:
                client.stat_object(MINIO_BUCKET, backup_name)
                backup_exists = True
            except S3Error:
                pass

            if not backup_exists:
                client.put_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=backup_name,
                    data=io.BytesIO(original_data),
                    length=len(original_data),
                    content_type=c["content_type"],
                    metadata={
                        "original-object-name": c["object_name"],
                        "backed-up-at": now_str,
                    },
                )

            # c. Remove background with rembg
            result_data = remove(original_data, session=session)

            # d. Convert to WebP with alpha channel
            img = Image.open(io.BytesIO(result_data))
            if img.mode != "RGBA":
                img = img.convert("RGBA")

            webp_buffer = io.BytesIO()
            img.save(webp_buffer, format="WEBP", quality=args.quality, lossless=False)
            webp_data = webp_buffer.getvalue()

            # e. Determine new object name (.webp extension)
            stem = Path(c["object_name"]).stem
            parent = str(Path(c["object_name"]).parent)
            new_object_name = f"{parent}/{stem}.webp"

            # f. Upload processed photo to MinIO
            client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=new_object_name,
                data=io.BytesIO(webp_data),
                length=len(webp_data),
                content_type="image/webp",
                metadata={
                    "bg-removed": "true",
                    "bg-removed-at": now_str,
                    "original-size": str(len(original_data)),
                    "original-object-name": c["object_name"],
                },
            )

            # g. Update DB if object name changed (e.g. .jpg -> .webp)
            if new_object_name != c["object_name"]:
                cur.execute(
                    "UPDATE players SET photo_url = %s WHERE id = %s",
                    (new_object_name, c["player_id"]),
                )
                db_updates += 1

                # Delete old object (different name)
                try:
                    client.remove_object(MINIO_BUCKET, c["object_name"])
                except S3Error:
                    pass  # not critical

            processed += 1
            saved_pct = (1 - len(webp_data) / len(original_data)) * 100 if original_data else 0
            if i % 10 == 0 or i == len(candidates):
                print(
                    f"  [{i}/{len(candidates)}] processed {processed}, "
                    f"errors {errors}"
                )
            else:
                print(
                    f"  [{i}/{len(candidates)}] {player_label}: "
                    f"{len(original_data) / 1024:.0f}KB -> {len(webp_data) / 1024:.0f}KB "
                    f"({saved_pct:+.0f}%)"
                )

        except Exception as e:
            errors += 1
            print(f"  ERROR {player_label}: {e}")

    # Commit DB changes
    if db_updates > 0:
        pg.commit()
        print(f"\nCommitted {db_updates} DB updates (photo_url extension changes).")
    else:
        print("\nNo DB updates needed (all photos were already .webp).")

    pg.close()

    print(f"\n{'=' * 70}")
    print(
        f"DONE: {processed} photos processed, {db_updates} DB updates, "
        f"{errors} errors"
    )
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
