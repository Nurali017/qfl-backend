#!/usr/bin/env python3
"""
Sync player photos from legacy MySQL (Bitrix CMS) to QFL PostgreSQL + MinIO.

The legacy MySQL database is the source of truth for photo-to-player assignments.
This script compares players by legacy_id (primary) or sota_id (fallback),
converts photos to WebP, uploads to MinIO, and updates QFL photo_url.

Run from host machine (not Docker) — all services are port-mapped locally.

Usage:
    # Dry-run with CSV report (default — no changes made)
    python scripts/sync_legacy_player_photos.py --dry-run --csv /tmp/photo_report.csv

    # Test with small batch
    python scripts/sync_legacy_player_photos.py --apply --limit 5

    # Fill gaps only (default — don't replace existing QFL photos)
    python scripts/sync_legacy_player_photos.py --apply

    # Full sync (force replace existing photos)
    python scripts/sync_legacy_player_photos.py --apply --force-replace
"""

import argparse
import csv
import io
import json
import subprocess
import sys
from pathlib import Path

import psycopg2
from minio import Minio
from PIL import Image

# ---------------------------------------------------------------------------
# Connection constants
# ---------------------------------------------------------------------------
# MySQL is accessed via docker exec (local Homebrew MySQL conflicts on port 3306)
MYSQL_CONTAINER = "kff-mysql"
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpassword"
MYSQL_DB = "p-348074_db"

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

LEGACY_PHOTOS_BASE = Path(
    "/Users/nuralisagyndykuly/ftp-backups/kffleague/httpdocs/public/upload/people"
)

WEBP_QUALITY = 85

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def convert_to_webp(image_path: Path) -> bytes:
    """Read an image file, convert to WebP bytes (RGB, white background)."""
    img = Image.open(image_path)
    if img.mode == "RGBA":
        background = Image.new("RGB", img.size, (255, 255, 255))
        background.paste(img, mask=img.split()[3])
        img = background
    elif img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="WEBP", quality=WEBP_QUALITY)
    return buf.getvalue()


def resolve_legacy_photo_path(photo_value: str) -> Path | None:
    """Resolve the legacy photo column value to an absolute file path."""
    if not photo_value:
        return None
    path = LEGACY_PHOTOS_BASE / photo_value
    if path.is_file():
        return path
    return None


def upload_to_minio(client: Minio, object_name: str, data: bytes, content_type: str = "image/webp"):
    """Upload bytes to MinIO."""
    buf = io.BytesIO(data)
    client.put_object(
        MINIO_BUCKET,
        object_name,
        buf,
        length=len(data),
        content_type=content_type,
    )


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def mysql_query(sql: str) -> list[dict]:
    """Run a MySQL query via docker exec and return rows as dicts."""
    cmd = [
        "docker", "exec", MYSQL_CONTAINER,
        "mysql", f"-u{MYSQL_USER}", f"-p{MYSQL_PASSWORD}",
        MYSQL_DB, "--default-character-set=utf8mb4",
        "-e", sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        stderr = result.stderr.replace("mysql: [Warning] Using a password on the command line interface can be insecure.\n", "")
        if stderr.strip():
            raise RuntimeError(f"MySQL query failed: {stderr.strip()}")
    output = result.stdout.strip()
    if not output:
        return []
    lines = output.split("\n")
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        vals = line.split("\t")
        row = {}
        for h, v in zip(headers, vals):
            row[h] = None if v == "NULL" else v
        rows.append(row)
    return rows


def fetch_mysql_players() -> dict:
    """Fetch all people from MySQL with the best available photo.

    Photo priority (per player):
      1. contracts.photo from the latest season_id  (webp files in root dir)
      2. people.photo with the freshest b_file.TIMESTAMP_X

    When duplicate sota_id entries exist, de-duplicates by picking the record
    with the best photo.

    Returns dict keyed by people.id.
    """
    # --- Fetch contract photos (latest season per player) ---
    contract_rows = mysql_query(
        "SELECT c.people_id, c.photo, c.season_id "
        "FROM contracts c "
        "WHERE c.photo IS NOT NULL AND c.photo <> '' "
        "ORDER BY c.season_id DESC"
    )
    # Keep only the latest-season contract photo per people_id
    contract_photo: dict[int, str] = {}
    for cr in contract_rows:
        pid = int(cr["people_id"])
        if pid not in contract_photo:
            contract_photo[pid] = cr["photo"]

    # --- Fetch people with b_file timestamps for fallback ---
    rows = mysql_query(
        "SELECT p.id, p.photo, p.sota_id, p.name_ru, p.surname_ru, "
        "f.TIMESTAMP_X as photo_ts "
        "FROM people p "
        "LEFT JOIN b_file f ON CONCAT(f.SUBDIR, '/', f.FILE_NAME) = p.photo"
    )
    for row in rows:
        row["id"] = int(row["id"])
        # Override with contract photo if available (higher priority)
        cp = contract_photo.get(row["id"])
        if cp:
            row["photo"] = cp
            row["photo_ts"] = "9999"  # ensure it wins over people.photo

    # De-duplicate by sota_id: keep the record with the freshest photo.
    # For records without a sota_id we just key by their own id (no conflict).
    best: dict[str, dict] = {}
    no_sota: dict[int, dict] = {}

    for row in rows:
        sid = (row.get("sota_id") or "").strip()
        if not sid:
            no_sota[row["id"]] = row
            continue

        existing = best.get(sid)
        if existing is None:
            best[sid] = row
            continue

        # Prefer the record whose photo has a later timestamp.
        # NULL timestamps (no photo / no b_file match) lose to any real ts.
        new_ts = row.get("photo_ts") or ""
        old_ts = existing.get("photo_ts") or ""
        if new_ts > old_ts:
            best[sid] = row

    result = {row["id"]: row for row in best.values()}
    result.update(no_sota)
    return result


def fetch_pg_players(conn) -> list[dict]:
    """Fetch all players from PostgreSQL with legacy_id or sota_id."""
    cur = conn.cursor()
    cur.execute(
        "SELECT id, legacy_id, sota_id::text, photo_url, first_name, last_name "
        "FROM players"
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return rows


# ---------------------------------------------------------------------------
# Matching & classification
# ---------------------------------------------------------------------------

def match_players(mysql_players: dict, pg_players: list[dict]) -> list[dict]:
    """Match QFL players to legacy players and classify photo status."""
    # Build sota_id → mysql_id index for fallback matching
    sota_index = {}
    for mid, mp in mysql_players.items():
        if mp["sota_id"]:
            sota_index[mp["sota_id"].strip()] = mid

    results = []
    for pg in pg_players:
        entry = {
            "qfl_id": pg["id"],
            "qfl_name": f"{pg['first_name'] or ''} {pg['last_name'] or ''}".strip(),
            "qfl_photo": pg["photo_url"],
            "legacy_id": pg["legacy_id"],
            "sota_id": pg["sota_id"],
            "match_method": None,
            "mysql_id": None,
            "mysql_name": None,
            "mysql_photo": None,
            "legacy_file_path": None,
            "classification": None,
        }

        # Try to match: priority legacy_id, fallback sota_id
        mysql_player = None
        if pg["legacy_id"] and pg["legacy_id"] in mysql_players:
            mysql_player = mysql_players[pg["legacy_id"]]
            entry["match_method"] = "legacy_id"
        elif pg["sota_id"] and pg["sota_id"] in sota_index:
            mysql_player = mysql_players[sota_index[pg["sota_id"]]]
            entry["match_method"] = "sota_id"

        if mysql_player is None:
            entry["classification"] = "no_legacy_match"
            results.append(entry)
            continue

        entry["mysql_id"] = mysql_player["id"]
        entry["mysql_name"] = f"{mysql_player['name_ru'] or ''} {mysql_player['surname_ru'] or ''}".strip()
        entry["mysql_photo"] = mysql_player["photo"]

        legacy_has_photo = bool(mysql_player["photo"])
        qfl_has_photo = bool(pg["photo_url"])

        if not legacy_has_photo and not qfl_has_photo:
            entry["classification"] = "both_missing_photo"
        elif not legacy_has_photo and qfl_has_photo:
            entry["classification"] = "legacy_missing_photo"
        elif legacy_has_photo:
            file_path = resolve_legacy_photo_path(mysql_player["photo"])
            if file_path is None:
                entry["classification"] = "legacy_file_missing"
            else:
                entry["legacy_file_path"] = str(file_path)
                if qfl_has_photo:
                    entry["classification"] = "both_have_photos"
                else:
                    entry["classification"] = "qfl_missing_photo"

        results.append(entry)
    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(results: list[dict]):
    """Print classification summary."""
    counts: dict[str, int] = {}
    for r in results:
        cls = r["classification"]
        counts[cls] = counts.get(cls, 0) + 1

    print("\n=== Classification Summary ===")
    for cls in sorted(counts.keys()):
        print(f"  {cls}: {counts[cls]}")
    print(f"  TOTAL: {len(results)}")

    actionable = sum(1 for r in results if r["classification"] in ("both_have_photos", "qfl_missing_photo"))
    print(f"\n  Actionable (replace + add): {actionable}")


def write_csv(results: list[dict], path: str):
    """Write results to CSV."""
    fields = [
        "qfl_id", "qfl_name", "qfl_photo", "legacy_id", "sota_id",
        "match_method", "mysql_id", "mysql_name", "mysql_photo",
        "legacy_file_path", "classification",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV report written to: {path}")


# ---------------------------------------------------------------------------
# Apply changes
# ---------------------------------------------------------------------------

def apply_changes(results: list[dict], pg_conn, minio_client: Minio, skip_existing: bool, limit: int | None):
    """Upload photos to MinIO and update PostgreSQL."""
    actionable = [
        r for r in results
        if r["classification"] in ("both_have_photos", "qfl_missing_photo")
    ]

    if skip_existing:
        actionable = [r for r in actionable if r["classification"] == "qfl_missing_photo"]
        print(f"\n--skip-existing: processing only {len(actionable)} players without QFL photos")

    if limit:
        actionable = actionable[:limit]
        print(f"--limit: processing only {len(actionable)} players")

    if not actionable:
        print("\nNo actionable players to process.")
        return

    print(f"\nProcessing {len(actionable)} players...")

    cur = pg_conn.cursor()
    success = 0
    errors = 0

    for i, entry in enumerate(actionable, 1):
        qfl_id = entry["qfl_id"]
        file_path = Path(entry["legacy_file_path"])
        object_name = f"player_photos/{qfl_id}.webp"

        try:
            webp_data = convert_to_webp(file_path)
            upload_to_minio(minio_client, object_name, webp_data)
            cur.execute(
                "UPDATE players SET photo_url = %s WHERE id = %s",
                (object_name, qfl_id),
            )
            pg_conn.commit()
            success += 1
            action = "replaced" if entry["classification"] == "both_have_photos" else "added"
            print(f"  [{i}/{len(actionable)}] {action} photo for QFL #{qfl_id} ({entry['qfl_name']}) — {len(webp_data)/1024:.0f}KB")
        except Exception as e:
            pg_conn.rollback()
            errors += 1
            print(f"  [{i}/{len(actionable)}] ERROR QFL #{qfl_id} ({entry['qfl_name']}): {e}")

    print(f"\nDone: {success} succeeded, {errors} failed")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sync player photos from legacy MySQL to QFL")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report only, no changes (default safeguard)")
    mode.add_argument("--apply", action="store_true", help="Upload photos and update DB")
    parser.add_argument("--csv", metavar="PATH", help="Export comparison report to CSV")
    parser.add_argument("--limit", type=int, help="Process only N players (for testing)")
    parser.add_argument("--force-replace", action="store_true", help="Replace existing QFL photos (default: skip existing)")
    args = parser.parse_args()

    # --- Connect ---
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB,
    )

    # --- Fetch ---
    print("Fetching MySQL players (via docker exec)...")
    mysql_players = fetch_mysql_players()
    print(f"  Found {len(mysql_players)} legacy players")

    print("Fetching PostgreSQL players...")
    pg_players = fetch_pg_players(pg_conn)
    print(f"  Found {len(pg_players)} QFL players")

    # --- Match & classify ---
    print("Matching players...")
    results = match_players(mysql_players, pg_players)
    print_summary(results)

    # --- CSV report ---
    if args.csv:
        write_csv(results, args.csv)

    # --- Apply ---
    if args.apply:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        skip_existing = not args.force_replace
        apply_changes(results, pg_conn, minio_client, skip_existing, args.limit)
    else:
        print("\nDry-run mode — no changes made. Use --apply to sync photos.")

    pg_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
