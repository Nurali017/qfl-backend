#!/usr/bin/env python3
"""
Import legacy player photos from kffleague FTP backup into MinIO.

Matches QFL players (without photo) to legacy MySQL by legacy_id / sota_id,
reads photo files from disk, uploads to MinIO, and updates players.photo_url.

Usage:
    cd backend
    python3 import_legacy_photos.py              # dry-run (default)
    python3 import_legacy_photos.py --dry-run     # explicit dry-run
    python3 import_legacy_photos.py --apply       # actually import
"""

import argparse
import io
import mimetypes
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

# ---------------------------------------------------------------------------
# Connection constants
# ---------------------------------------------------------------------------
PG_HOST = "localhost"
PG_PORT = 5434
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "qfl_db"

MYSQL_CONTAINER = "kff-mysql"
MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpassword"
MYSQL_DB = "p-348074_db"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "qfl-files"
MINIO_SECURE = False

LEGACY_PHOTOS_BASE = Path(
    "/Users/nuralisagyndykuly/ftp-backups/kffleague/httpdocs/public/upload/people"
)


# ---------------------------------------------------------------------------
# Helpers
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
        stderr = result.stderr.replace(
            "mysql: [Warning] Using a password on the command line interface can be insecure.\n", ""
        )
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


def get_content_type(filename: str) -> str:
    """Determine content type from file extension."""
    ct, _ = mimetypes.guess_type(filename)
    if ct:
        return ct
    ext = Path(filename).suffix.lower()
    return {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
    }.get(ext, "application/octet-stream")


def main():
    parser = argparse.ArgumentParser(description="Import legacy player photos into MinIO")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="Show what would be imported (default)")
    group.add_argument("--apply", action="store_true",
                       help="Actually import photos")
    args = parser.parse_args()

    apply = args.apply

    # ── 1. QFL PostgreSQL: players without photo ─────────────────────────
    print("Connecting to PostgreSQL...")
    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB,
    )
    cur = pg.cursor()

    cur.execute("""
        SELECT id, legacy_id, sota_id::text, first_name, last_name
        FROM players
        WHERE photo_url IS NULL
    """)
    cols = [d[0] for d in cur.description]
    no_photo = [dict(zip(cols, row)) for row in cur.fetchall()]
    print(f"  Players without photo: {len(no_photo)}")

    # ── 2. Legacy MySQL: people + contracts photos ───────────────────────
    print("Fetching legacy MySQL data (via docker exec)...")

    # contracts.photo — latest season per player
    contract_rows = mysql_query(
        "SELECT c.people_id, c.photo, c.season_id "
        "FROM contracts c "
        "WHERE c.photo IS NOT NULL AND c.photo <> '' "
        "ORDER BY c.season_id DESC"
    )
    contract_photo: dict[int, str] = {}
    for cr in contract_rows:
        pid = int(cr["people_id"])
        if pid not in contract_photo:
            contract_photo[pid] = cr["photo"]

    # people table
    people_rows = mysql_query(
        "SELECT p.id, p.photo, p.sota_id, p.name_ru, p.surname_ru "
        "FROM people p"
    )
    legacy: dict[int, dict] = {}
    sota_index: dict[str, int] = {}  # sota_id → people.id

    for row in people_rows:
        pid = int(row["id"])
        best_photo = contract_photo.get(pid) or row["photo"] or None
        photo_source = None
        if contract_photo.get(pid):
            photo_source = "contracts"
        elif row["photo"]:
            photo_source = "people"

        legacy[pid] = {
            "id": pid,
            "name": f"{row['name_ru'] or ''} {row['surname_ru'] or ''}".strip(),
            "photo": best_photo,
            "photo_source": photo_source,
            "sota_id": (row.get("sota_id") or "").strip() or None,
        }
        if legacy[pid]["sota_id"]:
            sota_index[legacy[pid]["sota_id"]] = pid

    print(f"  Legacy: {len(legacy)} people, {len(contract_photo)} with contract photos")

    # ── 3. Match & collect candidates ────────────────────────────────────
    candidates = []
    matched_no_photo = 0
    unmatched = 0

    for p in no_photo:
        legacy_player = None
        match_method = None

        if p["legacy_id"] and p["legacy_id"] in legacy:
            legacy_player = legacy[p["legacy_id"]]
            match_method = "legacy_id"
        elif p["sota_id"] and p["sota_id"] in sota_index:
            legacy_player = legacy[sota_index[p["sota_id"]]]
            match_method = "sota_id"
        else:
            unmatched += 1
            continue

        if not legacy_player["photo"]:
            matched_no_photo += 1
            continue

        file_path = LEGACY_PHOTOS_BASE / legacy_player["photo"]
        if not file_path.is_file():
            continue

        ext = Path(legacy_player["photo"]).suffix.lower().lstrip(".")
        if not ext:
            ext = "jpg"
        object_name = f"player_photos/{uuid.uuid4()}.{ext}"

        candidates.append({
            "qfl_id": p["id"],
            "qfl_name": f"{p['first_name'] or ''} {p['last_name'] or ''}".strip(),
            "match_method": match_method,
            "legacy_id": legacy_player["id"],
            "photo_filename": legacy_player["photo"],
            "photo_source": legacy_player["photo_source"],
            "file_path": file_path,
            "object_name": object_name,
            "content_type": get_content_type(legacy_player["photo"]),
        })

    print(f"  Matched without legacy photo: {matched_no_photo}")
    print(f"  Unmatched: {unmatched}")
    print(f"  Candidates to import: {len(candidates)}")

    if not candidates:
        print("\nNothing to import.")
        pg.close()
        return

    # ── 4. Dry-run or Apply ──────────────────────────────────────────────
    if not apply:
        print(f"\n{'=' * 65}")
        print(f"DRY-RUN: {len(candidates)} photos would be imported")
        print(f"{'=' * 65}\n")

        by_source = {"contracts": 0, "people": 0}
        by_match = {"legacy_id": 0, "sota_id": 0}
        for c in candidates:
            by_source[c["photo_source"]] = by_source.get(c["photo_source"], 0) + 1
            by_match[c["match_method"]] = by_match.get(c["match_method"], 0) + 1

        print(f"  By match method:  legacy_id={by_match['legacy_id']}, sota_id={by_match['sota_id']}")
        print(f"  By photo source:  contracts={by_source.get('contracts', 0)}, people={by_source.get('people', 0)}")

        print(f"\n  Listing all {len(candidates)} candidates:")
        for c in candidates:
            print(f"    QFL #{c['qfl_id']:>5}  {c['qfl_name']:<30}  "
                  f"match={c['match_method']:<10}  legacy={c['legacy_id']:<6}  "
                  f"src={c['photo_source']:<10}  {c['photo_filename']}")

        print(f"\nRun with --apply to import.")
        pg.close()
        return

    # ── Apply mode ───────────────────────────────────────────────────────
    from minio import Minio

    print(f"\n{'=' * 65}")
    print(f"IMPORTING {len(candidates)} photos...")
    print(f"{'=' * 65}\n")

    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    uploaded = 0
    errors = 0
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for i, c in enumerate(candidates, 1):
        try:
            file_data = c["file_path"].read_bytes()
            data_stream = io.BytesIO(file_data)

            client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=c["object_name"],
                data=data_stream,
                length=len(file_data),
                content_type=c["content_type"],
                metadata={
                    "original-filename": c["photo_filename"],
                    "uploaded-at": now_str,
                    "category": "player_photos",
                    "legacy-people-id": str(c["legacy_id"]),
                },
            )

            cur.execute(
                "UPDATE players SET photo_url = %s WHERE id = %s",
                (c["object_name"], c["qfl_id"]),
            )

            uploaded += 1
            if i % 50 == 0 or i == len(candidates):
                print(f"  [{i}/{len(candidates)}] uploaded {uploaded}, errors {errors}")

        except Exception as e:
            errors += 1
            print(f"  ERROR QFL #{c['qfl_id']} ({c['qfl_name']}): {e}")

    if errors == 0:
        pg.commit()
        print(f"\nCommitted {uploaded} updates to PostgreSQL.")
    else:
        print(f"\n{errors} errors encountered.")
        pg.commit()
        print(f"Committed {uploaded} successful updates to PostgreSQL.")

    pg.close()

    print(f"\n{'=' * 65}")
    print(f"DONE: {uploaded} photos imported, {errors} errors")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
