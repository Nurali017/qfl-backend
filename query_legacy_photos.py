#!/usr/bin/env python3
"""
Diagnostic: how many player photos can we recover from legacy MySQL?

Connects to QFL PostgreSQL and legacy MySQL (via docker exec),
matches players by legacy_id / sota_id, checks photo availability
in people.photo and contracts.photo, and verifies files on disk.

Usage:
    cd backend && python3 query_legacy_photos.py
"""

import subprocess
from pathlib import Path

import psycopg2

# ---------------------------------------------------------------------------
# Connection constants (same as the rest of the local dev setup)
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


def main():
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
    total_no_photo = len(no_photo)

    cur.execute("SELECT COUNT(*) FROM players")
    total_players = cur.fetchone()[0]

    print(f"  QFL: {total_players} total players, {total_no_photo} without photo\n")

    # ── 2. Legacy MySQL: people + contracts photos ───────────────────────
    print("Fetching legacy MySQL data (via docker exec)...")

    # 2a. contracts.photo — latest season per player
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

    # 2b. people table
    people_rows = mysql_query(
        "SELECT p.id, p.photo, p.sota_id, p.name_ru, p.surname_ru "
        "FROM people p"
    )
    # Build legacy dict keyed by people.id
    legacy: dict[int, dict] = {}
    sota_index: dict[str, int] = {}  # sota_id → people.id

    for row in people_rows:
        pid = int(row["id"])
        # best photo: contracts > people
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
    print(f"  Sota index: {len(sota_index)} people with sota_id\n")

    # ── 3. Matching ──────────────────────────────────────────────────────
    matched_legacy_id = 0
    matched_sota_id = 0
    unmatched = 0

    matched_with_photo = 0
    matched_photo_from_contracts = 0
    matched_photo_from_people = 0
    matched_no_photo = 0

    file_exists = 0
    file_missing = 0

    details = []  # for verbose output

    for p in no_photo:
        legacy_player = None
        match_method = None

        # Primary: legacy_id → people.id
        if p["legacy_id"] and p["legacy_id"] in legacy:
            legacy_player = legacy[p["legacy_id"]]
            match_method = "legacy_id"
            matched_legacy_id += 1
        # Fallback: sota_id → people.sota_id
        elif p["sota_id"] and p["sota_id"] in sota_index:
            legacy_player = legacy[sota_index[p["sota_id"]]]
            match_method = "sota_id"
            matched_sota_id += 1
        else:
            unmatched += 1
            continue

        if not legacy_player["photo"]:
            matched_no_photo += 1
            continue

        matched_with_photo += 1
        if legacy_player["photo_source"] == "contracts":
            matched_photo_from_contracts += 1
        else:
            matched_photo_from_people += 1

        # Check file on disk
        file_path = LEGACY_PHOTOS_BASE / legacy_player["photo"]
        if file_path.is_file():
            file_exists += 1
            details.append({
                "qfl_id": p["id"],
                "qfl_name": f"{p['first_name'] or ''} {p['last_name'] or ''}".strip(),
                "match": match_method,
                "legacy_id": legacy_player["id"],
                "photo": legacy_player["photo"],
                "source": legacy_player["photo_source"],
            })
        else:
            file_missing += 1

    # ── 4. Report ────────────────────────────────────────────────────────
    total_matched = matched_legacy_id + matched_sota_id

    print("=" * 65)
    print("ДИАГНОСТИКА: ФОТО ИЗ LEGACY MySQL")
    print("=" * 65)

    print(f"\n{'Игроков без фото в QFL:':<45} {total_no_photo}")
    print(f"{'  ├─ Сматчены с legacy:':<45} {total_matched}")
    print(f"{'  │   ├─ по legacy_id:':<45} {matched_legacy_id}")
    print(f"{'  │   └─ по sota_id (fallback):':<45} {matched_sota_id}")
    print(f"{'  └─ Не сматчены (нет legacy/sota):':<45} {unmatched}")

    print(f"\n{'Сматченные ({} чел.):':<45}".format(total_matched))
    print(f"{'  ├─ Есть фото в legacy:':<45} {matched_with_photo}")
    print(f"{'  │   ├─ из contracts.photo:':<45} {matched_photo_from_contracts}")
    print(f"{'  │   └─ из people.photo:':<45} {matched_photo_from_people}")
    print(f"{'  └─ Нет фото в legacy:':<45} {matched_no_photo}")

    print(f"\n{'Фото файлы на диске (FTP-бэкап):':<45}")
    print(f"{'  ├─ Файл существует:':<45} {file_exists}")
    print(f"{'  └─ Файл отсутствует:':<45} {file_missing}")

    print()
    print("-" * 65)
    print(f"{'МОЖЕМ ЗАКРЫТЬ (файл есть на диске):':<45} {file_exists}")
    print(f"{'ОСТАНЕТСЯ БЕЗ ФОТО:':<45} {total_no_photo - file_exists}")
    coverage_after = (total_players - total_no_photo + file_exists) / total_players * 100
    coverage_before = (total_players - total_no_photo) / total_players * 100
    print(f"{'Покрытие до:':<45} {coverage_before:.1f}%")
    print(f"{'Покрытие после:':<45} {coverage_after:.1f}%")
    print("-" * 65)

    # Verification
    assert matched_legacy_id + matched_sota_id + unmatched == total_no_photo, \
        "Суммы не сходятся: matched + unmatched != total without photo"

    # ── 5. Sample of actionable players ──────────────────────────────────
    if details:
        print(f"\nПримеры (первые 10 из {len(details)}):")
        for d in details[:10]:
            print(f"  QFL #{d['qfl_id']:>5}  {d['qfl_name']:<30}  "
                  f"match={d['match']:<10}  legacy={d['legacy_id']:<6}  "
                  f"src={d['source']:<10}  {d['photo']}")

    pg.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
