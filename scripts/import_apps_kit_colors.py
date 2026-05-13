"""Import per-match kit colours from apps.kffleague.kz into QFL games.

Source can be a MySQL dump of the `p-348074_apps` database (offline backup) or
the live MariaDB.  For each fixture we resolve the field-player shirt of each
team to a uniform PNG icon, take its dominant colour, and write it to
`games.home_kit_color` / `games.away_kit_color` (matching the QFL game by date +
team names).

Usage:
    # dry-run from the FTP backup dump (default — prints a report, writes nothing)
    python -m scripts.import_apps_kit_colors \
        --dump /path/to/p-348074_apps_2026-02-18_10-50-26.sql \
        --uniforms-dir /path/to/apps.kffleague.kz/login/uploads/uniforms

    # actually apply
    python -m scripts.import_apps_kit_colors --dump ... --uniforms-dir ... --apply

    # from the live MariaDB (uses APPS_MYSQL_* settings); fetch PNGs over HTTP
    python -m scripts.import_apps_kit_colors --from-mysql --since 2025-01-01 --apply

    # also mirror the touched uniform PNGs into MinIO under kits/uniforms/
    python -m scripts.import_apps_kit_colors --dump ... --uniforms-dir ... --apply --upload-images
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from datetime import date, datetime, time
from pathlib import Path

# Bootstrap app config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import AsyncSessionLocal  # noqa: E402
from app.services.kit_color_sync import AppsMatchKit, sync_kits_for_matches  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("import_apps_kit_colors")


# ---------------------------------------------------------------------------
# minimal MySQL dump parsing
# ---------------------------------------------------------------------------

def _parse_create_columns(sql_text: str, table: str) -> list[str]:
    """Return the column names of `CREATE TABLE \`table\` ( ... )` in order."""
    m = re.search(
        r"CREATE TABLE `" + re.escape(table) + r"` \((.*?)\)\s*ENGINE",
        sql_text,
        re.S,
    )
    if not m:
        raise ValueError(f"CREATE TABLE `{table}` not found in dump")
    cols: list[str] = []
    for line in m.group(1).splitlines():
        line = line.strip()
        cm = re.match(r"`([^`]+)`", line)
        if cm:
            cols.append(cm.group(1))
    if not cols:
        raise ValueError(f"No columns parsed for `{table}`")
    return cols


def _iter_value_tuples(values_blob: str):
    """Yield Python tuples from a MySQL ``VALUES (...),(...),...`` blob."""
    i, n = 0, len(values_blob)
    while i < n:
        # skip to next '('
        while i < n and values_blob[i] != "(":
            i += 1
        if i >= n:
            return
        i += 1  # past '('
        row: list = []
        cur: list[str] = []
        in_str = False
        while i < n:
            ch = values_blob[i]
            if in_str:
                if ch == "\\" and i + 1 < n:
                    cur.append(values_blob[i + 1])
                    i += 2
                    continue
                if ch == "'":
                    if i + 1 < n and values_blob[i + 1] == "'":  # '' escape
                        cur.append("'")
                        i += 2
                        continue
                    in_str = False
                    i += 1
                    continue
                cur.append(ch)
                i += 1
                continue
            # not in string
            if ch == "'":
                in_str = True
                i += 1
                continue
            if ch in ",)":
                token = "".join(cur).strip()
                cur = []
                if token.upper() == "NULL":
                    row.append(None)
                elif token == "":
                    row.append("")
                else:
                    row.append(token)
                i += 1
                if ch == ")":
                    break
                continue
            cur.append(ch)
            i += 1
        yield tuple(row)


def _table_rows(sql_text: str, table: str) -> list[dict]:
    """Parse all `INSERT INTO \`table\` VALUES (...);` rows into dicts."""
    cols = _parse_create_columns(sql_text, table)
    rows: list[dict] = []
    for m in re.finditer(
        r"INSERT INTO `" + re.escape(table) + r"`(?:\s*\([^)]*\))?\s+VALUES\s+(.*?);\s*\n",
        sql_text,
        re.S,
    ):
        for tup in _iter_value_tuples(m.group(1)):
            if len(tup) != len(cols):
                logger.warning(
                    "%s: row has %d values, expected %d — skipping", table, len(tup), len(cols)
                )
                continue
            rows.append(dict(zip(cols, tup)))
    return rows


def _to_int(v) -> int | None:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _parse_dt(v) -> tuple[date | None, time | None]:
    if isinstance(v, datetime):
        return v.date(), v.time()
    s = str(v or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date(), (dt.time() if " " in s else None)
        except ValueError:
            continue
    return None, None


def records_from_dump(dump_path: Path) -> list[AppsMatchKit]:
    text = dump_path.read_text(encoding="utf-8", errors="replace")
    commands = {_to_int(r["id"]): r for r in _table_rows(text, "commands")}
    uniforms = {_to_int(r["id"]): r for r in _table_rows(text, "commands_uniforms")}

    # latest valid field-shirt uniform ref per (match_id, command_id)
    tshirt_by_pair: dict[tuple[int, int], int] = {}
    for r in _table_rows(text, "match_uniform"):
        mid, cid, ts = _to_int(r["match_id"]), _to_int(r["command_id"]), _to_int(r["t_shirt"])
        if mid is None or cid is None or not ts:
            continue
        tshirt_by_pair[(mid, cid)] = ts  # rows are in id order → latest wins

    records: list[AppsMatchKit] = []
    for r in _table_rows(text, "matches"):
        mid = _to_int(r["id"])
        cmd1, cmd2 = _to_int(r["command_1"]), _to_int(r["command_2"])
        if mid is None or cmd1 is None or cmd2 is None:
            continue
        m_date, m_time = _parse_dt(r["datetime"])
        if m_date is None:
            continue
        home_uid = tshirt_by_pair.get((mid, cmd1))
        away_uid = tshirt_by_pair.get((mid, cmd2))
        home_u = uniforms.get(home_uid) if home_uid else None
        away_u = uniforms.get(away_uid) if away_uid else None
        home_img = (home_u or {}).get("image") or None
        away_img = (away_u or {}).get("image") or None
        if not home_img and not away_img:
            continue
        records.append(
            AppsMatchKit(
                apps_match_id=mid,
                match_date=m_date,
                match_time=m_time,
                home_name=str((commands.get(cmd1) or {}).get("title") or ""),
                away_name=str((commands.get(cmd2) or {}).get("title") or ""),
                home_image=home_img,
                away_image=away_img,
                home_color_name=(home_u or {}).get("color_name") or None,
                away_color_name=(away_u or {}).get("color_name") or None,
            )
        )
    return records


# ---------------------------------------------------------------------------
# PNG fetchers
# ---------------------------------------------------------------------------

def make_dir_png_fetcher(uniforms_dir: Path):
    def _fetch(filename: str) -> bytes | None:
        p = uniforms_dir / Path(filename).name
        if not p.is_file():
            return None
        try:
            return p.read_bytes()
        except OSError:
            return None
    return _fetch


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def _print_report_row(rec: AppsMatchKit, game, home_hex, away_hex):
    status = f"game={game.id}" if game is not None else "UNMATCHED"
    print(
        f"  [{rec.apps_match_id:>5}] {rec.match_date} {rec.home_name:<22} vs {rec.away_name:<22} "
        f"home={home_hex or '-':<8} away={away_hex or '-':<8} -> {status}"
    )


async def _run(args: argparse.Namespace) -> int:
    # 1) build records
    if args.from_mysql:
        from app.services.apps_kit_mysql_sync import fetch_apps_match_kits, make_url_png_fetcher

        since = datetime.strptime(args.since, "%Y-%m-%d").date() if args.since else date(2024, 1, 1)
        until = datetime.strptime(args.until, "%Y-%m-%d").date() if args.until else date(2099, 1, 1)
        records = fetch_apps_match_kits(date_from=since, date_to=until)
        fetch_png = make_dir_png_fetcher(Path(args.uniforms_dir)) if args.uniforms_dir else make_url_png_fetcher()
    else:
        if not args.dump:
            print("error: provide --dump <sql> or --from-mysql", file=sys.stderr)
            return 2
        records = records_from_dump(Path(args.dump))
        if args.uniforms_dir:
            fetch_png = make_dir_png_fetcher(Path(args.uniforms_dir))
        else:
            from app.services.apps_kit_mysql_sync import make_url_png_fetcher
            fetch_png = make_url_png_fetcher()

    logger.info("Built %d apps fixtures with uniform data", len(records))

    # Optional image mirroring needs to remember which files were used.
    used_images: set[str] = set()
    if args.upload_images:
        orig_fetch = fetch_png

        def _tracking_fetch(filename: str) -> bytes | None:
            data = orig_fetch(filename)
            if data is not None:
                used_images.add(Path(filename).name)
            return data
        fetch_png = _tracking_fetch

    dry_run = not args.apply
    async with AsyncSessionLocal() as db:
        result = await sync_kits_for_matches(
            db,
            records,
            fetch_png=fetch_png,
            allow_overwrite=True,
            dry_run=dry_run,
            on_match=_print_report_row,
        )

    print("\n=== summary ===")
    for k, v in result.as_dict().items():
        print(f"  {k}: {v}")
    if result.unmatched:
        print(f"\n  unmatched fixtures ({len(result.unmatched)}):")
        for mid, label in result.unmatched:
            print(f"    [{mid}] {label}")
    if dry_run:
        print("\n(dry-run — nothing written. Re-run with --apply to persist.)")

    # 2) optionally mirror uniform PNGs into MinIO
    if args.apply and args.upload_images and used_images:
        from io import BytesIO

        from PIL import Image

        from app.config import get_settings
        from app.minio_client import get_minio_client, get_public_url

        client = get_minio_client()
        bucket_name = get_settings().minio_bucket
        uploaded = 0
        for name in sorted(used_images):
            data = fetch_png(name)
            if not data:
                continue
            object_name = f"kits/uniforms/{name}"
            try:
                # normalise to PNG via Pillow (some files may be odd)
                buf = BytesIO()
                Image.open(BytesIO(data)).convert("RGBA").save(buf, "PNG")
                payload = buf.getvalue()
                client.put_object(
                    bucket_name, object_name, BytesIO(payload), length=len(payload),
                    content_type="image/png",
                )
                uploaded += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to upload %s: %s", object_name, exc)
        print(f"\n  mirrored {uploaded}/{len(used_images)} uniform PNGs to MinIO under kits/uniforms/")
        if uploaded:
            print(f"  e.g. {get_public_url('kits/uniforms/' + sorted(used_images)[0])}")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--dump", help="path to a p-348074_apps SQL dump")
    src.add_argument("--from-mysql", action="store_true", help="read from the live apps MariaDB (APPS_MYSQL_*)")
    ap.add_argument("--uniforms-dir", help="local dir with uniform PNGs (else fetched over HTTP)")
    ap.add_argument("--since", help="--from-mysql: only fixtures on/after this date (YYYY-MM-DD)")
    ap.add_argument("--until", help="--from-mysql: only fixtures on/before this date (YYYY-MM-DD)")
    ap.add_argument("--apply", action="store_true", help="actually write to the QFL DB (default: dry-run)")
    ap.add_argument("--upload-images", action="store_true", help="also mirror touched uniform PNGs to MinIO")
    args = ap.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
