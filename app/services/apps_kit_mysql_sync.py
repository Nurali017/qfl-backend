"""Pull kit colours from the live apps.kffleague.kz MariaDB into QFL games.

Used by the periodic Celery task (`app.tasks.sync_tasks.sync_apps_kit_colors`)
and reusable for one-off runs.  Connects to the clubs' match-ops MariaDB
(`p-348074_apps`), reads `match_uniform` for fixtures in a date window around
today, derives the field-player shirt HEX from the uniform PNG icons, and
updates `games.home_kit_color` / `games.away_kit_color`.

`match_uniform.command_type` (1=home?/2=away?) is unreliable per a comment in
the dump, so the side is determined by matching `command_id` against
`matches.command_1` (home) / `matches.command_2` (away).
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.services.kit_color_sync import AppsMatchKit, SyncResult, sync_kits_for_matches

logger = logging.getLogger(__name__)
settings = get_settings()

_ALMATY = ZoneInfo("Asia/Almaty")
_UNIFORM_CACHE_DIR = Path(tempfile.gettempdir()) / "apps_uniform_cache"


# ---------------------------------------------------------------------------
# MariaDB → AppsMatchKit records
# ---------------------------------------------------------------------------

def _connect():
    import pymysql

    return pymysql.connect(
        host=settings.apps_mysql_host,
        port=settings.apps_mysql_port,
        user=settings.apps_mysql_user,
        password=settings.apps_mysql_password,
        database=settings.apps_mysql_db,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=20,
        cursorclass=pymysql.cursors.Cursor,
    )


def fetch_apps_match_kits(*, date_from: date, date_to: date) -> list[AppsMatchKit]:
    """Read `match_uniform` / `matches` / `commands_uniforms` for the date window."""
    dt_from = datetime.combine(date_from, datetime.min.time())
    dt_to = datetime.combine(date_to, datetime.max.time())

    conn = _connect()
    try:
        with conn.cursor() as cur:
            # 1) latest valid field-shirt uniform ref per (match_id, command_id)
            cur.execute(
                "SELECT match_id, command_id, t_shirt FROM match_uniform "
                "WHERE t_shirt > 0 ORDER BY id"
            )
            tshirt_by_pair: dict[tuple[int, int], int] = {}
            for match_id, command_id, t_shirt in cur.fetchall():
                tshirt_by_pair[(int(match_id), int(command_id))] = int(t_shirt)

            # 2) image filename per commands_uniforms id (only the ones we use)
            uniform_ids = sorted(set(tshirt_by_pair.values()))
            uniform_meta: dict[int, tuple[str | None, str | None]] = {}
            if uniform_ids:
                placeholders = ",".join(["%s"] * len(uniform_ids))
                cur.execute(
                    f"SELECT id, image, color_name FROM commands_uniforms WHERE id IN ({placeholders})",
                    uniform_ids,
                )
                for uid, image, color_name in cur.fetchall():
                    uniform_meta[int(uid)] = (
                        (image or None),
                        (color_name or None),
                    )

            # 3) fixtures in the date window with team names
            cur.execute(
                "SELECT m.id, m.datetime, m.command_1, m.command_2, c1.title, c2.title "
                "FROM matches m "
                "JOIN commands c1 ON c1.id = m.command_1 "
                "JOIN commands c2 ON c2.id = m.command_2 "
                "WHERE m.datetime BETWEEN %s AND %s",
                (dt_from, dt_to),
            )
            records: list[AppsMatchKit] = []
            for mid, mdt, cmd1, cmd2, home_title, away_title in cur.fetchall():
                mid, cmd1, cmd2 = int(mid), int(cmd1), int(cmd2)
                home_uid = tshirt_by_pair.get((mid, cmd1))
                away_uid = tshirt_by_pair.get((mid, cmd2))
                home_img, home_name_color = uniform_meta.get(home_uid, (None, None)) if home_uid else (None, None)
                away_img, away_name_color = uniform_meta.get(away_uid, (None, None)) if away_uid else (None, None)
                if not home_img and not away_img:
                    continue
                m_date = mdt.date() if isinstance(mdt, datetime) else mdt
                m_time = mdt.time() if isinstance(mdt, datetime) else None
                records.append(
                    AppsMatchKit(
                        apps_match_id=mid,
                        match_date=m_date,
                        match_time=m_time,
                        home_name=str(home_title or ""),
                        away_name=str(away_title or ""),
                        home_image=home_img,
                        away_image=away_img,
                        home_color_name=home_name_color,
                        away_color_name=away_name_color,
                    )
                )
            return records
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# uniform PNG fetcher (public URL + disk cache)
# ---------------------------------------------------------------------------

def make_url_png_fetcher(base_url: str | None = None, *, cache_dir: Path | None = None):
    """Return a ``fetch_png(filename) -> bytes | None`` reading from the apps host.

    Successful downloads are cached on disk so repeated runs don't re-fetch the
    same ~150 icons.
    """
    base = (base_url or settings.apps_uniform_base_url).rstrip("/") + "/"
    cdir = cache_dir or _UNIFORM_CACHE_DIR
    cdir.mkdir(parents=True, exist_ok=True)

    def _fetch(filename: str) -> bytes | None:
        safe = os.path.basename(filename)
        if not safe:
            return None
        cached = cdir / safe
        if cached.is_file():
            try:
                return cached.read_bytes()
            except OSError:
                pass
        url = base + safe
        try:
            resp = httpx.get(url, timeout=15.0, follow_redirects=True)
        except httpx.HTTPError as exc:
            logger.warning("Failed to download uniform PNG %s: %s", url, exc)
            return None
        if resp.status_code != 200 or not resp.content:
            logger.warning("Uniform PNG %s returned HTTP %s", url, resp.status_code)
            return None
        try:
            cached.write_bytes(resp.content)
        except OSError:
            pass
        return resp.content

    return _fetch


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

async def sync_recent_kit_colors(
    db: AsyncSession,
    *,
    days_back: int | None = None,
    days_fwd: int | None = None,
    allow_overwrite: bool = True,
) -> SyncResult:
    """Sync kit colours for fixtures around today from the live apps MariaDB."""
    if not settings.apps_mysql_user or not settings.apps_mysql_password:
        raise RuntimeError("APPS_MYSQL_USER / APPS_MYSQL_PASSWORD not configured")

    today = datetime.now(_ALMATY).date()
    db_ = days_back if days_back is not None else settings.apps_kit_sync_days_back
    df_ = days_fwd if days_fwd is not None else settings.apps_kit_sync_days_fwd
    date_from = today - timedelta(days=max(0, db_))
    date_to = today + timedelta(days=max(0, df_))

    records = fetch_apps_match_kits(date_from=date_from, date_to=date_to)
    logger.info(
        "apps kit sync: %d fixtures with uniforms in [%s, %s]",
        len(records), date_from, date_to,
    )
    fetch_png = make_url_png_fetcher()
    result = await sync_kits_for_matches(
        db, records, fetch_png=fetch_png, allow_overwrite=allow_overwrite,
    )
    logger.info("apps kit sync result: %s", result.as_dict())
    if result.unmatched:
        sample = ", ".join(label for _, label in result.unmatched[:10])
        logger.warning(
            "apps kit sync: %d unmatched fixtures (sample: %s)",
            len(result.unmatched), sample,
        )
    return result
