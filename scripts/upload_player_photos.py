"""Bulk upload player photos from local folder to MinIO and update player_teams DB records.

Photos are organized as: team_photos/<team_folder>/<first>_<last>_<number>_<variant>.png
Variant: 1=main, 2=avatar, 3=leaderboard, 4=player_page

Usage:
    # Dry run (default) — shows what would be uploaded
    python -m scripts.upload_player_photos --team Актобе

    # Actually upload
    python -m scripts.upload_player_photos --team Актобе --apply

    # Force re-upload even if already present
    python -m scripts.upload_player_photos --team Актобе --apply --force
"""

import argparse
import asyncio
import io
import logging
import re
import sys
from pathlib import Path, PurePosixPath

from PIL import Image

# Bootstrap app config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_settings  # noqa: E402
from app.minio_client import get_minio_client, get_public_url  # noqa: E402
from app.database import AsyncSessionLocal  # noqa: E402

from sqlalchemy import select, update  # noqa: E402
from app.models.player_team import PlayerTeam  # noqa: E402
from app.models.player import Player  # noqa: E402
from app.models.team import Team  # noqa: E402

settings = get_settings()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Photo source directory ──
PHOTOS_BASE = Path(__file__).resolve().parent.parent.parent / "scripts" / "team_photos"

# ── Kazakh folder name → Russian DB name ──
TEAM_MAP = {
    "Актобе": "Актобе",
    "Астана": "Астана",
    "Жетісу": "Жетысу",
    "Қайрат": "Кайрат",
    "Қызылжар": "Кызылжар",
    "Оқжетпес": "Окжетпес",
    "Ұлытау": "Улытау",
    "Ордабасы": "Ордабасы",
    "Жеңіс": "Женис",
    "Алтай": "Алтай",
    "Тобыл": "Тобыл",
    "Ертіс": "Иртыш",
    "Елімай": "Елимай",
    "Кайсар": "Кайсар",
    "Каспий": "Каспий",
}

SEASON_ID = 200

# Variant config: (db_column, object_suffix, skip_optimization)
VARIANT_CONFIG = {
    1: ("photo_url", "_main.webp", False),
    2: ("photo_url_avatar", "_avatar.png", True),
    3: ("photo_url_leaderboard", "_leaderboard.png", True),
    4: ("photo_url_player_page", "_player_page.png", True),
}

# Filename pattern: firstname_lastname_number_variant.png
# Supports multi-word names like: luis_carlos_almeida_da_cunha_17_3.png
FILE_PATTERN = re.compile(r"^(.+)_(\d+)_([1-4])\.png$")

MAX_SIZE = (800, 1200)
WEBP_QUALITY = 85


def optimize_image(data: bytes) -> tuple[bytes, str]:
    """Resize to max 800x1200 and convert to WebP (no bg removal)."""
    img = Image.open(io.BytesIO(data))
    if img.mode not in ("RGBA", "RGB"):
        img = img.convert("RGBA")
    img.thumbnail(MAX_SIZE, Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="WEBP", quality=WEBP_QUALITY)
    return buf.getvalue(), "image/webp"


def parse_filename(name: str) -> tuple[str, int, int] | None:
    """Parse 'firstname_lastname_number_variant.png' → (name_part, number, variant)."""
    m = FILE_PATTERN.match(name)
    if not m:
        return None
    name_part = m.group(1)  # e.g. "luis_carlos_almeida_da_cunha"
    number = int(m.group(2))
    variant = int(m.group(3))
    return name_part, number, variant


def fuzzy_name_match(file_name: str, db_first: str, db_last: str) -> bool:
    """Check if file name parts roughly match DB English names."""
    file_parts = set(file_name.lower().split("_"))
    db_parts = set()
    for n in (db_first or "", db_last or ""):
        for word in n.lower().split():
            db_parts.add(word)
    if not db_parts:
        return False
    overlap = file_parts & db_parts
    return len(overlap) >= min(2, len(db_parts))


async def run(team_folder: str, apply: bool, force: bool):
    team_ru = TEAM_MAP.get(team_folder, team_folder)
    photos_dir = PHOTOS_BASE / team_folder

    if not photos_dir.is_dir():
        logger.error("Directory not found: %s", photos_dir)
        return

    # Parse all PNG files
    files: list[tuple[Path, str, int, int]] = []
    for f in sorted(photos_dir.glob("*.png")):
        parsed = parse_filename(f.name)
        if parsed is None:
            logger.warning("Skipping unparseable file: %s", f.name)
            continue
        name_part, number, variant = parsed
        files.append((f, name_part, number, variant))

    logger.info("Found %d parseable PNG files in %s", len(files), photos_dir)

    # Fetch roster from DB
    async with AsyncSessionLocal() as db:
        team_result = await db.execute(
            select(Team)
            .join(PlayerTeam, PlayerTeam.team_id == Team.id)
            .where(Team.name == team_ru, PlayerTeam.season_id == SEASON_ID)
            .distinct()
        )
        teams = team_result.scalars().all()
        if not teams:
            logger.error("Team '%s' not found in DB for season %d", team_ru, SEASON_ID)
            return
        if len(teams) > 1:
            logger.error(
                "Multiple teams named '%s' with rosters in season %d: %s",
                team_ru, SEASON_ID, [t.id for t in teams],
            )
            return
        team = teams[0]

        roster_result = await db.execute(
            select(
                PlayerTeam.id, PlayerTeam.number,
                Player.first_name_en, Player.last_name_en,
            )
            .join(Player, Player.id == PlayerTeam.player_id)
            .where(
                PlayerTeam.team_id == team.id,
                PlayerTeam.season_id == SEASON_ID,
                PlayerTeam.is_active == True,  # noqa: E712
            )
        )
        roster_rows = roster_result.all()

    # Build index: number → (binding_id, first_en, last_en)
    roster_by_number: dict[int, list[tuple[int, str, str]]] = {}
    for binding_id, num, first_en, last_en in roster_rows:
        if num is not None:
            roster_by_number.setdefault(num, []).append(
                (binding_id, first_en or "", last_en or "")
            )

    logger.info("Roster: %d active players (team=%s, season=%d)", len(roster_rows), team_ru, SEASON_ID)

    # Match files to bindings
    matched = 0
    skipped = 0
    unmatched = 0
    uploaded = 0
    errors = 0

    client = get_minio_client()
    bucket = settings.minio_bucket

    for filepath, name_part, number, variant in files:
        candidates = roster_by_number.get(number, [])
        binding_id = None

        if len(candidates) == 1:
            binding_id = candidates[0][0]
        elif len(candidates) > 1:
            for bid, first_en, last_en in candidates:
                if fuzzy_name_match(name_part, first_en, last_en):
                    binding_id = bid
                    break
            if binding_id is None:
                binding_id = candidates[0][0]
                logger.warning(
                    "Multiple players with #%d, using first match (binding %d) for %s",
                    number, binding_id, filepath.name,
                )

        if binding_id is None:
            logger.warning("No roster match for %s (number=%d)", filepath.name, number)
            unmatched += 1
            continue

        matched += 1
        db_column, suffix, skip_opt = VARIANT_CONFIG[variant]
        object_name = f"player_photos/{binding_id}{suffix}"

        # Check if already uploaded
        if not force:
            try:
                client.stat_object(bucket, object_name)
                logger.debug("Already exists: %s — skipping", object_name)
                skipped += 1
                continue
            except Exception:
                pass

        if not apply:
            logger.info("[DRY] Would upload %s → %s (column=%s)", filepath.name, object_name, db_column)
            continue

        # Read and optionally optimize
        raw_bytes = filepath.read_bytes()
        content_type = "image/png"

        if not skip_opt:
            try:
                raw_bytes, content_type = optimize_image(raw_bytes)
                logger.info("Optimized %s: %d bytes", filepath.name, len(raw_bytes))
            except Exception:
                logger.warning("Optimization failed for %s, uploading original", filepath.name)

        # Upload to MinIO
        try:
            data = io.BytesIO(raw_bytes)
            client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data,
                length=len(raw_bytes),
                content_type=content_type,
            )
        except Exception as e:
            logger.error("Upload failed for %s: %s", filepath.name, e)
            errors += 1
            continue

        # Update DB
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(PlayerTeam)
                .where(PlayerTeam.id == binding_id)
                .values(**{db_column: object_name})
            )
            await db.commit()

        uploaded += 1
        logger.info("Uploaded %s → %s", filepath.name, object_name)

    logger.info(
        "\nSummary for %s:\n  Matched: %d\n  Uploaded: %d\n  Skipped (exists): %d\n  Unmatched: %d\n  Errors: %d",
        team_folder, matched, uploaded, skipped, unmatched, errors,
    )


def main():
    parser = argparse.ArgumentParser(description="Upload player photos to MinIO")
    parser.add_argument("--team", type=str, help="Team folder name (e.g. 'Актобе'). If omitted, process all teams.")
    parser.add_argument("--apply", action="store_true", help="Actually upload (default is dry-run)")
    parser.add_argument("--force", action="store_true", help="Re-upload even if object exists in MinIO")
    args = parser.parse_args()

    teams = [args.team] if args.team else list(TEAM_MAP.keys())

    for team_folder in teams:
        asyncio.run(run(team_folder, apply=args.apply, force=args.force))


if __name__ == "__main__":
    main()
