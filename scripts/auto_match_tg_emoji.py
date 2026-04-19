"""Auto-match KazakhstanFootballClubs pack emojis to PL-2026 teams by logo similarity.

For each team in PL-2026:
  - download its logo_url (MinIO public URL)
  - compute pHash
  - compare against all 35 webp previews in scripts/kff_emoji_previews/
  - pick closest match (smallest hamming distance)

Output: print suggested mapping + hamming distance. User eyeballs and confirms.
"""
from __future__ import annotations

import asyncio
import io
from pathlib import Path

import httpx
import imagehash
from dotenv import load_dotenv
from PIL import Image
from sqlalchemy import distinct, select

from app.database import AsyncSessionLocal
from app.models import Game, Team

PREVIEWS_DIR = Path(__file__).parent / "kff_emoji_previews"


def load_hashes_from_previews() -> dict[str, imagehash.ImageHash]:
    out: dict[str, imagehash.ImageHash] = {}
    for fp in PREVIEWS_DIR.glob("*.webp"):
        try:
            img = Image.open(fp).convert("RGBA")
            # white background for consistent pHash
            bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            out[fp.stem] = imagehash.phash(bg.convert("RGB"), hash_size=16)
        except Exception as e:  # noqa
            print(f"  skip {fp.name}: {e}")
    return out


async def fetch_team_logo_hash(url: str) -> imagehash.ImageHash | None:
    # redirect localhost MinIO to prod public endpoint
    prod_url = url.replace(
        "https://localhost:9000/qfl-files",
        "https://kffleague.kz/storage",
    ).replace(
        "http://localhost:9000/qfl-files",
        "https://kffleague.kz/storage",
    )
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(prod_url)
            r.raise_for_status()
            img = Image.open(io.BytesIO(r.content)).convert("RGBA")
            bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            return imagehash.phash(bg.convert("RGB"), hash_size=16)
    except Exception as e:
        print(f"  logo fetch failed: {e}")
        return None


async def main() -> None:
    load_dotenv(".env", override=True)

    preview_hashes = load_hashes_from_previews()
    print(f"Loaded {len(preview_hashes)} preview hashes\n")

    async with AsyncSessionLocal() as db:
        home = select(distinct(Game.home_team_id)).where(Game.season_id == 200)
        away = select(distinct(Game.away_team_id)).where(Game.season_id == 200)
        ids = set((await db.execute(home)).scalars().all()) | set(
            (await db.execute(away)).scalars().all()
        )
        ids.discard(None)
        teams = (
            await db.execute(
                select(Team).where(Team.id.in_(ids)).order_by(Team.name_kz)
            )
        ).scalars().all()

        print(f"{'team':<22} {'doc_id':<22} {'dist':<6} logo")
        print("-" * 90)
        suggestions: dict[str, str] = {}
        for t in teams:
            if not t.logo_url:
                print(f"{(t.name_kz or t.name):<22} {'(no logo)':<22}")
                continue
            team_hash = await fetch_team_logo_hash(t.logo_url)
            if team_hash is None:
                continue
            scored = sorted(
                ((doc_id, team_hash - h) for doc_id, h in preview_hashes.items()),
                key=lambda x: x[1],
            )
            best_id, best_dist = scored[0]
            suggestions[t.name_kz or t.name] = best_id
            print(
                f"{(t.name_kz or t.name):<22} {best_id:<22} {best_dist:<6} {t.logo_url[-40:]}"
            )

        print("\n--- mapping (copy-paste to confirm) ---")
        for name, doc_id in suggestions.items():
            print(f"  {name}: {doc_id}")


if __name__ == "__main__":
    asyncio.run(main())
