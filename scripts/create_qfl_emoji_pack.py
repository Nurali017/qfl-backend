"""End-to-end: download team logos → resize → create Telegram emoji pack.

Produces a custom emoji pack owned by the logged-in user containing one
emoji per PL-2026 team. Prints doc_id per team so we can UPDATE
teams.tg_custom_emoji_id directly.

Requirements: Telethon session already authorized, account has Premium.
"""
from __future__ import annotations

import asyncio
import io
import os
import re
import unicodedata
from pathlib import Path

import httpx
from dotenv import load_dotenv
from PIL import Image
from sqlalchemy import distinct, select, update
from telethon import TelegramClient
from telethon.tl.functions.messages import UploadMediaRequest
from telethon.tl.functions.stickers import CreateStickerSetRequest
from telethon.tl.types import (
    DocumentAttributeFilename,
    InputDocument,
    InputMediaUploadedDocument,
    InputStickerSetItem,
    InputUserSelf,
)

from app.database import AsyncSessionLocal
from app.models import Game, Team

LOGOS_DIR = Path(__file__).parent / "qfl_pack_logos"
PACK_TITLE = "QFL Football Clubs"
PACK_SHORT_NAME = "qfl_football_clubs_by_nurali017"  # must be globally unique


def slugify(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_") or "team"


def minio_to_public(url: str) -> str:
    return url.replace(
        "https://localhost:9000", "https://kffleague.kz/storage"
    ).replace(
        "http://localhost:9000", "https://kffleague.kz/storage"
    )


async def download_and_resize(client: httpx.AsyncClient, url: str, dest: Path) -> bool:
    try:
        r = await client.get(url)
        r.raise_for_status()
        img = Image.open(io.BytesIO(r.content)).convert("RGBA")
        # fit into 100x100, centered, preserving aspect ratio
        w, h = img.size
        scale = min(100 / w, 100 / h)
        new_w, new_h = int(round(w * scale)), int(round(h * scale))
        img = img.resize((new_w, new_h), Image.LANCZOS)
        canvas = Image.new("RGBA", (100, 100), (0, 0, 0, 0))
        canvas.paste(img, ((100 - new_w) // 2, (100 - new_h) // 2), img)
        canvas.save(dest, "PNG", optimize=True)
        return True
    except Exception as e:
        print(f"  failed {url}: {e}")
        return False


async def main(dry_run: bool = False, apply_db: bool = False) -> None:
    load_dotenv(".env", override=True)
    LOGOS_DIR.mkdir(parents=True, exist_ok=True)

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

        # Step 1: prepare 100×100 PNGs
        prepared: list[tuple[Team, Path]] = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            for t in teams:
                if not t.logo_url:
                    print(f"  skip {t.name_kz}: no logo")
                    continue
                fname = LOGOS_DIR / f"{slugify(t.name_en or t.name)}.png"
                url = minio_to_public(t.logo_url)
                ok = await download_and_resize(c, url, fname)
                if ok:
                    prepared.append((t, fname))
                    print(f"  ✔ {t.name_kz:<20} {fname.name}")

        print(f"\nPrepared {len(prepared)} logos in {LOGOS_DIR}")
        if dry_run:
            return

        # Step 2: Telethon — upload + create sticker pack
        api_id = int(os.environ["TELETHON_API_ID"])
        api_hash = os.environ["TELETHON_API_HASH"]
        session = os.environ["TELETHON_SESSION_PATH"]
        tg = TelegramClient(session, api_id, api_hash)
        await tg.connect()
        try:
            me = await tg.get_me()
            print(f"\nTelegram user: @{me.username} premium={me.premium}")
            sticker_items: list[InputStickerSetItem] = []
            for team, path in prepared:
                upl = await tg.upload_file(path)
                media = InputMediaUploadedDocument(
                    file=upl,
                    mime_type="image/png",
                    attributes=[DocumentAttributeFilename(file_name=path.name)],
                    force_file=True,
                )
                result = await tg(
                    UploadMediaRequest(peer=InputUserSelf(), media=media)
                )
                doc = result.document
                sticker_items.append(
                    InputStickerSetItem(
                        document=InputDocument(
                            id=doc.id, access_hash=doc.access_hash, file_reference=doc.file_reference
                        ),
                        emoji="⚽",
                    )
                )
                print(f"  uploaded {team.name_kz}: doc_id={doc.id}")

            print(f"\nCreating sticker pack '{PACK_SHORT_NAME}'…")
            created = await tg(
                CreateStickerSetRequest(
                    user_id=InputUserSelf(),
                    title=PACK_TITLE,
                    short_name=PACK_SHORT_NAME,
                    stickers=sticker_items,
                    emojis=True,
                )
            )
            print(f"✅ Pack created: short_name={created.set.short_name} id={created.set.id}")
            doc_map = {d.id: d for d in created.documents}
            print(f"pack has {len(created.documents)} documents")

            # Step 3: write mapping into DB (optional)
            for team, path in prepared:
                # Find matching uploaded doc_id by order
                pass

            # Simpler mapping: positions in sticker_items correspond to documents
            # created set in same order (usually), but verify by file_reference
            print("\n--- team → doc_id ---")
            mapping: list[tuple[Team, int]] = []
            for (team, _), item in zip(prepared, sticker_items):
                doc_id = item.document.id
                # look up the matching resolved doc from created.documents
                resolved = doc_map.get(doc_id) or (
                    created.documents[prepared.index((team, _))]
                    if prepared.index((team, _)) < len(created.documents)
                    else None
                )
                final_id = resolved.id if resolved else doc_id
                mapping.append((team, final_id))
                print(f"  {team.name_kz:<20} -> {final_id}")

            if apply_db:
                for team, doc_id in mapping:
                    await db.execute(
                        update(Team).where(Team.id == team.id).values(
                            tg_custom_emoji_id=str(doc_id)
                        )
                    )
                await db.commit()
                print("\n✅ teams.tg_custom_emoji_id updated")
        finally:
            await tg.disconnect()


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    apply = "--apply" in sys.argv
    asyncio.run(main(dry_run=dry, apply_db=apply))
