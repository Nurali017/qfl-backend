"""Append season/league logo to existing QFL emoji pack.

Uses scripts.create_qfl_emoji_pack helpers.
"""
from __future__ import annotations

import asyncio
import os
import sys

import httpx
from dotenv import load_dotenv
from sqlalchemy import update, select
from telethon import TelegramClient
from telethon.tl.functions.messages import UploadMediaRequest
from telethon.tl.functions.stickers import AddStickerToSetRequest
from telethon.tl.types import (
    DocumentAttributeFilename,
    InputDocument,
    InputMediaUploadedDocument,
    InputStickerSetItem,
    InputStickerSetShortName,
    InputUserSelf,
)

from app.database import AsyncSessionLocal
from app.models import Season
from scripts.create_qfl_emoji_pack import (
    LOGOS_DIR,
    download_and_resize,
    minio_to_public,
)

PACK_SHORT_NAME = "qfl_football_clubs_by_nurali017"


async def main() -> None:
    load_dotenv(".env", override=True)
    season_id = int(sys.argv[1]) if len(sys.argv) > 1 else 200

    async with AsyncSessionLocal() as db:
        season = (
            await db.execute(select(Season).where(Season.id == season_id))
        ).scalar_one()
        if not season.logo:
            print(f"Season {season_id} has no logo")
            return

        LOGOS_DIR.mkdir(parents=True, exist_ok=True)
        local_path = LOGOS_DIR / f"league_{season_id}.png"
        logo_url = season.logo
        if logo_url.startswith("/"):
            logo_url = f"https://kffleague.kz{logo_url}"
        elif "localhost" in logo_url:
            logo_url = minio_to_public(logo_url)
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            ok = await download_and_resize(c, logo_url, local_path)
        if not ok:
            return
        print(f"  ✔ {season.name_kz or season.name}: {local_path.name}")

        api_id = int(os.environ["TELETHON_API_ID"])
        api_hash = os.environ["TELETHON_API_HASH"]
        sess = os.environ["TELETHON_SESSION_PATH"]
        tg = TelegramClient(sess, api_id, api_hash)
        await tg.connect()
        try:
            upl = await tg.upload_file(local_path)
            media = InputMediaUploadedDocument(
                file=upl,
                mime_type="image/png",
                attributes=[DocumentAttributeFilename(file_name=local_path.name)],
                force_file=True,
            )
            uploaded = await tg(UploadMediaRequest(peer=InputUserSelf(), media=media))
            doc = uploaded.document

            result = await tg(
                AddStickerToSetRequest(
                    stickerset=InputStickerSetShortName(short_name=PACK_SHORT_NAME),
                    sticker=InputStickerSetItem(
                        document=InputDocument(
                            id=doc.id,
                            access_hash=doc.access_hash,
                            file_reference=doc.file_reference,
                        ),
                        emoji="🏆",
                    ),
                )
            )
            # The newly added sticker is the LAST one in result.documents
            new_doc_id = result.documents[-1].id
            print(f"Added. doc_id={new_doc_id}")

            # Persist
            await db.execute(
                update(Season).where(Season.id == season_id).values(
                    tg_custom_emoji_id=str(new_doc_id)
                )
            )
            await db.commit()
            print(f"✅ seasons.tg_custom_emoji_id set for season {season_id}")
        finally:
            await tg.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
