"""Append one or more team logos to the existing QFL emoji pack.

Usage:
    PYTHONPATH=. python3 scripts/add_team_logo_to_pack.py <team_id> [<team_id> ...]
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
from app.models import Team
from scripts.create_qfl_emoji_pack import LOGOS_DIR, download_and_resize, minio_to_public, slugify

PACK_SHORT_NAME = "qfl_football_clubs_by_nurali017"


async def add_one(team_id: int) -> None:
    load_dotenv(".env", override=True)
    LOGOS_DIR.mkdir(parents=True, exist_ok=True)

    async with AsyncSessionLocal() as db:
        team = (await db.execute(select(Team).where(Team.id == team_id))).scalar_one()
        if not team.logo_url:
            print(f"team {team_id} {team.name_kz!r}: no logo")
            return

        fname = LOGOS_DIR / f"{slugify(team.name_en or team.name)}_{team_id}.png"
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            ok = await download_and_resize(c, minio_to_public(team.logo_url), fname)
        if not ok:
            return
        print(f"  ✔ prepared {team.name_kz!r} ({team_id}) -> {fname.name}")

        api_id = int(os.environ["TELETHON_API_ID"])
        api_hash = os.environ["TELETHON_API_HASH"]
        sess = os.environ["TELETHON_SESSION_PATH"]
        tg = TelegramClient(sess, api_id, api_hash)
        await tg.connect()
        try:
            upl = await tg.upload_file(fname)
            media = InputMediaUploadedDocument(
                file=upl, mime_type="image/png",
                attributes=[DocumentAttributeFilename(file_name=fname.name)],
                force_file=True,
            )
            uploaded = await tg(UploadMediaRequest(peer=InputUserSelf(), media=media))
            doc = uploaded.document
            res = await tg(
                AddStickerToSetRequest(
                    stickerset=InputStickerSetShortName(short_name=PACK_SHORT_NAME),
                    sticker=InputStickerSetItem(
                        document=InputDocument(
                            id=doc.id, access_hash=doc.access_hash, file_reference=doc.file_reference,
                        ),
                        emoji="⚽",
                    ),
                )
            )
            new_doc_id = res.documents[-1].id
            print(f"  added doc_id={new_doc_id}")
            await db.execute(
                update(Team).where(Team.id == team_id).values(tg_custom_emoji_id=str(new_doc_id))
            )
            await db.commit()
            print(f"  ✅ teams.tg_custom_emoji_id set for {team_id}")
        finally:
            await tg.disconnect()


async def main() -> None:
    ids = [int(a) for a in sys.argv[1:]]
    for tid in ids:
        await add_one(tid)


if __name__ == "__main__":
    asyncio.run(main())
