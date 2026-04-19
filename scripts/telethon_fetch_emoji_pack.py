"""One-off helper: dump KazakhstanFootballClubs emoji pack.

Downloads each custom emoji thumbnail (WEBP) into
scripts/kff_emoji_previews/ with the filename equal to document_id.
You can then open the folder in Finder, visually match each logo to a
club, and feed the mapping to update_team_tg_emoji.py.

Usage:
    cd backend && python3 scripts/telethon_fetch_emoji_pack.py

Needs env TELETHON_API_ID, TELETHON_API_HASH, TELETHON_SESSION_PATH already
authorized.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.messages import GetStickerSetRequest
from telethon.tl.types import InputStickerSetShortName

PACK_SHORT_NAME = "KazakhstanFootballClubs"
OUT_DIR = Path(__file__).parent / "kff_emoji_previews"


async def main() -> None:
    load_dotenv(".env", override=True)
    api_id = int(os.environ["TELETHON_API_ID"])
    api_hash = os.environ["TELETHON_API_HASH"]
    session = os.environ.get("TELETHON_SESSION_PATH", ".telethon_qfl_session")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    client = TelegramClient(session, api_id, api_hash)
    await client.connect()
    try:
        result = await client(
            GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=PACK_SHORT_NAME),
                hash=0,
            )
        )
        stickers = result.documents
        print(f"Pack '{PACK_SHORT_NAME}' contains {len(stickers)} stickers\n")
        print(f"{'doc_id':<24} {'alt':<6}  saved_as")
        print("-" * 80)
        for doc in stickers:
            # Try to extract alt/fallback char from attributes
            alt = ""
            for attr in doc.attributes:
                if hasattr(attr, "alt"):
                    alt = attr.alt
                    break
            fname = OUT_DIR / f"{doc.id}.webp"
            if not fname.exists():
                await client.download_media(doc, file=str(fname))
            print(f"{doc.id:<24} {alt:<6}  {fname.name}")

        print(f"\n✅ Done. Open {OUT_DIR} in Finder to identify logos.")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
