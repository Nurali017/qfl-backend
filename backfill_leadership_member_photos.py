"""Backfill leadership member photos from absolute bucket URLs to object names.

Default mode is dry-run. Use --apply to persist updates.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from copy import deepcopy
from typing import Any

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import Page
from app.utils.file_urls import to_object_name

TARGET_SLUGS = ("rukovodstvo", "basshylyk")


async def run(*, apply: bool) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "dry_run": not apply,
        "pages_scanned": 0,
        "pages_changed": 0,
        "members_scanned": 0,
        "members_changed": 0,
        "changed_page_ids": [],
    }

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Page).where(Page.slug.in_(TARGET_SLUGS)).order_by(Page.id.asc())
        )
        pages = result.scalars().all()

        for page in pages:
            summary["pages_scanned"] += 1
            if not isinstance(page.structured_data, dict):
                continue

            members = page.structured_data.get("members")
            if not isinstance(members, list):
                continue

            updated_structured_data = deepcopy(page.structured_data)
            updated_members = updated_structured_data.get("members")
            if not isinstance(updated_members, list):
                continue

            page_changed = False
            for member in updated_members:
                if not isinstance(member, dict):
                    continue

                summary["members_scanned"] += 1
                photo = member.get("photo")
                if not isinstance(photo, str) or not photo:
                    continue

                normalized_photo = to_object_name(photo)
                if not normalized_photo or normalized_photo == photo:
                    continue

                member["photo"] = normalized_photo
                summary["members_changed"] += 1
                page_changed = True

            if not page_changed:
                continue

            summary["pages_changed"] += 1
            summary["changed_page_ids"].append(page.id)

            if apply:
                page.structured_data = updated_structured_data

        if apply and summary["pages_changed"] > 0:
            await session.commit()

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize leadership member photos in pages.structured_data.members "
            "from absolute bucket URLs to object names."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates to DB. Without this flag, script runs in dry-run mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = asyncio.run(run(apply=args.apply))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
