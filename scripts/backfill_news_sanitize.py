"""Backfill: run sanitize_news_html() over all existing News.content rows.

Default: dry-run. Prints unified diff for the first N=20 changed rows plus a
summary count. Use --apply to write back.

Idempotent: re-running after --apply is a no-op (sanitizer is deterministic).

Usage:
    docker exec qfl-backend python scripts/backfill_news_sanitize.py
    docker exec qfl-backend python scripts/backfill_news_sanitize.py --apply
"""
import argparse
import asyncio
import difflib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from app.database import AsyncSessionLocal  # noqa: E402
from app.models import News  # noqa: E402
from app.utils.html_cleaner import sanitize_news_html  # noqa: E402

DIFF_SAMPLE_SIZE = 20


def _short_diff(before: str, after: str, label: str) -> str:
    lines = list(
        difflib.unified_diff(
            before.splitlines(keepends=False),
            after.splitlines(keepends=False),
            fromfile=f"{label} (before)",
            tofile=f"{label} (after)",
            lineterm="",
            n=2,
        )
    )
    return "\n".join(lines)


async def run(apply: bool) -> int:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(News.id, News.language, News.content).where(News.content.is_not(None))
        )
        rows = result.all()

    total = len(rows)
    changed = 0
    diffs_shown = 0
    changed_ids: list[int] = []

    for news_id, language, content in rows:
        sanitized = sanitize_news_html(content)
        if sanitized == content:
            continue
        changed += 1
        changed_ids.append(news_id)
        if diffs_shown < DIFF_SAMPLE_SIZE:
            label = f"News id={news_id} lang={language.value if hasattr(language, 'value') else language}"
            diff = _short_diff(content or "", sanitized or "", label)
            if diff:
                print(diff)
                print("---")
                diffs_shown += 1

    print()
    print(f"Summary: changed={changed} total={total} (sample diffs shown: {diffs_shown})")

    if not apply:
        print("Dry-run. Pass --apply to write back.")
        return changed

    if changed == 0:
        print("Nothing to apply.")
        return 0

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(News).where(News.id.in_(changed_ids)))
        items = result.scalars().all()
        for item in items:
            item.content = sanitize_news_html(item.content)
        await db.commit()

    print(f"Applied: updated {len(changed_ids)} rows.")
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write sanitized content back to DB")
    args = parser.parse_args()
    asyncio.run(run(args.apply))


if __name__ == "__main__":
    main()
