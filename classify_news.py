"""
Batch classify news materials into NEWS/ANALYTICS.

Default mode is dry-run. Use --apply to persist article_type updates.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import desc, select

from app.database import AsyncSessionLocal
from app.models.news import News
from app.services.news_classifier import NewsClassifierService


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def serialize_report(
    summary: dict[str, Any],
    needs_review: list[dict[str, Any]],
    updated_group_ids: list[UUID],
) -> dict[str, Any]:
    return {
        "summary": summary,
        "needs_review": needs_review,
        "updated_group_ids": [str(group_id) for group_id in updated_group_ids],
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".csv":
        rows = report["needs_review"]
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "group_id",
                    "representative_news_id",
                    "representative_title",
                    "confidence",
                    "source",
                    "reason",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
        return

    with path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.min_confidence < 0 or args.min_confidence > 1:
        raise ValueError("min-confidence must be between 0 and 1")

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(News).order_by(desc(News.publish_date), desc(News.id)))
        rows = result.scalars().all()

        grouped: dict[UUID, list[News]] = {}
        for row in rows:
            grouped.setdefault(row.translation_group_id, []).append(row)

        groups = list(grouped.items())
        if args.only_unclassified:
            groups = [
                (group_id, items)
                for group_id, items in groups
                if any(item.article_type is None for item in items)
            ]

        if args.championship_code:
            groups = [
                (group_id, items)
                for group_id, items in groups
                if any(item.championship_code == args.championship_code for item in items)
            ]

        if args.date_from or args.date_to:
            filtered_groups: list[tuple[UUID, list[News]]] = []
            for group_id, items in groups:
                has_in_range = False
                for item in items:
                    if item.publish_date is None:
                        continue
                    if args.date_from and item.publish_date < args.date_from:
                        continue
                    if args.date_to and item.publish_date > args.date_to:
                        continue
                    has_in_range = True
                    break
                if has_in_range:
                    filtered_groups.append((group_id, items))
            groups = filtered_groups

        groups.sort(
            key=lambda pair: max((item.updated_at for item in pair[1]), default=datetime.min),
            reverse=True,
        )
        if args.limit is not None:
            groups = groups[: args.limit]

        classifier = NewsClassifierService()
        needs_review: list[dict[str, Any]] = []
        updated_group_ids: list[UUID] = []
        classified_groups = 0

        for group_id, items in groups:
            decision = await classifier.classify_group(
                items,
                min_confidence=args.min_confidence,
            )
            if decision.article_type is None:
                needs_review.append(
                    {
                        "group_id": str(group_id),
                        "representative_news_id": decision.representative_news_id,
                        "representative_title": decision.representative_title,
                        "confidence": round(decision.confidence, 4),
                        "source": decision.source,
                        "reason": decision.reason,
                    }
                )
                continue

            classified_groups += 1
            will_change = any(item.article_type != decision.article_type for item in items)
            if will_change:
                updated_group_ids.append(group_id)
                if args.apply:
                    for item in items:
                        item.article_type = decision.article_type

        if args.apply and updated_group_ids:
            await session.commit()

        summary = {
            "dry_run": not args.apply,
            "total_groups": len(groups),
            "classified_groups": classified_groups,
            "updated_groups": len(updated_group_ids),
            "unchanged_groups": max(len(groups) - len(updated_group_ids) - len(needs_review), 0),
            "needs_review_count": len(needs_review),
        }
        report = serialize_report(summary, needs_review, updated_group_ids)
        if args.report_path:
            write_report(Path(args.report_path), report)
        return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch classify news materials as NEWS/ANALYTICS."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates to DB. Without this flag script runs in dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit amount of material groups to process.",
    )
    parser.add_argument(
        "--only-unclassified",
        action="store_true",
        help="Process only groups that have at least one translation with NULL article_type.",
    )
    parser.add_argument(
        "--championship-code",
        type=str,
        default=None,
        help="Filter by championship_code (pl, 1l, cup, 2l, el).",
    )
    parser.add_argument(
        "--date-from",
        type=parse_date,
        default=None,
        help="Filter groups by publish_date from this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--date-to",
        type=parse_date,
        default=None,
        help="Filter groups by publish_date to this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.70,
        help="Confidence threshold (0..1). Lower confidence will stay unclassified.",
    )
    parser.add_argument(
        "--report-path",
        type=str,
        default=None,
        help="Optional report output path (.json or .csv).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = asyncio.run(run(args))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
