#!/usr/bin/env python3
"""
Season remediation utility for SOTA sync issues.

Workflow:
1) Collect baseline quality metrics
2) Deduplicate events for a target season (dry-run/apply)
3) Rebuild game lineups from pre_game_lineup endpoint (dry-run/apply)
4) Collect post-remediation metrics
5) Generate incident report artifacts (Markdown + CSV)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import delete, select, text

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import AsyncSessionLocal
from app.models import Game, GameLineup
from app.services.sota_client import SotaClient
from app.services.sync import SyncOrchestrator


MARKER_NUMBERS = {
    "TEAM",
    "FORMATION",
    "COACH",
    "MAIN",
    "ОСНОВНЫЕ",
    "ЗАПАСНЫЕ",
    "STADIUM",
    "VENUE",
    "TIME",
    "DATE",
}


@dataclass
class GameRef:
    game_id: str
    tour: int | None
    home_team: str
    away_team: str
    home_score: int | None
    away_score: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remediate SOTA data issues for a season")
    parser.add_argument("--season-id", type=int, default=61, help="Season ID to process")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default behavior)")
    parser.add_argument("--apply", action="store_true", help="Apply remediation changes")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory for incident artifacts (md/csv). Defaults to backend/docs/incidents",
    )
    parser.add_argument("--output", type=str, default=None, help="Optional JSON summary output file")
    return parser.parse_args()


async def get_metrics(db, season_id: int) -> dict[str, Any]:
    metrics: dict[str, Any] = {"season_id": season_id}

    counts_query = text(
        """
        SELECT
          count(*) AS games_total,
          count(*) FILTER (WHERE ge.cnt IS NULL OR ge.cnt = 0) AS no_events,
          count(*) FILTER (WHERE gt.cnt IS NULL OR gt.cnt = 0) AS no_team_stats,
          count(*) FILTER (WHERE gp.cnt IS NULL OR gp.cnt = 0) AS no_player_stats,
          count(*) FILTER (WHERE gr.cnt IS NULL OR gr.cnt = 0) AS no_referees
        FROM games g
        LEFT JOIN (SELECT game_id, count(*) cnt FROM game_events GROUP BY game_id) ge ON ge.game_id = g.id
        LEFT JOIN (SELECT game_id, count(*) cnt FROM game_team_stats GROUP BY game_id) gt ON gt.game_id = g.id
        LEFT JOIN (SELECT game_id, count(*) cnt FROM game_player_stats GROUP BY game_id) gp ON gp.game_id = g.id
        LEFT JOIN (SELECT game_id, count(*) cnt FROM game_referees GROUP BY game_id) gr ON gr.game_id = g.id
        WHERE g.season_id = :season_id
        """
    )
    row = (await db.execute(counts_query, {"season_id": season_id})).mappings().one()
    metrics.update(dict(row))

    starters_query = text(
        """
        WITH starters AS (
          SELECT
            g.id,
            count(*) FILTER (WHERE gl.team_id = g.home_team_id AND gl.lineup_type = 'starter') AS home_starters,
            count(*) FILTER (WHERE gl.team_id = g.away_team_id AND gl.lineup_type = 'starter') AS away_starters
          FROM games g
          LEFT JOIN game_lineups gl ON gl.game_id = g.id
          WHERE g.season_id = :season_id
          GROUP BY g.id
        )
        SELECT
          count(*) FILTER (WHERE home_starters = 11 AND away_starters = 11) AS exact_11_11,
          count(*) FILTER (WHERE home_starters < 11 OR away_starters < 11) AS lt_11_any,
          count(*) FILTER (WHERE home_starters > 11 OR away_starters > 11) AS gt_11_any,
          count(*) FILTER (WHERE home_starters <> 11 OR away_starters <> 11) AS not_11_11,
          count(*) FILTER (WHERE home_starters = 0 OR away_starters = 0) AS zero_starters_any
        FROM starters
        """
    )
    row = (await db.execute(starters_query, {"season_id": season_id})).mappings().one()
    metrics.update(dict(row))

    duplicate_query = text(
        """
        WITH ranked AS (
          SELECT
            ge.id,
            row_number() OVER (
              PARTITION BY ge.game_id, ge.half, ge.minute, ge.event_type,
              COALESCE(ge.player_id::text, lower(trim(ge.player_name)))
              ORDER BY ge.id
            ) AS rn
          FROM game_events ge
          JOIN games g ON g.id = ge.game_id
          WHERE g.season_id = :season_id
        )
        SELECT count(*) AS duplicate_events FROM ranked WHERE rn > 1
        """
    )
    metrics["duplicate_events"] = (await db.execute(duplicate_query, {"season_id": season_id})).scalar_one()

    mismatch_query = text(
        """
        WITH ge_goals AS (
          SELECT game_id, count(*) AS goal_events
          FROM game_events
          WHERE event_type = 'goal'
          GROUP BY game_id
        )
        SELECT
          count(*) FILTER (
            WHERE COALESCE(gg.goal_events, 0) <> COALESCE(g.home_score, 0) + COALESCE(g.away_score, 0)
          ) AS goal_mismatch_total,
          count(*) FILTER (
            WHERE COALESCE(gg.goal_events, 0) > COALESCE(g.home_score, 0) + COALESCE(g.away_score, 0)
          ) AS goal_mismatch_positive,
          count(*) FILTER (
            WHERE COALESCE(gg.goal_events, 0) < COALESCE(g.home_score, 0) + COALESCE(g.away_score, 0)
          ) AS goal_mismatch_negative
        FROM games g
        LEFT JOIN ge_goals gg ON gg.game_id = g.id
        WHERE g.season_id = :season_id
        """
    )
    row = (await db.execute(mismatch_query, {"season_id": season_id})).mappings().one()
    metrics.update(dict(row))

    return metrics


async def fetch_no_events_games(db, season_id: int) -> list[GameRef]:
    query = text(
        """
        SELECT
          g.id::text AS game_id,
          g.tour,
          ht.name AS home_team,
          at.name AS away_team,
          g.home_score,
          g.away_score
        FROM games g
        JOIN teams ht ON ht.id = g.home_team_id
        JOIN teams at ON at.id = g.away_team_id
        LEFT JOIN (SELECT game_id, count(*) cnt FROM game_events GROUP BY game_id) ge ON ge.game_id = g.id
        WHERE g.season_id = :season_id AND COALESCE(ge.cnt, 0) = 0
        ORDER BY g.tour, g.date, g.time
        """
    )
    rows = (await db.execute(query, {"season_id": season_id})).mappings().all()
    return [GameRef(**row) for row in rows]


async def fetch_goal_mismatch_examples(db, season_id: int, limit: int = 5) -> list[dict[str, Any]]:
    query = text(
        """
        WITH ge_goals AS (
          SELECT game_id, count(*) AS goal_events
          FROM game_events
          WHERE event_type = 'goal'
          GROUP BY game_id
        )
        SELECT
          g.id::text AS game_id,
          g.tour,
          ht.name AS home_team,
          at.name AS away_team,
          g.home_score,
          g.away_score,
          COALESCE(gg.goal_events, 0) AS goal_events,
          (COALESCE(gg.goal_events, 0) - COALESCE(g.home_score, 0) - COALESCE(g.away_score, 0)) AS diff
        FROM games g
        JOIN teams ht ON ht.id = g.home_team_id
        JOIN teams at ON at.id = g.away_team_id
        LEFT JOIN ge_goals gg ON gg.game_id = g.id
        WHERE g.season_id = :season_id
          AND COALESCE(gg.goal_events, 0) > COALESCE(g.home_score, 0) + COALESCE(g.away_score, 0)
        ORDER BY diff DESC, g.tour
        LIMIT :limit
        """
    )
    rows = (await db.execute(query, {"season_id": season_id, "limit": limit})).mappings().all()
    return [dict(row) for row in rows]


async def dedupe_events(db, season_id: int, dry_run: bool) -> dict[str, int]:
    if dry_run:
        query = text(
            """
            WITH ranked AS (
              SELECT
                ge.id,
                row_number() OVER (
                  PARTITION BY ge.game_id, ge.half, ge.minute, ge.event_type,
                  COALESCE(ge.player_id::text, lower(trim(ge.player_name)))
                  ORDER BY ge.id
                ) AS rn
              FROM game_events ge
              JOIN games g ON g.id = ge.game_id
              WHERE g.season_id = :season_id
            )
            SELECT count(*) AS duplicates_found FROM ranked WHERE rn > 1
            """
        )
        duplicates_found = (await db.execute(query, {"season_id": season_id})).scalar_one()
        return {"duplicates_found": duplicates_found, "duplicates_deleted": 0}

    delete_query = text(
        """
        WITH ranked AS (
          SELECT
            ge.id,
            row_number() OVER (
              PARTITION BY ge.game_id, ge.half, ge.minute, ge.event_type,
              COALESCE(ge.player_id::text, lower(trim(ge.player_name)))
              ORDER BY ge.id
            ) AS rn
          FROM game_events ge
          JOIN games g ON g.id = ge.game_id
          WHERE g.season_id = :season_id
        )
        DELETE FROM game_events
        WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
        RETURNING id
        """
    )
    deleted_rows = (await db.execute(delete_query, {"season_id": season_id})).all()
    await db.commit()
    deleted = len(deleted_rows)
    return {"duplicates_found": deleted, "duplicates_deleted": deleted}


async def rebuild_lineups_from_pre_game(db, season_id: int, dry_run: bool) -> dict[str, Any]:
    games = list(
        (
            await db.execute(
                select(Game.id).where(Game.season_id == season_id).order_by(Game.date, Game.time, Game.id)
            )
        ).scalars().all()
    )
    result = {
        "games_total": len(games),
        "games_rebuilt": 0,
        "games_failed": 0,
        "errors": [],
    }

    if dry_run:
        return result

    orchestrator = SyncOrchestrator(db)
    for game_id in games:
        game_id_str = str(game_id)
        try:
            await db.execute(delete(GameLineup).where(GameLineup.game_id == game_id))
            await db.commit()
            await orchestrator.sync_pre_game_lineup(game_id_str)
            result["games_rebuilt"] += 1
        except Exception as exc:
            await db.rollback()
            result["games_failed"] += 1
            result["errors"].append({"game_id": game_id_str, "error": str(exc)})
    return result


def _extract_player_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []

    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        number = item.get("number")
        if isinstance(number, str) and number in MARKER_NUMBERS:
            continue
        if isinstance(number, int):
            rows.append(item)
    return rows


async def collect_sota_report_rows(
    db,
    season_id: int,
    no_events_games: list[GameRef],
    mismatch_examples: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    # Local starter counts for local_state annotation.
    starters_query = text(
        """
        SELECT
          g.id::text AS game_id,
          count(*) FILTER (WHERE gl.team_id = g.home_team_id AND gl.lineup_type = 'starter') AS home_starters,
          count(*) FILTER (WHERE gl.team_id = g.away_team_id AND gl.lineup_type = 'starter') AS away_starters
        FROM games g
        LEFT JOIN game_lineups gl ON gl.game_id = g.id
        WHERE g.season_id = :season_id
        GROUP BY g.id
        """
    )
    starter_rows = (await db.execute(starters_query, {"season_id": season_id})).mappings().all()
    starters_map = {
        row["game_id"]: (int(row["home_starters"] or 0), int(row["away_starters"] or 0))
        for row in starter_rows
    }

    game_refs_query = text(
        """
        SELECT
          g.id::text AS game_id,
          g.tour,
          ht.name AS home_team,
          at.name AS away_team,
          g.home_score,
          g.away_score
        FROM games g
        JOIN teams ht ON ht.id = g.home_team_id
        JOIN teams at ON at.id = g.away_team_id
        WHERE g.season_id = :season_id
        ORDER BY g.tour, g.date, g.time
        """
    )
    all_games = [GameRef(**row) for row in (await db.execute(game_refs_query, {"season_id": season_id})).mappings().all()]

    client = SotaClient()
    await client.ensure_authenticated()
    token = client.access_token
    if not token:
        raise RuntimeError("Failed to obtain SOTA access token")

    rows: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as http:
        async def safe_get(url: str) -> httpx.Response | Exception:
            last_error: Exception | None = None
            for attempt in range(3):
                try:
                    return await http.get(url, params={"access_token": token})
                except (
                    httpx.RemoteProtocolError,
                    httpx.ReadTimeout,
                    httpx.ConnectTimeout,
                    httpx.ConnectError,
                ) as exc:
                    last_error = exc
                    if attempt < 2:
                        await asyncio.sleep(1 + attempt)
            assert last_error is not None
            return last_error

        # 1) no-events source gap rows
        for g in no_events_games:
            endpoint = f"https://sota.id/em/{g.game_id}-list.json"
            response = await safe_get(endpoint)
            if isinstance(response, Exception):
                rows.append(
                    {
                        "game_id": g.game_id,
                        "tour": g.tour,
                        "issue_type": "em_events_missing",
                        "endpoint": endpoint,
                        "status": "error",
                        "payload_size_or_count": "",
                        "local_state": f"local_events=0 score={g.home_score}:{g.away_score}",
                        "notes": f"SOTA request failed: {response.__class__.__name__}",
                    }
                )
                continue
            payload_count = ""
            note = "SOTA list endpoint unavailable"
            if response.status_code == 200:
                payload = response.json()
                payload_count = len(payload) if isinstance(payload, list) else -1
                if payload_count == 0:
                    note = "SOTA list endpoint returns empty array"
                else:
                    note = "SOTA list endpoint returns non-empty payload (needs manual check)"
            rows.append(
                {
                    "game_id": g.game_id,
                    "tour": g.tour,
                    "issue_type": "em_events_missing",
                    "endpoint": endpoint,
                    "status": response.status_code,
                    "payload_size_or_count": payload_count,
                    "local_state": f"local_events=0 score={g.home_score}:{g.away_score}",
                    "notes": note,
                }
            )

        # 2) em/team missing id / amplua
        for g in all_games:
            hs, as_ = starters_map.get(g.game_id, (0, 0))
            endpoint_home = f"https://sota.id/em/{g.game_id}-team-home.json"
            endpoint_away = f"https://sota.id/em/{g.game_id}-team-away.json"
            home = await safe_get(endpoint_home)
            away = await safe_get(endpoint_away)
            if isinstance(home, Exception) or isinstance(away, Exception):
                continue
            if home.status_code != 200 or away.status_code != 200:
                continue

            home_rows = _extract_player_rows(home.json())
            away_rows = _extract_player_rows(away.json())

            home_with_id = sum(1 for p in home_rows if p.get("id"))
            away_with_id = sum(1 for p in away_rows if p.get("id"))
            home_with_amplua = sum(1 for p in home_rows if p.get("amplua"))
            away_with_amplua = sum(1 for p in away_rows if p.get("amplua"))

            if home_with_id == 0 and away_with_id == 0:
                rows.append(
                    {
                        "game_id": g.game_id,
                        "tour": g.tour,
                        "issue_type": "em_team_missing_ids",
                        "endpoint": f"{endpoint_home} | {endpoint_away}",
                        "status": "200/200",
                        "payload_size_or_count": (
                            f"home_ids={home_with_id}/{len(home_rows)};"
                            f"away_ids={away_with_id}/{len(away_rows)}"
                        ),
                        "local_state": f"starters={hs}-{as_}",
                        "notes": "Both team payloads have no player id values",
                    }
                )

            if home_with_amplua == 0 and away_with_amplua == 0:
                rows.append(
                    {
                        "game_id": g.game_id,
                        "tour": g.tour,
                        "issue_type": "em_team_missing_amplua",
                        "endpoint": f"{endpoint_home} | {endpoint_away}",
                        "status": "200/200",
                        "payload_size_or_count": (
                            f"home_amplua={home_with_amplua}/{len(home_rows)};"
                            f"away_amplua={away_with_amplua}/{len(away_rows)}"
                        ),
                        "local_state": f"starters={hs}-{as_}",
                        "notes": "No amplua provided for either team payload",
                    }
                )

        # 3) historical duplicate desync samples
        for sample in mismatch_examples:
            game_id = sample["game_id"]
            endpoint = f"https://sota.id/em/{game_id}-list.json"
            response = await safe_get(endpoint)
            if isinstance(response, Exception):
                rows.append(
                    {
                        "game_id": game_id,
                        "tour": sample["tour"],
                        "issue_type": "historical_duplicate_desync",
                        "endpoint": endpoint,
                        "status": "error",
                        "payload_size_or_count": (
                            f"local_goal_events={sample['goal_events']};"
                            f"sota_goal_events="
                        ),
                        "local_state": (
                            f"score={sample['home_score']}:{sample['away_score']};"
                            f"diff={sample['diff']}"
                        ),
                        "notes": f"SOTA request failed: {response.__class__.__name__}",
                    }
                )
                continue
            sota_goal_count = ""
            if response.status_code == 200:
                payload = response.json()
                if isinstance(payload, list):
                    sota_goal_count = sum(1 for item in payload if item.get("action") == "ГОЛ")
            rows.append(
                {
                    "game_id": game_id,
                    "tour": sample["tour"],
                    "issue_type": "historical_duplicate_desync",
                    "endpoint": endpoint,
                    "status": response.status_code,
                    "payload_size_or_count": (
                        f"local_goal_events={sample['goal_events']};"
                        f"sota_goal_events={sota_goal_count}"
                    ),
                    "local_state": (
                        f"score={sample['home_score']}:{sample['away_score']};"
                        f"diff={sample['diff']}"
                    ),
                    "notes": "Historical local events exceed current SOTA event feed",
                }
            )

    return rows


def write_incident_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "game_id",
        "tour",
        "issue_type",
        "endpoint",
        "status",
        "payload_size_or_count",
        "local_state",
        "notes",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_markdown_report(
    season_id: int,
    started_at: str,
    mode: str,
    baseline: dict[str, Any],
    post: dict[str, Any],
    dedupe_result: dict[str, int],
    lineup_result: dict[str, Any],
    report_rows: list[dict[str, Any]],
) -> str:
    def count(issue_type: str) -> int:
        return sum(1 for r in report_rows if r["issue_type"] == issue_type)

    no_events_example = next((r["game_id"] for r in report_rows if r["issue_type"] == "em_events_missing"), "")
    team_example = next((r["game_id"] for r in report_rows if r["issue_type"] == "em_team_missing_amplua"), "")
    desync_example = next((r["game_id"] for r in report_rows if r["issue_type"] == "historical_duplicate_desync"), "")

    lines = [
        f"# SOTA Incident Report: Season {season_id}",
        "",
        f"- Generated at (UTC): {started_at}",
        f"- Mode: {mode}",
        "",
        "## Symptoms",
        f"- Local no-events matches: {baseline['no_events']}",
        f"- Local duplicate events (signature-based): {baseline['duplicate_events']}",
        f"- Starter anomalies before remediation: `{baseline['not_11_11']}` / `{baseline['games_total']}`",
        f"- Zero-starter matches before remediation: {baseline['zero_starters_any']}",
        "",
        "## Evidence",
        f"- `em/list` missing rows in CSV: {count('em_events_missing')}",
        f"- `em/team` missing ids rows in CSV: {count('em_team_missing_ids')}",
        f"- `em/team` missing amplua rows in CSV: {count('em_team_missing_amplua')}",
        f"- Historical duplicate desync rows in CSV: {count('historical_duplicate_desync')}",
        "",
        "## Remediation Performed",
        f"- Event deduplication: found `{dedupe_result['duplicates_found']}`, deleted `{dedupe_result['duplicates_deleted']}`",
        (
            f"- Lineup rebuild from pre_game_lineup: rebuilt `{lineup_result['games_rebuilt']}`"
            f", failed `{lineup_result['games_failed']}` (planned `{lineup_result['games_total']}`)"
        ),
        "",
        "## Post-Remediation State",
        f"- Local no-events matches: {post['no_events']} (expected source gap only)",
        f"- Local duplicate events: {post['duplicate_events']}",
        f"- Starter anomalies after remediation: `{post['not_11_11']}` / `{post['games_total']}`",
        f"- Zero-starter matches after remediation: {post['zero_starters_any']}",
        "",
        "## Reproduction Steps",
        "Use a valid SOTA token (`<TOKEN>`) in requests below.",
        "```bash",
    ]
    if no_events_example:
        lines.append(f"curl \"https://sota.id/em/{no_events_example}-list.json?access_token=<TOKEN>\"")
    if team_example:
        lines.append(f"curl \"https://sota.id/em/{team_example}-team-home.json?access_token=<TOKEN>\"")
        lines.append(f"curl \"https://sota.id/em/{team_example}-team-away.json?access_token=<TOKEN>\"")
    if desync_example:
        lines.append(f"curl \"https://sota.id/em/{desync_example}-list.json?access_token=<TOKEN>\"")
    lines.extend(
        [
            "```",
            "",
            "## Impact",
            "- Incomplete/empty source events cause irreversible local gaps without synthetic backfill.",
            "- Missing `id` / `amplua` in `/em/team` payloads can degrade lineup classification.",
            "- Historical mixed sync runs may leave duplicate local events until deduplicated.",
            "",
            "## Request to SOTA",
            "1. Restore or provide canonical event payloads for games where `/em/*-list.json` is `404` or empty.",
            "2. Ensure `/em/*-team-{home,away}.json` contains stable player `id` and `amplua` fields.",
            "3. Confirm event-feed retention policy and whether historical edits can remove past event rows.",
            "",
        ]
    )
    return "\n".join(lines)


async def main() -> None:
    args = parse_args()
    apply = bool(args.apply)
    season_id = args.season_id
    mode = "apply" if apply else "dry-run"
    started_at = datetime.now(timezone.utc).isoformat()
    report_date = datetime.now().date().isoformat()

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent.parent / "docs" / "incidents"
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"sota_s{season_id}_incident_{report_date}.csv"
    md_path = output_dir / f"sota_s{season_id}_incident_{report_date}.md"

    async with AsyncSessionLocal() as db:
        baseline = await get_metrics(db, season_id)
        no_events_games = await fetch_no_events_games(db, season_id)
        mismatch_examples = await fetch_goal_mismatch_examples(db, season_id, limit=5)

        dedupe_result = await dedupe_events(db, season_id, dry_run=not apply)
        lineup_result = await rebuild_lineups_from_pre_game(db, season_id, dry_run=not apply)

        post = await get_metrics(db, season_id)
        report_rows = await collect_sota_report_rows(db, season_id, no_events_games, mismatch_examples)

    write_incident_csv(csv_path, report_rows)
    markdown = build_markdown_report(
        season_id=season_id,
        started_at=started_at,
        mode=mode,
        baseline=baseline,
        post=post,
        dedupe_result=dedupe_result,
        lineup_result=lineup_result,
        report_rows=report_rows,
    )
    md_path.write_text(markdown, encoding="utf-8")

    summary = {
        "generated_at": started_at,
        "season_id": season_id,
        "mode": mode,
        "baseline": baseline,
        "post": post,
        "dedupe_result": dedupe_result,
        "lineup_result": lineup_result,
        "report_csv": str(csv_path),
        "report_md": str(md_path),
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.output:
        output_file = Path(args.output)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Summary written to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
