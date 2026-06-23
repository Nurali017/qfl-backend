"""Withdraw a team from a league season and ANNUL its results (reg. 2.16/2.9).

Used when a club withdraws from a round-robin championship having played
FEWER than half of its matches: per Second League regulation 2.16 all of its
results are annulled, and per 2.9 the team disappears from the table (its slot
stays vacant). The award-defeats branch (half or more played) is intentionally
NOT implemented here — the script refuses to run in that case.

What it does (idempotent, one transaction):
  1. Refuses to run while ANY live game is in progress.
  2. Snapshots the team's games + score_table/participant/team_season_stats
     rows to a JSON audit file BEFORE any change.
  3. Recomputes the regulation verdict; aborts unless it is "annul".
  4. Marks every one of the team's games in the season as cancelled +
     sync_disabled and NULLs all four score fields (so every score-only
     aggregator drops them) — scores are preserved in the audit file.
  5. Deletes the team's score_table, season_participants and
     team_season_stats rows for the season (so the team leaves the table and
     is not re-created by the stats sync).

Usage:
    docker exec qfl-backend python -m scripts.withdraw_team_from_season \
        --season-id 203 --team-id 10            # dry-run (no writes)
    docker exec qfl-backend python -m scripts.withdraw_team_from_season \
        --season-id 203 --team-id 10 --apply    # actually write
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum
from uuid import UUID

from sqlalchemy import delete, func, inspect, or_, select

from app.database import AsyncSessionLocal
from app.models import (
    Game,
    GameStatus,
    ScoreTable,
    SeasonParticipant,
    Team,
    TeamSeasonStats,
)
from app.services.regulations import compute_withdrawal_outcome

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("withdraw_team")

DEFAULT_REASON = "Команда снялась с турнира; результаты аннулированы (регламент, п. 2.16/2.9)"

# Statuses that count as a played match for the regulation threshold.
_PLAYED_STATUSES = (GameStatus.finished, GameStatus.technical_defeat)


def _json_safe(value):
    """Coerce a column value into a JSON-serialisable form."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    return str(value)


def _row_to_dict(obj) -> dict | None:
    """Serialise every mapped column of an ORM row to a restore-grade dict."""
    if obj is None:
        return None
    mapper = inspect(obj).mapper
    return {col.key: _json_safe(getattr(obj, col.key)) for col in mapper.column_attrs}


def _game_row(g: Game) -> dict:
    return {
        "id": g.id,
        "tour": g.tour,
        "date": g.date.isoformat() if g.date else None,
        "status": g.status.value if g.status else None,
        "home_team_id": g.home_team_id,
        "away_team_id": g.away_team_id,
        "home_score": g.home_score,
        "away_score": g.away_score,
        "home_penalty_score": g.home_penalty_score,
        "away_penalty_score": g.away_penalty_score,
        "sota_id": str(g.sota_id) if g.sota_id else None,
        "sync_disabled": g.sync_disabled,
    }


def _is_played(g: Game) -> bool:
    return (
        g.status in _PLAYED_STATUSES
        and g.home_score is not None
        and g.away_score is not None
    )


def _is_already_annulled(g: Game) -> bool:
    return (
        g.status == GameStatus.cancelled
        and g.sync_disabled
        and g.home_score is None
        and g.away_score is None
        and g.home_penalty_score is None
        and g.away_penalty_score is None
    )


def _opponent_deltas(games: list[Game], team_id: int) -> list[dict]:
    """Per-opponent loss of points/goals/games when this team's results vanish."""
    deltas: list[dict] = []
    for g in games:
        if not _is_played(g):
            continue
        if g.home_team_id == team_id:
            opp_id, team_score, opp_score = g.away_team_id, g.home_score, g.away_score
        else:
            opp_id, team_score, opp_score = g.home_team_id, g.away_score, g.home_score
        if opp_score > team_score:
            pts = 3
        elif opp_score == team_score:
            pts = 1
        else:
            pts = 0
        deltas.append({
            "opponent_id": opp_id,
            "game_id": g.id,
            "tour": g.tour,
            "points_removed": pts,
            "games_played_removed": 1,
            "goals_for_removed": opp_score,
            "goals_against_removed": team_score,
        })
    return deltas


async def _count_live(db) -> int:
    return await db.scalar(
        select(func.count()).select_from(Game).where(Game.status == GameStatus.live)
    ) or 0


async def run(
    *,
    season_id: int,
    team_id: int,
    apply: bool,
    reason: str,
    audit_dir: str,
    allow_live: bool = False,
) -> None:
    async with AsyncSessionLocal() as db:
        mode = "APPLY" if apply else "DRY-RUN"
        logger.info("=" * 70)
        logger.info("%s — withdraw team_id=%s from season_id=%s", mode, team_id, season_id)

        team = await db.get(Team, team_id)
        team_name = team.name if team else f"<team {team_id}>"
        logger.info("Team: %s", team_name)

        # 1) Live guard.
        live = await _count_live(db)
        if live:
            msg = f"{live} live game(s) in progress — modifying production during live."
            if apply and not allow_live:
                raise SystemExit(
                    f"ABORT: {msg} Pass --allow-live to override (this is a "
                    f"DATA-only change; it does NOT restart the backend)."
                )
            logger.warning("WARNING: %s", msg)

        # 2) Load the team's games for this season.
        games = list((await db.execute(
            select(Game)
            .where(
                Game.season_id == season_id,
                or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
            )
            .order_by(Game.tour, Game.date)
        )).scalars().all())

        if not games:
            raise SystemExit(f"ABORT: no games found for team {team_id} in season {season_id}")

        total = len(games)
        played = sum(1 for g in games if _is_played(g))
        logger.info("Games in season: total=%d, played(with result)=%d", total, played)

        # 3) Regulation verdict — only the annul branch is supported here.
        outcome = compute_withdrawal_outcome(played, total)
        logger.info("Regulation verdict (reg. 2.16): %s (half = %.1f)", outcome, total / 2)
        if outcome != "annul":
            raise SystemExit(
                f"ABORT: outcome is '{outcome}', not 'annul'. This script only "
                f"implements the annulment branch (played < half). played={played}, total={total}."
            )

        # Aux rows.
        score_row = (await db.execute(
            select(ScoreTable).where(
                ScoreTable.season_id == season_id, ScoreTable.team_id == team_id
            )
        )).scalar_one_or_none()
        participant_row = (await db.execute(
            select(SeasonParticipant).where(
                SeasonParticipant.season_id == season_id,
                SeasonParticipant.team_id == team_id,
            )
        )).scalar_one_or_none()
        tss_row = (await db.execute(
            select(TeamSeasonStats).where(
                TeamSeasonStats.season_id == season_id,
                TeamSeasonStats.team_id == team_id,
            )
        )).scalar_one_or_none()

        deltas = _opponent_deltas(games, team_id)

        # 4) BEFORE snapshot + audit file (always written, suffixed in dry-run).
        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "season_id": season_id,
            "team_id": team_id,
            "team_name": team_name,
            "reason": reason,
            "verdict": outcome,
            "played": played,
            "total": total,
            "games": [_game_row(g) for g in games],
            # Full row values (not just presence) so the change is restore-grade.
            "score_table_row": _row_to_dict(score_row),
            "season_participant_row": _row_to_dict(participant_row),
            "team_season_stats_row": _row_to_dict(tss_row),
            "opponent_deltas": deltas,
        }
        suffix = "apply" if apply else "dryrun"
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        audit_path = f"{audit_dir.rstrip('/')}/withdraw_team{team_id}_season{season_id}_{suffix}_{stamp}.json"
        with open(audit_path, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, ensure_ascii=False, indent=2)
        logger.info("Audit snapshot written: %s", audit_path)

        logger.info("--- BEFORE: games ---")
        for g in games:
            logger.info(
                "  game=%s tour=%s %s %s:%s status=%s sync_disabled=%s",
                g.id, g.tour, g.date, g.home_score, g.away_score,
                g.status.value if g.status else None, g.sync_disabled,
            )
        logger.info(
            "--- aux rows: score_table=%s season_participant=%s team_season_stats=%s ---",
            "present" if score_row else "absent",
            "present" if participant_row else "absent",
            "present" if tss_row else "absent",
        )
        logger.info("--- opponent deltas (each loses its match vs the withdrawn team) ---")
        for d in deltas:
            logger.info(
                "  opponent_id=%s game=%s tour=%s  points-=%d gp-=1 gf-=%d ga-=%d",
                d["opponent_id"], d["game_id"], d["tour"],
                d["points_removed"], d["goals_for_removed"], d["goals_against_removed"],
            )

        # 5) Mutations.
        verb = "Updating" if apply else "WOULD update"
        to_change = [g for g in games if not _is_already_annulled(g)]
        logger.info("%s %d game(s) → cancelled + sync_disabled + scores NULL "
                    "(%d already annulled)", verb, len(to_change), total - len(to_change))

        if apply:
            for g in to_change:
                g.status = GameStatus.cancelled
                g.sync_disabled = True
                g.home_score = None
                g.away_score = None
                g.home_penalty_score = None
                g.away_penalty_score = None

            deleted_st = (await db.execute(
                delete(ScoreTable).where(
                    ScoreTable.season_id == season_id, ScoreTable.team_id == team_id
                )
            )).rowcount
            deleted_sp = (await db.execute(
                delete(SeasonParticipant).where(
                    SeasonParticipant.season_id == season_id,
                    SeasonParticipant.team_id == team_id,
                )
            )).rowcount
            deleted_tss = (await db.execute(
                delete(TeamSeasonStats).where(
                    TeamSeasonStats.season_id == season_id,
                    TeamSeasonStats.team_id == team_id,
                )
            )).rowcount

            await db.commit()
            logger.info(
                "COMMITTED: %d games annulled; deleted score_table=%d, "
                "season_participant=%d, team_season_stats=%d",
                len(to_change), deleted_st, deleted_sp, deleted_tss,
            )

            # AFTER snapshot.
            after = list((await db.execute(
                select(Game)
                .where(
                    Game.season_id == season_id,
                    or_(Game.home_team_id == team_id, Game.away_team_id == team_id),
                )
                .order_by(Game.tour, Game.date)
            )).scalars().all())
            remaining_played = sum(1 for g in after if _is_played(g))
            logger.info("--- AFTER: played-with-result games for team = %d (expect 0) ---",
                        remaining_played)
        else:
            logger.info("DRY-RUN: would delete score_table=%s, season_participant=%s, "
                        "team_season_stats=%s; no writes performed.",
                        "yes" if score_row else "no",
                        "yes" if participant_row else "no",
                        "yes" if tss_row else "no")

        logger.info("Done (%s).", mode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument("--team-id", type=int, required=True)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write to DB (default: dry-run)")
    parser.add_argument("--reason", type=str, default=DEFAULT_REASON)
    parser.add_argument("--audit-dir", type=str, default=".",
                        help="Directory for the JSON audit snapshot")
    parser.add_argument("--allow-live", action="store_true",
                        help="Override the live-game guard (DATA-only change; "
                             "does NOT restart the backend)")
    args = parser.parse_args()

    asyncio.run(run(
        season_id=args.season_id,
        team_id=args.team_id,
        apply=args.apply,
        reason=args.reason,
        audit_dir=args.audit_dir,
        allow_live=args.allow_live,
    ))


if __name__ == "__main__":
    main()
