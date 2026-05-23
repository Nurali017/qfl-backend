"""
Stats sync service.

Handles synchronization of team season statistics from SOTA API.
Score table is managed locally — no longer synced from SOTA.
"""
import logging

from sqlalchemy import or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError

# Advisory lock namespace for team_season_stats writers.
# Pairs with player_sync._PLAYER_STATS_LOCK_NS to serialize concurrent UPSERTs.
_TEAM_STATS_LOCK_NS = 2
_LOCK_TIMEOUT = "60s"

from app.config import get_settings
from app.models import Game, GameStatus, GameTeamStats, ScoreTable, SeasonParticipant, TeamSeasonStats
from app.services.sync.base import BaseSyncService, TEAM_SEASON_STATS_FIELDS
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)


class StatsSyncService(BaseSyncService):
    """
    Service for syncing team season statistics.

    Handles:
    - Team season statistics (92 metrics from v2 API)
    """

    async def sync_team_season_stats(self, season_id: int) -> int:
        """
        Sync season stats for ALL teams in a season from SOTA API v2.

        Collect-then-write: resolve the team set and (when applicable) local
        aggregates, commit to release the connection, fetch every team's v2
        stats from SOTA with no transaction open, then upsert everything under
        one short advisory-locked write transaction. This keeps the connection
        out of ``idle in transaction`` and the advisory lock held only for the
        write — see the extended-stats incident notes.

        Uses v2 endpoint which provides 92 metrics (xG, possession, duels, etc.).
        """
        # Phase A — reads. Build candidate team set from all season sources:
        # score_table + season_participants + games. This prevents partial sync
        # when score_table is incomplete (e.g. split groups). Then commit so the
        # connection returns to the pool for the fetch phase.
        score_table_result = await self.db.execute(
            select(ScoreTable).where(ScoreTable.season_id == season_id)
        )
        team_ids = {st.team_id for st in score_table_result.scalars().all()}

        participants_result = await self.db.execute(
            select(SeasonParticipant.team_id).where(SeasonParticipant.season_id == season_id)
        )
        for team_id in participants_result.scalars().all():
            if team_id:
                team_ids.add(team_id)

        # Cup-style seasons or partially populated participants are additionally covered by games.
        games_result = await self.db.execute(
            select(Game.home_team_id, Game.away_team_id).where(Game.season_id == season_id)
        )
        for home_id, away_id in games_result.all():
            if home_id:
                team_ids.add(home_id)
            if away_id:
                team_ids.add(away_id)

        if not team_ids:
            await self.db.rollback()  # leave the session clean for the caller
            return 0

        # Resolve all SOTA season IDs (usually 1, but 2L has SW+NE)
        sota_season_ids = await self.get_all_sota_season_ids(season_id)

        # SOTA does not expose v2 team season-stats for tournaments without
        # extended stats (e.g. Вторая Лига). For those we aggregate locally
        # from games + game_team_stats — the per-match data is already synced.
        use_local_aggregate = (
            season_id not in get_settings().extended_stats_season_ids
        )

        # Pre-load local aggregates once for the whole season when needed.
        local_aggregates: dict[int, dict] = {}
        if use_local_aggregate:
            local_aggregates = await self._aggregate_team_stats_locally(season_id)
        await self.db.commit()

        # Phase B — fetch (no transaction open). The local-aggregate path makes
        # no HTTP at all; the extended path fetches v2 stats per team.
        collected: list[tuple[int, dict]] = []
        for team_id in sorted(team_ids):
            try:
                if use_local_aggregate:
                    stats = local_aggregates.get(team_id, {})
                else:
                    stats = {}
                    # Try each SOTA season ID (team belongs to one conference)
                    for sid in sota_season_ids:
                        stats = await self.client.get_team_season_stats_v2(team_id, sid)
                        if stats and stats.get("games_played"):
                            break
            except Exception as e:
                logger.warning(f"Failed to fetch team season stats for team {team_id}: {e}")
                continue
            collected.append((team_id, stats))

        # Phase C — write under a short advisory-locked transaction.
        if not collected:
            return 0
        await self.db.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
        await self.db.execute(
            text("SELECT pg_advisory_xact_lock(:ns, :sid)"),
            {"ns": _TEAM_STATS_LOCK_NS, "sid": season_id},
        )
        count = 0
        for team_id, stats in collected:
            stmt = self._build_team_upsert(team_id, season_id, stats)
            try:
                async with self.db.begin_nested():
                    await self.db.execute(stmt)
            except DBAPIError as db_exc:
                logger.warning(
                    "DB error upserting team_season_stats for team %s: %s",
                    team_id, db_exc,
                )
                continue
            count += 1

        await self.db.commit()
        logger.info(f"Synced {count} team season stats for season {season_id}")
        return count

    def _build_team_upsert(self, team_id: int, season_id: int, stats: dict):
        """Build the TeamSeasonStats upsert statement for one team."""
        # Extract extra stats (fields not in our known list)
        extra_stats = {k: v for k, v in stats.items() if k not in TEAM_SEASON_STATS_FIELDS}

        stmt = insert(TeamSeasonStats).values(
            team_id=team_id,
            season_id=season_id,
            # Basic stats
            games_played=stats.get("games_played"),
            games_total=stats.get("games_total"),
            win=stats.get("win"),
            draw=stats.get("draw"),
            match_loss=stats.get("match_loss"),
            goal=stats.get("goal"),
            goals_conceded=stats.get("goals_conceded"),
            goals_difference=stats.get("goals_difference"),
            points=stats.get("points"),
            # xG
            xg=stats.get("xg"),
            xg_per_match=stats.get("xg_per_match"),
            opponent_xg=stats.get("opponent_xg"),
            # Shots
            shot=stats.get("shot"),
            shots_on_goal=stats.get("shots_on_goal"),
            shots_on_goal_per_match=stats.get("shots_on_goal_per_match"),
            shots_off_goal=stats.get("shots_off_goal"),
            shot_per_90=stats.get("shot_per_90"),
            shot_per_match=stats.get("shot_per_match"),
            shot_to_goal=stats.get("shot_to_goal"),
            goal_to_shot_ratio=stats.get("goal_to_shot_ratio"),
            shots_against_penalty=stats.get("shots_against_penalty"),
            # Possession
            possession_percent_average=stats.get("possession_percent_average"),
            # Passes
            passes=stats.get("pass"),
            pass_per_match=stats.get("pass_per_match"),
            pass_ratio=stats.get("pass_ratio"),
            pass_forward=stats.get("pass_forward"),
            pass_forward_per_match=stats.get("pass_forward_per_match"),
            pass_forward_ratio=stats.get("pass_forward_ratio"),
            pass_long=stats.get("pass_long"),
            pass_long_per_match=stats.get("pass_long_per_match"),
            pass_long_ratio=stats.get("pass_long_ratio"),
            pass_progressive=stats.get("pass_progressive"),
            pass_progressive_per_match=stats.get("pass_progressive_per_match"),
            pass_cross=stats.get("pass_cross"),
            pass_cross_per_match=stats.get("pass_cross_per_match"),
            pass_cross_ratio=stats.get("pass_cross_ratio"),
            pass_to_box=stats.get("pass_to_box"),
            pass_to_box_per_match=stats.get("pass_to_box_per_match"),
            pass_to_box_ratio=stats.get("pass_to_box_ratio"),
            pass_to_3rd=stats.get("pass_to_3rd"),
            pass_to_3rd_acc=stats.get("pass_to_3rd_acc"),
            pass_to_3rd_per_match=stats.get("pass_to_3rd_per_match"),
            pass_to_3rd_ratio=stats.get("pass_to_3rd_ratio"),
            key_pass=stats.get("key_pass"),
            key_pass_per_match=stats.get("key_pass_per_match"),
            key_pass_ratio=stats.get("key_pass_ratio"),
            goal_pass=stats.get("goal_pass"),
            # Free kicks
            freekick_pass=stats.get("freekick_pass"),
            freekick_pass_per_match=stats.get("freekick_pass_per_match"),
            freekick_shot=stats.get("freekick_shot"),
            freekick_shot_per_match=stats.get("freekick_shot_per_match"),
            # Duels
            duel=stats.get("duel"),
            duel_per_match=stats.get("duel_per_match"),
            duel_ratio=stats.get("duel_ratio"),
            aerial_duel_offence=stats.get("aerial_duel_offence"),
            aerial_duel_offence_per_match=stats.get("aerial_duel_offence_per_match"),
            aerial_duel_offence_ratio=stats.get("aerial_duel_offence_ratio"),
            aerial_duel_defence=stats.get("aerial_duel_defence"),
            aerial_duel_defence_per_match=stats.get("aerial_duel_defence_per_match"),
            aerial_duel_defence_ratio=stats.get("aerial_duel_defence_ratio"),
            ground_duel_offence=stats.get("ground_duel_offence"),
            ground_duel_offence_per_match=stats.get("ground_duel_offence_per_match"),
            ground_duel_offence_ratio=stats.get("ground_duel_offence_ratio"),
            ground_duel_defence=stats.get("ground_duel_defence"),
            ground_duel_defence_per_match=stats.get("ground_duel_defence_per_match"),
            ground_duel_defence_ratio=stats.get("ground_duel_defence_ratio"),
            # Defense
            tackle=stats.get("tackle"),
            tackle_per_match=stats.get("tackle_per_match"),
            tackle1_1=stats.get("tackle1-1"),
            tackle1_1_per_match=stats.get("tackle1-1_per_match"),
            tackle1_1_ratio=stats.get("tackle1-1_ratio"),
            interception=stats.get("interception"),
            interception_per_match=stats.get("interception_per_match"),
            recovery=stats.get("recovery"),
            recovery_per_match=stats.get("recovery_per_match"),
            # Dribbles
            dribble=stats.get("dribble"),
            dribble_per_match=stats.get("dribble_per_match"),
            dribble_ratio=stats.get("dribble_ratio"),
            # Penalties
            penalty=stats.get("penalty"),
            penalty_ratio=stats.get("penalty_ratio"),
            save_penalty_ratio=stats.get("save_penalty_ratio"),
            # Discipline
            foul=stats.get("foul"),
            foul_taken=stats.get("foul_taken"),
            yellow_cards=stats.get("yellow_cards"),
            second_yellow_cards=stats.get("second_yellow_cards"),
            red_cards=stats.get("red_cards"),
            # Set pieces
            corner=stats.get("corner"),
            corner_per_match=stats.get("corner_per_match"),
            offside=stats.get("offside"),
            out=stats.get("out"),
            # Visitors
            visitor_total=stats.get("visitor_total"),
            average_visitors=stats.get("average_visitors"),
            # Extra stats for unknown fields
            extra_stats=extra_stats if extra_stats else None,
            updated_at=utcnow(),
        )
        return stmt.on_conflict_do_update(
            index_elements=["team_id", "season_id"],
            set_={
                "games_played": stmt.excluded.games_played,
                "games_total": stmt.excluded.games_total,
                "win": stmt.excluded.win,
                "draw": stmt.excluded.draw,
                "match_loss": stmt.excluded.match_loss,
                "goal": stmt.excluded.goal,
                "goals_conceded": stmt.excluded.goals_conceded,
                "goals_difference": stmt.excluded.goals_difference,
                "points": stmt.excluded.points,
                "xg": stmt.excluded.xg,
                "xg_per_match": stmt.excluded.xg_per_match,
                "opponent_xg": stmt.excluded.opponent_xg,
                "shot": stmt.excluded.shot,
                "shots_on_goal": stmt.excluded.shots_on_goal,
                "shots_on_goal_per_match": stmt.excluded.shots_on_goal_per_match,
                "shots_off_goal": stmt.excluded.shots_off_goal,
                "shot_per_90": stmt.excluded.shot_per_90,
                "shot_per_match": stmt.excluded.shot_per_match,
                "shot_to_goal": stmt.excluded.shot_to_goal,
                "goal_to_shot_ratio": stmt.excluded.goal_to_shot_ratio,
                "shots_against_penalty": stmt.excluded.shots_against_penalty,
                "possession_percent_average": stmt.excluded.possession_percent_average,
                "passes": stmt.excluded.passes,
                "pass_per_match": stmt.excluded.pass_per_match,
                "pass_ratio": stmt.excluded.pass_ratio,
                "pass_forward": stmt.excluded.pass_forward,
                "pass_forward_per_match": stmt.excluded.pass_forward_per_match,
                "pass_forward_ratio": stmt.excluded.pass_forward_ratio,
                "pass_long": stmt.excluded.pass_long,
                "pass_long_per_match": stmt.excluded.pass_long_per_match,
                "pass_long_ratio": stmt.excluded.pass_long_ratio,
                "pass_progressive": stmt.excluded.pass_progressive,
                "pass_progressive_per_match": stmt.excluded.pass_progressive_per_match,
                "pass_cross": stmt.excluded.pass_cross,
                "pass_cross_per_match": stmt.excluded.pass_cross_per_match,
                "pass_cross_ratio": stmt.excluded.pass_cross_ratio,
                "pass_to_box": stmt.excluded.pass_to_box,
                "pass_to_box_per_match": stmt.excluded.pass_to_box_per_match,
                "pass_to_box_ratio": stmt.excluded.pass_to_box_ratio,
                "pass_to_3rd": stmt.excluded.pass_to_3rd,
                "pass_to_3rd_acc": stmt.excluded.pass_to_3rd_acc,
                "pass_to_3rd_per_match": stmt.excluded.pass_to_3rd_per_match,
                "pass_to_3rd_ratio": stmt.excluded.pass_to_3rd_ratio,
                "key_pass": stmt.excluded.key_pass,
                "key_pass_per_match": stmt.excluded.key_pass_per_match,
                "key_pass_ratio": stmt.excluded.key_pass_ratio,
                "goal_pass": stmt.excluded.goal_pass,
                "freekick_pass": stmt.excluded.freekick_pass,
                "freekick_pass_per_match": stmt.excluded.freekick_pass_per_match,
                "freekick_shot": stmt.excluded.freekick_shot,
                "freekick_shot_per_match": stmt.excluded.freekick_shot_per_match,
                "duel": stmt.excluded.duel,
                "duel_per_match": stmt.excluded.duel_per_match,
                "duel_ratio": stmt.excluded.duel_ratio,
                "aerial_duel_offence": stmt.excluded.aerial_duel_offence,
                "aerial_duel_offence_per_match": stmt.excluded.aerial_duel_offence_per_match,
                "aerial_duel_offence_ratio": stmt.excluded.aerial_duel_offence_ratio,
                "aerial_duel_defence": stmt.excluded.aerial_duel_defence,
                "aerial_duel_defence_per_match": stmt.excluded.aerial_duel_defence_per_match,
                "aerial_duel_defence_ratio": stmt.excluded.aerial_duel_defence_ratio,
                "ground_duel_offence": stmt.excluded.ground_duel_offence,
                "ground_duel_offence_per_match": stmt.excluded.ground_duel_offence_per_match,
                "ground_duel_offence_ratio": stmt.excluded.ground_duel_offence_ratio,
                "ground_duel_defence": stmt.excluded.ground_duel_defence,
                "ground_duel_defence_per_match": stmt.excluded.ground_duel_defence_per_match,
                "ground_duel_defence_ratio": stmt.excluded.ground_duel_defence_ratio,
                "tackle": stmt.excluded.tackle,
                "tackle_per_match": stmt.excluded.tackle_per_match,
                "tackle1_1": stmt.excluded.tackle1_1,
                "tackle1_1_per_match": stmt.excluded.tackle1_1_per_match,
                "tackle1_1_ratio": stmt.excluded.tackle1_1_ratio,
                "interception": stmt.excluded.interception,
                "interception_per_match": stmt.excluded.interception_per_match,
                "recovery": stmt.excluded.recovery,
                "recovery_per_match": stmt.excluded.recovery_per_match,
                "dribble": stmt.excluded.dribble,
                "dribble_per_match": stmt.excluded.dribble_per_match,
                "dribble_ratio": stmt.excluded.dribble_ratio,
                "penalty": stmt.excluded.penalty,
                "penalty_ratio": stmt.excluded.penalty_ratio,
                "save_penalty_ratio": stmt.excluded.save_penalty_ratio,
                "foul": stmt.excluded.foul,
                "foul_taken": stmt.excluded.foul_taken,
                "yellow_cards": stmt.excluded.yellow_cards,
                "second_yellow_cards": stmt.excluded.second_yellow_cards,
                "red_cards": stmt.excluded.red_cards,
                "corner": stmt.excluded.corner,
                "corner_per_match": stmt.excluded.corner_per_match,
                "offside": stmt.excluded.offside,
                "out": stmt.excluded.out,
                "visitor_total": stmt.excluded.visitor_total,
                "average_visitors": stmt.excluded.average_visitors,
                "extra_stats": stmt.excluded.extra_stats,
                "updated_at": stmt.excluded.updated_at,
            },
        )

    async def _aggregate_team_stats_locally(self, season_id: int) -> dict[int, dict]:
        """Aggregate season stats per team from local games + game_team_stats.

        Used for tournaments without extended SOTA v2 stats (e.g. Вторая Лига),
        where /v2/teams/{id}/season_stats/?season_id=... returns 404.

        Returns: {team_id: stats_dict} — keys mirror SOTA v2 response so the
        existing UPSERT block treats it the same way.
        """
        result = await self.db.execute(
            select(Game).where(
                Game.season_id == season_id,
                Game.status == GameStatus.finished,
            )
        )
        finished = list(result.scalars().all())
        if not finished:
            return {}

        game_ids = [g.id for g in finished]

        gts_result = await self.db.execute(
            select(GameTeamStats).where(GameTeamStats.game_id.in_(game_ids))
        )
        gts_by_key: dict[tuple[int, int], GameTeamStats] = {
            (row.game_id, row.team_id): row for row in gts_result.scalars().all()
        }

        per_team: dict[int, dict] = {}
        for g in finished:
            for team_id, opp_id, gf, ga in (
                (g.home_team_id, g.away_team_id, g.home_score or 0, g.away_score or 0),
                (g.away_team_id, g.home_team_id, g.away_score or 0, g.home_score or 0),
            ):
                if not team_id:
                    continue
                bucket = per_team.setdefault(team_id, {
                    "games_played": 0, "win": 0, "draw": 0, "match_loss": 0,
                    "goal": 0, "goals_conceded": 0,
                    "shot": 0, "shots_on_goal": 0, "shots_off_goal": 0,
                    "pass": 0, "foul": 0, "corner": 0, "offside": 0,
                    "yellow_cards": 0, "red_cards": 0,
                })
                bucket["games_played"] += 1
                bucket["goal"] += gf
                bucket["goals_conceded"] += ga
                if gf > ga:
                    bucket["win"] += 1
                elif gf == ga:
                    bucket["draw"] += 1
                else:
                    bucket["match_loss"] += 1

                gts = gts_by_key.get((g.id, team_id))
                if gts is not None:
                    bucket["shot"] += gts.shots or 0
                    bucket["shots_on_goal"] += gts.shots_on_goal or 0
                    bucket["shots_off_goal"] += gts.shots_off_goal or 0
                    bucket["pass"] += gts.passes or 0
                    bucket["foul"] += gts.fouls or 0
                    bucket["corner"] += gts.corners or 0
                    bucket["offside"] += gts.offsides or 0
                    bucket["yellow_cards"] += gts.yellow_cards or 0
                    bucket["red_cards"] += gts.red_cards or 0

        for team_id, bucket in per_team.items():
            gp = bucket["games_played"] or 1
            bucket["points"] = bucket["win"] * 3 + bucket["draw"]
            bucket["goals_difference"] = bucket["goal"] - bucket["goals_conceded"]
            bucket["shot_per_match"] = round(bucket["shot"] / gp, 2)

        return per_team
