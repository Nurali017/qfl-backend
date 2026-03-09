"""
Game sync service.

Handles synchronization of games, game statistics, and game events from SOTA API.
"""
import asyncio
import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from app.models import (
    Game, Team, Player, GameTeamStats, GamePlayerStats,
    GameEvent, GameEventType, GameLineup,
)
from app.services.sync.base import (
    BaseSyncService, parse_date, parse_time,
    GAME_PLAYER_STATS_FIELDS, GAME_TEAM_STATS_FIELDS,
)
from app.services.season_visibility import get_current_season_id

logger = logging.getLogger(__name__)


class GameSyncService(BaseSyncService):
    """
    Service for syncing game data, statistics, and events.

    Handles:
    - Game schedule and scores
    - Team and player statistics per game
    - Game events (goals, cards, substitutions)
    - Formations and metadata from live endpoints
    """

    async def sync_games(self, season_id: int) -> int:
        """
        Game schedule is managed locally — SOTA sync is disabled.

        Games are created and updated manually via the admin panel.
        SOTA is not the source of truth for game schedule or scores.
        """
        logger.info("sync_games is disabled: games are managed locally, not from SOTA")
        return 0

    async def _sync_games_from_sota(self, season_id: int) -> int:
        """
        Original SOTA game sync — kept for reference but not called.
        """
        games_data = await self.client.get_games(season_id)
        count = 0

        for g in games_data:
            game_id = UUID(g["id"])
            home_team = g.get("home_team", {})
            away_team = g.get("away_team", {})

            # Get or create stadium from SOTA
            stadium_name = g.get("stadium")
            stadium_id = await self._get_or_create_stadium(stadium_name)

            # Fallback: Use home team's stadium if SOTA doesn't provide one
            if not stadium_id and home_team.get("id"):
                stadium_id = await self._get_home_stadium_for_team(home_team["id"])

            stmt = insert(Game).values(
                id=game_id,
                date=parse_date(g["date"]),
                time=parse_time(g.get("time")),
                tour=g.get("tour"),
                season_id=g.get("season_id"),
                home_team_id=home_team.get("id") if home_team else None,
                away_team_id=away_team.get("id") if away_team else None,
                home_score=home_team.get("score") if home_team else None,
                away_score=away_team.get("score") if away_team else None,
                has_stats=g.get("has_stats", False),
                stadium_id=stadium_id,
                visitors=g.get("visitors"),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "date": stmt.excluded.date,
                    "time": stmt.excluded.time,
                    "tour": stmt.excluded.tour,
                    "home_score": stmt.excluded.home_score,
                    "away_score": stmt.excluded.away_score,
                    "has_stats": stmt.excluded.has_stats,
                    "stadium_id": stmt.excluded.stadium_id,
                    "visitors": stmt.excluded.visitors,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        logger.info(f"Synced {count} games for season {season_id}")
        return count

    async def sync_game_stats(self, game_id: int) -> dict:
        """
        Sync statistics for a specific game.

        Args:
            game_id: Game int ID

        Returns:
            Dict with team and player counts
        """
        game = await self.db.get(Game, game_id)
        if not game or not game.sota_id:
            return {"error": f"Game {game_id} not found or has no sota_id", "teams": 0, "players": 0}
        sota_uuid = str(game.sota_id)

        stats_data = await self.client.get_game_stats(sota_uuid)

        # Build team name to ID mapping from team stats
        team_name_to_id = {}
        for ts in stats_data.get("teams", []):
            if isinstance(ts, dict) and ts.get("id") and ts.get("name"):
                team_name_to_id[ts["name"]] = ts["id"]

        # Sync team stats
        team_count = 0
        for ts in stats_data.get("teams", []):
            stats = ts.get("stats", {})

            # Extract extra stats (fields not in our known list)
            extra_stats = {k: v for k, v in stats.items() if k not in GAME_TEAM_STATS_FIELDS}

            stmt = insert(GameTeamStats).values(
                game_id=game_id,
                team_id=ts["id"],
                possession=stats.get("possession"),
                possession_percent=stats.get("possession_percent"),
                shots=stats.get("shot"),
                shots_on_goal=stats.get("shots_on_goal"),
                shots_off_goal=stats.get("shots_off_goal"),
                passes=stats.get("pass"),
                pass_accuracy=stats.get("pass_accuracy"),
                fouls=stats.get("foul"),
                yellow_cards=stats.get("yellow_cards"),
                red_cards=stats.get("red_cards"),
                corners=stats.get("corner"),
                offsides=stats.get("offside"),
                shots_on_bar=stats.get("shots_on_bar") or stats.get("shot_on_bar"),
                shots_blocked=stats.get("shots_blocked") or stats.get("shot_blocked"),
                penalties=stats.get("penalty") or stats.get("penalties"),
                saves=stats.get("saves") or stats.get("save") or stats.get("save_shot"),
                extra_stats=extra_stats if extra_stats else None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["game_id", "team_id"],
                set_={
                    "possession": stmt.excluded.possession,
                    "possession_percent": stmt.excluded.possession_percent,
                    "shots": stmt.excluded.shots,
                    "shots_on_goal": stmt.excluded.shots_on_goal,
                    "shots_off_goal": stmt.excluded.shots_off_goal,
                    "passes": stmt.excluded.passes,
                    "pass_accuracy": stmt.excluded.pass_accuracy,
                    "fouls": stmt.excluded.fouls,
                    "yellow_cards": stmt.excluded.yellow_cards,
                    "red_cards": stmt.excluded.red_cards,
                    "corners": stmt.excluded.corners,
                    "offsides": stmt.excluded.offsides,
                    "shots_on_bar": stmt.excluded.shots_on_bar,
                    "shots_blocked": stmt.excluded.shots_blocked,
                    "penalties": stmt.excluded.penalties,
                    "saves": stmt.excluded.saves,
                    "extra_stats": stmt.excluded.extra_stats,
                },
            )
            await self.db.execute(stmt)
            team_count += 1

        # Sync player stats
        player_count = 0
        for ps in stats_data.get("players", []):
            player_id = await self._get_or_create_player_by_sota(
                ps.get("id"),
                ps.get("first_name"),
                ps.get("last_name"),
            )
            if player_id is None:
                continue
            stats = ps.get("stats", {})

            # Get team_id from mapping or directly from player stats
            team_id = ps.get("team_id") or team_name_to_id.get(ps.get("team"))
            if not team_id:
                continue  # Skip if we can't determine team

            # Extract extra stats
            extra_stats = {k: v for k, v in stats.items() if k not in GAME_PLAYER_STATS_FIELDS}

            stmt = insert(GamePlayerStats).values(
                game_id=game_id,
                player_id=player_id,
                team_id=team_id,
                minutes_played=ps.get("minutes_played"),
                started=ps.get("started", False),
                position=ps.get("position"),
                shots=stats.get("shot", 0),
                shots_on_goal=stats.get("shots_on_goal", 0),
                shots_off_goal=stats.get("shots_off_goal", 0),
                passes=stats.get("pass", 0),
                pass_accuracy=stats.get("pass_accuracy"),
                duel=stats.get("duel", 0),
                tackle=stats.get("tackle", 0),
                corner=stats.get("corner", 0),
                offside=stats.get("offside", 0),
                foul=stats.get("foul", 0),
                yellow_cards=stats.get("yellow_cards", 0),
                red_cards=stats.get("red_cards", 0),
                extra_stats=extra_stats if extra_stats else None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["game_id", "player_id"],
                set_={
                    "team_id": stmt.excluded.team_id,
                    "minutes_played": stmt.excluded.minutes_played,
                    "started": stmt.excluded.started,
                    "position": stmt.excluded.position,
                    "shots": stmt.excluded.shots,
                    "shots_on_goal": stmt.excluded.shots_on_goal,
                    "shots_off_goal": stmt.excluded.shots_off_goal,
                    "passes": stmt.excluded.passes,
                    "pass_accuracy": stmt.excluded.pass_accuracy,
                    "duel": stmt.excluded.duel,
                    "tackle": stmt.excluded.tackle,
                    "corner": stmt.excluded.corner,
                    "offside": stmt.excluded.offside,
                    "foul": stmt.excluded.foul,
                    "yellow_cards": stmt.excluded.yellow_cards,
                    "red_cards": stmt.excluded.red_cards,
                    "extra_stats": stmt.excluded.extra_stats,
                },
            )
            await self.db.execute(stmt)
            player_count += 1

        await self.db.commit()
        logger.info(f"Synced game stats for {game_id}: {team_count} teams, {player_count} players")

        # Enrich team stats from /em/ live endpoint (has shots_on_bar, saves, etc.)
        try:
            em_ok = await self._enrich_team_stats_from_live(game_id, sota_uuid)
            if em_ok:
                logger.info(f"Enriched team stats from /em/ for game {game_id}")
        except Exception as e:
            logger.warning(f"/em/ team stats enrichment failed for game {game_id}: {e}")

        # Enrich player stats from /em/ endpoint
        try:
            em_player_count = await self._enrich_player_stats_from_em(game_id, sota_uuid)
            if em_player_count:
                logger.info(f"Enriched {em_player_count} players from /em/ for game {game_id}")
        except Exception as e:
            logger.warning(f"/em/ player stats enrichment failed for game {game_id}: {e}")

        # Enrich with v2 stats (after v1 commit — v1 data is never lost)
        v2_count = 0
        try:
            v2_count = await self._enrich_with_v2_stats(game_id, sota_uuid)
            logger.info(f"v2 enrichment: {v2_count} players for game {game_id}")
        except Exception as e:
            logger.error(f"v2 enrichment failed for game {game_id}: {e}")

        return {"teams": team_count, "players": player_count, "v2_enriched": v2_count}

    async def _enrich_team_stats_from_live(self, game_id: int, sota_game_uuid: str) -> bool:
        """Enrich team stats with data from /em/{id}-stat.json (shots_on_bar, saves, etc.)."""
        em_data = await self.client.get_live_match_stats(sota_game_uuid)

        # Build lookup: metric -> {home, away}
        # Capture per-half breakdowns (_1, _2) into by_half dict
        import re
        em_stats: dict[str, dict] = {}
        by_half: dict[str, dict[str, dict]] = {"1": {}, "2": {}}
        for item in em_data:
            metric = item.get("metric", "")
            match = re.match(r"^(.+)_([12])$", metric)
            if match:
                base, half = match.groups()
                by_half[half][base] = {"home": item.get("home"), "away": item.get("away")}
                continue
            if "_" in metric and metric.rsplit("_", 1)[-1].isdigit():
                continue  # still skip _3, _4, _5
            em_stats[metric] = {"home": item.get("home"), "away": item.get("away")}

        if not em_stats:
            return False

        game = await self.db.get(Game, game_id)
        if not game:
            return False

        # Map of em metric -> (model column, parser)
        def _parse_int(v):
            if v is None:
                return None
            try:
                return int(v)
            except (ValueError, TypeError):
                return None

        def _parse_possession(v):
            if v is None:
                return None
            if isinstance(v, str):
                v = v.replace("%", "").strip()
            try:
                return int(v)
            except (ValueError, TypeError):
                return None

        METRIC_MAP = {
            "shots_on_bar": ("shots_on_bar", _parse_int),
            "shots_blocked": ("shots_blocked", _parse_int),
            "penalties": ("penalties", _parse_int),
            "saves": ("saves", _parse_int),
        }

        for side, team_id in [("home", game.home_team_id), ("away", game.away_team_id)]:
            if not team_id:
                continue
            result = await self.db.execute(
                select(GameTeamStats).where(
                    GameTeamStats.game_id == game_id,
                    GameTeamStats.team_id == team_id,
                )
            )
            ts = result.scalar_one_or_none()
            if not ts:
                continue

            for em_key, (col, parser) in METRIC_MAP.items():
                if em_key in em_stats:
                    val = parser(em_stats[em_key].get(side))
                    if val is not None and getattr(ts, col) is None:
                        setattr(ts, col, val)

            # Also fill possession_percent from /em/ if missing
            if ts.possession_percent is None and "possessions" in em_stats:
                pct = _parse_possession(em_stats["possessions"].get(side))
                if pct is not None:
                    ts.possession_percent = pct

            # Build per-half extra_stats from captured _1/_2 metrics
            side_by_half = {}
            for half_num in ("1", "2"):
                half_data = {}
                for base_metric, vals in by_half[half_num].items():
                    raw = vals.get(side)
                    if base_metric == "possessions":
                        parsed = _parse_possession(raw)
                    else:
                        parsed = _parse_int(raw)
                    if parsed is not None:
                        half_data[base_metric] = parsed
                if half_data:
                    side_by_half[half_num] = half_data
            if side_by_half:
                ts.extra_stats = {**(ts.extra_stats or {}), "by_half": side_by_half}

        await self.db.commit()
        return True

    async def _enrich_player_stats_from_em(self, game_id: int, sota_game_uuid: str) -> int:
        """Fetch per-player stats from /em/{id}-players-{side}.json and store in extra_stats."""
        result = await self.db.execute(
            select(GamePlayerStats)
            .where(GamePlayerStats.game_id == game_id)
            .options(selectinload(GamePlayerStats.player))
        )
        player_stats_rows = list(result.scalars().all())

        # Build lookup: sota_id -> GamePlayerStats row
        sota_lookup: dict[str, GamePlayerStats] = {}
        for ps in player_stats_rows:
            if ps.player and ps.player.sota_id:
                sota_lookup[str(ps.player.sota_id)] = ps

        if not sota_lookup:
            return 0

        SKIP_FIELDS = {"kind", "team", "first_name", "last_name", "full_name", "number"}
        enriched = 0

        for side in ("home", "away"):
            try:
                em_players = await self.client.get_live_match_player_stats(sota_game_uuid, side)
            except Exception as e:
                logger.warning(f"/em/ player stats for {side} failed: {e}")
                continue

            if not isinstance(em_players, list):
                continue

            for ep in em_players:
                player_id = ep.get("id")
                if not player_id:
                    continue
                ps = sota_lookup.get(str(player_id))
                if not ps:
                    continue
                em_data = {k: v for k, v in ep.items() if k not in SKIP_FIELDS and k != "id"}
                if em_data:
                    ps.extra_stats = {**(ps.extra_stats or {}), "em_stats": em_data}
                    enriched += 1

            await asyncio.sleep(0.2)

        if enriched:
            await self.db.commit()
        return enriched

    async def _enrich_with_v2_stats(self, game_id: int, sota_game_uuid: str) -> int:
        """Fetch v2 per-player stats and merge into extra_stats JSONB."""
        result = await self.db.execute(
            select(GamePlayerStats)
            .where(GamePlayerStats.game_id == game_id)
            .options(selectinload(GamePlayerStats.player))
        )
        player_stats_rows = list(result.scalars().all())
        enriched = 0
        for ps in player_stats_rows:
            if not ps.player or not ps.player.sota_id:
                continue
            try:
                v2_data = await self.client.get_player_game_stats_v2(
                    str(ps.player.sota_id), sota_game_uuid
                )
                if v2_data:
                    ps.extra_stats = {**(ps.extra_stats or {}), **v2_data}
                    enriched += 1
            except Exception as e:
                logger.warning(f"v2 stats failed for player {ps.player_id}: {e}")
            await asyncio.sleep(0.2)
        if enriched:
            await self.db.commit()
        return enriched

    async def _get_or_create_player_by_sota(
        self,
        sota_id_raw: str | None,
        first_name: str | None,
        last_name: str | None,
    ) -> int | None:
        if not sota_id_raw:
            return None

        try:
            sota_id = UUID(str(sota_id_raw))
        except (ValueError, TypeError):
            return None

        result = await self.db.execute(select(Player).where(Player.sota_id == sota_id))
        player = result.scalar_one_or_none()

        if player is not None:
            return player.id

        player = Player(
            sota_id=sota_id,
            first_name=first_name or "",
            last_name=last_name or "",
            updated_at=datetime.utcnow(),
        )
        self.db.add(player)
        await self.db.flush()
        return player.id

    def _event_signature(self, event_type_value: str, half: int, minute: int,
                         player_id: int | None, player_name: str | None) -> tuple:
        """Build a signature tuple for event matching."""
        name = player_name.strip().lower() if player_name else ""
        return (half, minute, event_type_value, player_id, name)

    def _signatures_match(self, sig_a: tuple, sig_b: tuple) -> bool:
        """Check if two event signatures match (by player_id OR by name)."""
        half_a, min_a, type_a, pid_a, name_a = sig_a
        half_b, min_b, type_b, pid_b, name_b = sig_b
        if half_a != half_b or min_a != min_b or type_a != type_b:
            return False
        if pid_a and pid_b and pid_a == pid_b:
            return True
        if name_a and name_b and name_a == name_b:
            return True
        return False

    async def sync_game_events(self, game_id: int) -> dict:
        """
        Sync events for a specific game from SOTA /em/ endpoint.

        Full reconciliation: adds new events, updates changed ones,
        deletes SOTA events that no longer exist. Manual events are protected.
        """
        game = await self.db.get(Game, game_id)
        if not game:
            return {"error": f"Game {game_id} not found", "added": 0, "updated": 0, "deleted": 0}
        if not game.sota_id:
            return {"error": f"Game {game_id} has no sota_id", "added": 0, "updated": 0, "deleted": 0}
        sota_uuid = str(game.sota_id)

        # Load teams
        home_team = None
        away_team = None
        if game.home_team_id:
            result = await self.db.execute(select(Team).where(Team.id == game.home_team_id))
            home_team = result.scalar_one_or_none()
        if game.away_team_id:
            result = await self.db.execute(select(Team).where(Team.id == game.away_team_id))
            away_team = result.scalar_one_or_none()

        # Load all existing events
        result = await self.db.execute(
            select(GameEvent).where(GameEvent.game_id == game_id)
        )
        existing_events = list(result.scalars().all())

        # Separate SOTA events from manual events
        sota_events = [e for e in existing_events if e.source == "sota"]

        # Build list-based signature map for SOTA events (supports duplicates)
        sota_by_sig: dict[tuple, list[GameEvent]] = {}
        for e in sota_events:
            sig = self._event_signature(e.event_type.value, e.half, e.minute, e.player_id, e.player_name)
            sota_by_sig.setdefault(sig, []).append(e)

        # Fetch fresh events from SOTA
        events_data = await self.client.get_live_match_events(sota_uuid)

        # Safety check: if SOTA returns empty but we have SOTA events, skip deletion
        skip_deletes = False
        if not events_data and sota_events:
            logger.warning(
                "Game %s: SOTA returned empty events but %d sota events exist in DB — skipping deletes",
                game_id, len(sota_events),
            )
            skip_deletes = True

        ACTION_TYPE_MAP = {
            "ГОЛ": GameEventType.goal,
            "АВТОГОЛ": GameEventType.own_goal,
            "ПЕНАЛЬТИ": GameEventType.penalty,
            "НЕЗАБИТЫЙ ПЕНАЛЬТИ": GameEventType.missed_penalty,
            "ГОЛЕВОЙ ПАС": GameEventType.assist,
            "ЖК": GameEventType.yellow_card,
            "2ЖК": GameEventType.second_yellow,
            "КК": GameEventType.red_card,
            "ЗАМЕНА": GameEventType.substitution,
        }

        # Track which DB events were matched
        matched_db_events: set[int] = set()
        added = 0
        updated = 0
        assists_map: dict[tuple, dict] = {}
        all_goal_events: list[GameEvent] = []

        for event_data in events_data:
            action = event_data.get("action", "")
            event_type = ACTION_TYPE_MAP.get(action)
            if not event_type:
                continue

            standard = (event_data.get("standard") or "").strip().upper()
            if event_type == GameEventType.goal and standard == "ПЕНАЛЬТИ":
                event_type = GameEventType.penalty

            half = event_data.get("half", 1)
            minute = event_data.get("time", 0)
            first_name1 = event_data.get("first_name1", "")
            last_name1 = event_data.get("last_name1", "")
            player_name = f"{first_name1} {last_name1}".strip()

            team_name = event_data.get("team1", "")
            team_name_normalized = team_name.strip().lower() if team_name else ""
            team_id = None
            if home_team and home_team.name and home_team.name.strip().lower() == team_name_normalized:
                team_id = game.home_team_id
            elif away_team and away_team.name and away_team.name.strip().lower() == team_name_normalized:
                team_id = game.away_team_id

            player_id = await self._find_player_id_from_lineup(
                game_id, first_name1, last_name1, team_id
            )

            # Collect assists into map
            if event_type == GameEventType.assist:
                key = (half, minute, team_id)
                assists_map[key] = {
                    "player_id": player_id,
                    "player_name": player_name,
                }
                continue

            # Find player2
            first_name2 = event_data.get("first_name2", "")
            last_name2 = event_data.get("last_name2", "")
            team2_name = event_data.get("team2", "")
            team2_id = None
            if team2_name:
                team2_normalized = team2_name.strip().lower()
                if home_team and home_team.name and home_team.name.strip().lower() == team2_normalized:
                    team2_id = game.home_team_id
                elif away_team and away_team.name and away_team.name.strip().lower() == team2_normalized:
                    team2_id = game.away_team_id
            player2_id = await self._find_player_id_from_lineup(
                game_id, first_name2, last_name2, team2_id
            )

            # Build new event fields
            new_fields = {
                "half": half,
                "minute": minute,
                "event_type": event_type,
                "team_id": team_id,
                "team_name": team_name,
                "player_id": player_id,
                "player_number": self._parse_number(event_data.get("number1")),
                "player_name": player_name,
                "player2_id": player2_id,
                "player2_number": self._parse_number(event_data.get("number2")),
                "player2_name": f"{first_name2} {last_name2}".strip(),
                "player2_team_name": team2_name,
            }

            # Try to match against existing SOTA event
            sota_sig = self._event_signature(event_type.value, half, minute, player_id, player_name)
            matched_event = None
            for sig, candidates in sota_by_sig.items():
                if self._signatures_match(sota_sig, sig):
                    for candidate in candidates:
                        if candidate.id not in matched_db_events:
                            matched_event = candidate
                            break
                    if matched_event:
                        break

            if matched_event:
                # Mark as matched
                matched_db_events.add(matched_event.id)
                # Check if fields changed, update if needed
                changed = False
                for field, value in new_fields.items():
                    old_value = getattr(matched_event, field)
                    if field == "event_type":
                        if old_value != value:
                            changed = True
                            setattr(matched_event, field, value)
                    elif old_value != value:
                        changed = True
                        setattr(matched_event, field, value)
                if changed:
                    updated += 1
                # Track for assist linking
                if event_type in (GameEventType.goal, GameEventType.penalty):
                    all_goal_events.append(matched_event)
            else:
                # Insert new event
                event = GameEvent(
                    game_id=game_id,
                    source="sota",
                    **new_fields,
                )
                self.db.add(event)
                added += 1
                if event_type in (GameEventType.goal, GameEventType.penalty):
                    all_goal_events.append(event)

        # Delete unmatched SOTA events
        deleted = 0
        if not skip_deletes:
            for e in sota_events:
                if e.id not in matched_db_events:
                    await self.db.delete(e)
                    deleted += 1

        # Link assists to all goal events (clear first, then re-link)
        for event in all_goal_events:
            key = (event.half, event.minute, event.team_id)
            assist_info = assists_map.get(key)
            if assist_info:
                event.assist_player_id = assist_info["player_id"]
                event.assist_player_name = assist_info["player_name"]
            else:
                event.assist_player_id = None
                event.assist_player_name = None

        if added or updated or deleted or assists_map:
            await self.db.commit()
            logger.info(
                "Game %s: events added=%d updated=%d deleted=%d assists=%d",
                game_id, added, updated, deleted, len(assists_map),
            )

        return {"game_id": game_id, "added": added, "updated": updated, "deleted": deleted}

    async def _find_player_id_from_lineup(
        self, game_id: int, first_name: str, last_name: str, team_id: int | None
    ) -> int | None:
        """Find player ID by name from game lineup."""
        if not first_name and not last_name:
            return None

        query = (
            select(Player.id)
            .join(GameLineup, GameLineup.player_id == Player.id)
            .where(
                GameLineup.game_id == game_id,
                Player.first_name == first_name,
                Player.last_name == last_name,
            )
        )
        if team_id:
            query = query.where(GameLineup.team_id == team_id)

        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    def _parse_number(self, value) -> int | None:
        """Parse player number from various formats."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    async def sync_all_game_events(self, season_id: int | None = None) -> dict:
        """
        Sync events for all games in a season.

        Args:
            season_id: Season ID (uses current if None)

        Returns:
            Dict with sync results
        """
        if season_id is None:
            season_id = await get_current_season_id(self.db)

        # Get all games for the season
        result = await self.db.execute(
            select(Game).where(Game.season_id == season_id)
        )
        games = list(result.scalars().all())

        total_added = 0
        total_updated = 0
        total_deleted = 0
        games_synced = 0
        errors = []

        for game in games:
            try:
                result = await self.sync_game_events(game.id)
                if "error" not in result:
                    total_added += result.get("added", 0)
                    total_updated += result.get("updated", 0)
                    total_deleted += result.get("deleted", 0)
                    games_synced += 1
                else:
                    errors.append({"game_id": game.id, "error": result["error"]})
            except Exception as e:
                logger.error(f"Failed to sync events for game {game.id}: {e}")
                errors.append({"game_id": game.id, "error": str(e)})

        return {
            "games_synced": games_synced,
            "total_added": total_added,
            "total_updated": total_updated,
            "total_deleted": total_deleted,
            "errors": errors,
        }

    async def sync_game_metadata_from_live(self, season_id: int | None = None) -> dict:
        """
        Sync stadium and time for games from live SOTA /em/ endpoints.

        Stadium and time are extracted from /em/<game_id>-team-home.json
        using special markers like STADIUM, TIME, VENUE, DATE.
        """
        if season_id is None:
            season_id = await get_current_season_id(self.db)

        # Get games without stadium or time
        result = await self.db.execute(
            select(Game).where(
                Game.season_id == season_id,
                or_(Game.stadium_id.is_(None), Game.time.is_(None)),
            )
        )
        games = list(result.scalars().all())

        updated = 0
        errors = []

        for game in games:
            if not game.sota_id:
                continue
            try:
                # Try to get live lineup data (may contain stadium and time)
                home_data = await self.client.get_live_team_lineup(str(game.sota_id), "home")

                # Extract stadium and time from special markers
                stadium_name = None
                time_str = None

                for item in home_data:
                    number = item.get("number", "")

                    # Check for stadium markers
                    if number in ["STADIUM", "VENUE"]:
                        stadium_name = item.get("first_name") or item.get("full_name")

                    # Check for time markers
                    if number in ["TIME", "DATE"]:
                        time_str = item.get("first_name") or item.get("full_name")

                # Update game if we found new data
                game_updated = False

                if stadium_name and not game.stadium_id:
                    stadium_id = await self._get_or_create_stadium(stadium_name)
                    if stadium_id:
                        game.stadium_id = stadium_id
                        game_updated = True

                if time_str and not game.time:
                    parsed_time = parse_time(time_str)
                    if parsed_time:
                        game.time = parsed_time
                        game_updated = True

                if game_updated:
                    updated += 1

            except Exception as e:
                # Many games won't have live endpoints - this is expected
                if "404" not in str(e):
                    logger.warning(f"Failed to sync metadata for game {game.id}: {e}")
                errors.append({"game_id": str(game.id), "error": str(e)})

        await self.db.commit()

        return {
            "games_checked": len(games),
            "metadata_updated": updated,
            "errors_count": len(errors),
        }

    async def sync_game_formations(self, season_id: int | None = None) -> dict:
        """
        Sync formations for all games using live SOTA endpoints.

        Formations are extracted from /em/<game_id>-team-home.json and
        /em/<game_id>-team-away.json endpoints.
        """
        if season_id is None:
            season_id = await get_current_season_id(self.db)

        # Get games without formations
        result = await self.db.execute(
            select(Game).where(
                Game.season_id == season_id,
                Game.home_formation.is_(None),
            )
        )
        games = list(result.scalars().all())

        updated = 0
        errors = []

        for game in games:
            if not game.sota_id:
                continue
            try:
                # Try to get live lineup data (contains formations)
                home_data = await self.client.get_live_team_lineup(str(game.sota_id), "home")
                away_data = await self.client.get_live_team_lineup(str(game.sota_id), "away")

                # Extract formations
                home_formation = None
                away_formation = None

                for item in home_data:
                    if item.get("number") == "FORMATION":
                        home_formation = item.get("first_name")
                        break

                for item in away_data:
                    if item.get("number") == "FORMATION":
                        away_formation = item.get("first_name")
                        break

                if home_formation or away_formation:
                    game.home_formation = home_formation
                    game.away_formation = away_formation
                    updated += 1

            except Exception as e:
                # Many games won't have live endpoints - this is expected
                if "404" not in str(e):
                    logger.warning(f"Failed to sync formations for game {game.id}: {e}")
                errors.append({"game_id": str(game.id), "error": str(e)})

        await self.db.commit()

        return {
            "games_checked": len(games),
            "formations_updated": updated,
            "errors_count": len(errors),
        }
