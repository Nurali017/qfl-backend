"""
Live synchronization service for real-time match data from SOTA /em/ endpoints.

Handles:
- Pre-game lineup sync (30 min before match)
- Live event sync during match (goals, cards, substitutions)
- Formation extraction from lineup data
"""
import logging
from datetime import datetime, date, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from typing import Any
from uuid import UUID

from sqlalchemy import select, and_, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameEvent, GameEventType, GameLineup, GamePlayerStats, GameTeamStats, GameStatus, LineupType, Team, Player, PlayerTeam
from app.services.sota_client import SotaClient
from app.services.sync.lineup_sync import LineupSyncService
from app.utils.team_name_matcher import TeamNameMatcher


# Mapping SOTA action names to our event types
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

logger = logging.getLogger(__name__)


class LiveSyncService:
    """Service for syncing live match data from SOTA /em/ endpoints."""

    def __init__(self, db: AsyncSession, client: SotaClient):
        self.db = db
        self.client = client

    async def get_games_to_start(self) -> list[Game]:
        """Get games whose scheduled start time is within 1 min ahead or up to 30 min ago."""
        now = datetime.now(ZoneInfo("Asia/Almaty"))
        today = now.date()
        # Allow starting 1 minute before scheduled time
        latest_time = (now + timedelta(minutes=1)).time()
        # Only auto-start games whose time passed within the last 30 minutes
        earliest_time = (now - timedelta(minutes=30)).time()

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.date == today,
                    Game.time.isnot(None),
                    Game.time <= latest_time,
                    Game.time >= earliest_time,
                    Game.status == GameStatus.created,
                    Game.sota_id.isnot(None),
                    Game.sync_disabled == False,
                    Game.is_schedule_tentative == False,
                )
            )
        )
        return list(result.scalars().all())

    async def get_active_games(self) -> list[Game]:
        """Get games that are currently live."""
        result = await self.db.execute(
            select(Game).where(Game.status == GameStatus.live)
        )
        return list(result.scalars().all())

    async def get_games_to_end(self) -> list[Game]:
        """Get live games that should have ended (started > 2 hours ago)."""
        now = datetime.now(ZoneInfo("Asia/Almaty"))
        cutoff = now - timedelta(hours=2, minutes=15)

        # Combine date + time into a timestamp for proper comparison.
        # COALESCE(time, '00:00:00') handles nullable time field.
        game_start = Game.date + func.coalesce(Game.time, dt_time(0, 0, 0))

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.status == GameStatus.live,
                    game_start <= cutoff,
                )
            )
        )
        return list(result.scalars().all())

    def _extract_formation(self, lineup_data: list[dict]) -> str | None:
        """Extract formation string from lineup data."""
        for item in lineup_data:
            if item.get("number") == "FORMATION":
                return item.get("first_name")  # e.g., "4-2-3-1"
        return None

    def _extract_players(self, lineup_data: list[dict]) -> tuple[list[dict], list[dict]]:
        """Extract starters and substitutes from lineup data."""
        starters = []
        substitutes = []
        current_section = None

        for item in lineup_data:
            number = item.get("number")

            if number == "ОСНОВНЫЕ":
                current_section = "starters"
                continue
            elif number == "ЗАПАСНЫЕ":
                current_section = "substitutes"
                continue
            elif number in ("TEAM", "FORMATION", "COACH", "MAIN"):
                continue

            if not item.get("id"):
                continue

            if current_section == "starters":
                starters.append(item)
            elif current_section == "substitutes":
                substitutes.append(item)

        return starters, substitutes

    async def _get_team_id_by_name(self, team_name: str) -> int | None:
        """Get team ID by name."""
        result = await self.db.execute(
            select(Team.id).where(Team.name == team_name)
        )
        row = result.scalar_one_or_none()
        return row

    async def sync_pregame_lineup(self, game_id: int) -> dict:
        """Sync pre-game lineup data for a match."""
        sync_result = await LineupSyncService(self.db, self.client).sync_pre_game_lineup(game_id)
        game = await self.db.get(Game, game_id)
        if not game:
            return {"error": f"Game {game_id} not found"}
        return {
            "game_id": game_id,
            "home_formation": game.home_formation,
            "away_formation": game.away_formation,
            "lineup_count": int(sync_result.get("lineups", 0)),
            "positions_updated": int(sync_result.get("positions_updated", 0)),
            "kit_colors_updated": int(sync_result.get("kit_colors_updated", 0)),
        }

    async def sync_live_lineup(self, game_id: int) -> dict:
        """
        Sync lineup from SOTA live feed (team-home.json / team-away.json).

        Unlike sync_pregame_lineup which uses /pre_game_lineup/ (flat list),
        this uses the /em/ live feed which has explicit ОСНОВНЫЕ/ЗАПАСНЫЕ markers
        for accurate starter/substitute classification.
        """
        game = await self.db.get(Game, game_id)
        if not game or not game.sota_id:
            return {"error": f"Game {game_id} not found or no sota_id"}

        sota_uuid = str(game.sota_id)
        total_lineup = 0

        for side, team_id in (("home", game.home_team_id), ("away", game.away_team_id)):
            if not team_id:
                continue

            try:
                live_data = await self.client.get_live_team_lineup(sota_uuid, side)
            except Exception as exc:
                logger.warning("Failed to fetch live lineup for game %s (%s): %s", game_id, side, exc)
                continue

            if not isinstance(live_data, list) or not live_data:
                continue

            # Extract formation
            formation = self._extract_formation(live_data)
            if formation:
                if side == "home":
                    game.home_formation = formation
                else:
                    game.away_formation = formation

            # Extract starters and substitutes using ОСНОВНЫЕ/ЗАПАСНЫЕ markers
            starters, substitutes = self._extract_players(live_data)

            for player_data in starters:
                total_lineup += await self._save_player_lineup(
                    game_id, team_id, player_data, LineupType.starter,
                    season_id=game.season_id,
                )
            for player_data in substitutes:
                total_lineup += await self._save_player_lineup(
                    game_id, team_id, player_data, LineupType.substitute,
                    season_id=game.season_id,
                )

        game.lineup_source = "sota_live"
        await self.db.commit()

        return {
            "game_id": game_id,
            "home_formation": game.home_formation,
            "away_formation": game.away_formation,
            "lineup_count": total_lineup,
        }

    async def _save_player_lineup(
        self,
        game_id: int,
        team_id: int,
        player_data: dict,
        lineup_type: LineupType,
        season_id: int | None = None,
    ) -> int:
        """Save a single player lineup entry."""
        player_internal_id = await self._get_or_create_player_by_sota(
            player_data.get("id"),
            player_data.get("first_name"),
            player_data.get("last_name"),
            team_id=team_id,
            season_id=season_id,
        )
        if player_internal_id is None:
            return 0

        shirt_number = player_data.get("number")
        if isinstance(shirt_number, str):
            try:
                shirt_number = int(shirt_number)
            except ValueError:
                shirt_number = None

        is_captain = player_data.get("capitan", False)
        if is_captain == "":
            is_captain = False

        amplua = player_data.get("amplua") or None
        field_position = player_data.get("position") or None

        stmt = insert(GameLineup).values(
            game_id=game_id,
            team_id=team_id,
            player_id=player_internal_id,
            lineup_type=lineup_type,
            shirt_number=shirt_number,
            is_captain=bool(is_captain),
            amplua=amplua,
            field_position=field_position,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_game_lineup_player",
            set_={
                "lineup_type": lineup_type,
                "shirt_number": shirt_number,
                "is_captain": bool(is_captain),
                "amplua": func.coalesce(stmt.excluded.amplua, GameLineup.amplua),
                "field_position": func.coalesce(stmt.excluded.field_position, GameLineup.field_position),
            }
        )
        await self.db.execute(stmt)
        return 1

    async def _get_or_create_player_by_sota(
        self,
        sota_id_raw: str | None,
        first_name: str | None,
        last_name: str | None,
        team_id: int | None = None,
        season_id: int | None = None,
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

        # Fallback: поиск по имени в составе команды/сезона.
        # Если игрок создан вручную без sota_id — привязываем его sota_id вместо создания дубля.
        if team_id and season_id and (first_name or last_name):
            result = await self.db.execute(
                select(Player)
                .join(PlayerTeam, PlayerTeam.player_id == Player.id)
                .where(
                    PlayerTeam.team_id == team_id,
                    PlayerTeam.season_id == season_id,
                    Player.last_name == (last_name or ""),
                    Player.first_name == (first_name or ""),
                    Player.sota_id.is_(None),
                )
            )
            existing = result.scalar_one_or_none()
            if existing is not None:
                existing.sota_id = sota_id
                await self.db.flush()
                logger.info("Linked sota_id to existing player %s (id=%s)", f"{first_name} {last_name}", existing.id)
                return existing.id

        player = Player(
            sota_id=sota_id,
            first_name=first_name or "",
            last_name=last_name or "",
        )
        self.db.add(player)
        await self.db.flush()
        return player.id

    async def sync_live_stats(self, game_id: int) -> dict:
        """
        Sync live match statistics from /em/{sota_id}-stat.json.

        SOTA returns array of {metric, home, away} objects. We map them
        to game_team_stats rows and update score on the game itself.
        """
        game = await self.db.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == game_id)
        )
        game = game.scalar_one_or_none()
        if not game or not game.sota_id:
            return {"error": f"Game {game_id} not found or no sota_id"}

        sota_uuid = str(game.sota_id)
        try:
            stats_data = await self.client.get_live_match_stats(sota_uuid)
        except Exception as exc:
            logger.warning("Failed to fetch live stats for game %s: %s", game_id, exc)
            return {"error": str(exc)}

        if not isinstance(stats_data, list):
            return {"error": "Unexpected stats format"}

        # Build lookup: metric -> {home, away}
        # Capture per-half breakdowns (_1, _2) into by_half dict
        # Skip _3, _4, _5 (extra time, penalties)
        import re
        metrics = {}
        by_half = {"1": {}, "2": {}}
        for item in stats_data:
            metric = item.get("metric", "")
            if not metric or metric == "name":
                continue
            match = re.match(r"^(.+)_([12])$", metric)
            if match:
                base, half = match.groups()
                by_half[half][base] = {"home": item.get("home"), "away": item.get("away")}
                continue
            if re.match(r"^.+_\d+$", metric):
                continue  # still skip _3, _4, _5
            metrics[metric] = {"home": item.get("home"), "away": item.get("away")}

        def _parse_int(val: any) -> int | None:
            if val is None or val == "":
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        def _parse_possession(val: any) -> float | None:
            if val is None or val == "":
                return None
            if isinstance(val, str):
                val = val.replace("%", "").strip()
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Update score — use "scores" (includes own goals), not "goals"
        scores = metrics.get("scores", {})
        home_score = _parse_int(scores.get("home"))
        away_score = _parse_int(scores.get("away"))
        if home_score is not None:
            game.home_score = home_score
        if away_score is not None:
            game.away_score = away_score

        # Upsert stats for each team
        for side, team_id in (("home", game.home_team_id), ("away", game.away_team_id)):
            if not team_id:
                continue

            values = {
                "game_id": game_id,
                "team_id": team_id,
                "shots": _parse_int(metrics.get("shots", {}).get(side)),
                "shots_on_goal": _parse_int(metrics.get("shots_on_target", {}).get(side)),
                "shots_off_goal": _parse_int(metrics.get("shots_missed", {}).get(side)),
                "corners": _parse_int(metrics.get("corners", {}).get(side)),
                "fouls": _parse_int(metrics.get("fouls", {}).get(side)),
                "yellow_cards": _parse_int(metrics.get("yc", {}).get(side)),
                "red_cards": _parse_int(metrics.get("rc", {}).get(side)),
                "offsides": _parse_int(metrics.get("offsides", {}).get(side)),
                "possession": _parse_possession(metrics.get("possessions", {}).get(side)),
                "possession_percent": _parse_possession(metrics.get("possessions", {}).get(side)),
                "shots_on_bar": _parse_int(
                    metrics.get("shots_on_bar", {}).get(side)
                    or metrics.get("shot_on_bar", {}).get(side)
                ),
                "shots_blocked": _parse_int(
                    metrics.get("shots_blocked", {}).get(side)
                    or metrics.get("shot_blocked", {}).get(side)
                ),
                "penalties": _parse_int(
                    metrics.get("penalty", {}).get(side)
                    or metrics.get("penalties", {}).get(side)
                ),
                "saves": _parse_int(
                    metrics.get("saves", {}).get(side)
                    or metrics.get("save", {}).get(side)
                    or metrics.get("save_shot", {}).get(side)
                ),
            }

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
                values["extra_stats"] = {"by_half": side_by_half}

            stmt = insert(GameTeamStats).values(**values)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_game_team_stats",
                set_={k: v for k, v in values.items() if k not in ("game_id", "team_id")},
            )
            await self.db.execute(stmt)

        await self.db.commit()

        return {
            "game_id": game_id,
            "home_score": game.home_score,
            "away_score": game.away_score,
            "metrics_synced": len(metrics),
        }

    @staticmethod
    def _amplua_to_position(amplua: str | None) -> str:
        """Convert SOTA amplua code to position string for GamePlayerStats."""
        if not amplua:
            return "MID"
        return {"Gk": "GK", "D": "DEF", "DM": "DEF", "M": "MID", "AM": "MID", "F": "FWD"}.get(amplua, "MID")

    async def sync_live_player_stats(self, game_id: int) -> dict:
        """
        Sync per-player stats from /em/{sota_id}-players-{side}.json.

        Upserts into game_player_stats so the "Player Stats" tab works during live matches.
        Goals/assists are NOT written here (single source of truth = game_events).
        """
        game = await self.db.execute(
            select(Game).where(Game.id == game_id)
        )
        game = game.scalar_one_or_none()
        if not game or not game.sota_id:
            return {"error": f"Game {game_id} not found or no sota_id"}

        sota_uuid = str(game.sota_id)

        # Build amplua lookup from lineup: player_id -> amplua
        lineup_result = await self.db.execute(
            select(GameLineup.player_id, GameLineup.amplua)
            .where(GameLineup.game_id == game_id)
        )
        amplua_map = {row.player_id: row.amplua for row in lineup_result.all()}

        total_upserted = 0

        for side, team_id in (("home", game.home_team_id), ("away", game.away_team_id)):
            if not team_id:
                continue

            try:
                players_data = await self.client.get_live_match_player_stats(sota_uuid, side)
            except Exception as exc:
                logger.warning("Failed to fetch live player stats for game %s (%s): %s", game_id, side, exc)
                continue

            if not isinstance(players_data, list):
                continue

            for ep in players_data:
                sota_id_raw = ep.get("id")
                if not sota_id_raw:
                    continue

                player_id = await self._get_or_create_player_by_sota(
                    sota_id_raw,
                    ep.get("first_name"),
                    ep.get("last_name"),
                    team_id=team_id,
                    season_id=game.season_id,
                )
                if player_id is None:
                    continue

                # Direct-mapped fields
                shots = self._parse_stat(ep.get("shots"))
                shots_on_goal = self._parse_stat(ep.get("shots_on_target"))
                shots_off_goal = self._parse_stat(ep.get("shots_missed"))
                yellow_cards = self._parse_stat(ep.get("yc"))
                red_cards = self._parse_stat(ep.get("rc"))
                offside = self._parse_stat(ep.get("offsides"))

                # Fields without DB column → extra_stats
                extra = {}
                for key in ("saves", "shots_on_bar", "shots_blocked", "penalties"):
                    val = self._parse_stat(ep.get(key))
                    if val is not None:
                        extra[key] = val

                # Per-half breakdown → extra_stats.by_half
                by_half = {}
                for suffix in ("1", "2", "3", "4", "5"):
                    half_data = {}
                    for base in ("goals", "assists", "shots", "shots_on_target", "shots_missed",
                                 "shots_on_bar", "shots_blocked", "saves", "yc", "rc",
                                 "offsides", "penalties"):
                        val = self._parse_stat(ep.get(f"{base}_{suffix}"))
                        if val is not None:
                            half_data[base] = val
                    if half_data:
                        by_half[suffix] = half_data
                if by_half:
                    extra["by_half"] = by_half

                position = self._amplua_to_position(amplua_map.get(player_id))

                values = {
                    "game_id": game_id,
                    "player_id": player_id,
                    "team_id": team_id,
                    "position": position,
                    "shots": shots or 0,
                    "shots_on_goal": shots_on_goal or 0,
                    "shots_off_goal": shots_off_goal or 0,
                    "yellow_cards": yellow_cards or 0,
                    "red_cards": red_cards or 0,
                    "offside": offside or 0,
                    "extra_stats": extra if extra else None,
                }

                stmt = insert(GamePlayerStats).values(**values)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_game_player_stats",
                    set_={k: v for k, v in values.items() if k not in ("game_id", "player_id")},
                )
                await self.db.execute(stmt)
                total_upserted += 1

        await self.db.commit()
        return {"game_id": game_id, "players_upserted": total_upserted}

    @staticmethod
    def _parse_stat(val) -> int | None:
        """Parse an integer stat value from SOTA data."""
        if val is None or val == "":
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _event_signature(event_type_value: str, half: int, minute: int,
                         player_id: int | None, player_name: str | None) -> tuple:
        """Build a signature tuple for event matching."""
        name = player_name.strip().lower() if player_name else ""
        return (half, minute, event_type_value, player_id, name)

    @staticmethod
    def _signatures_match(sig_a: tuple, sig_b: tuple) -> bool:
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

    async def sync_live_events(self, game_id: int) -> dict:
        """
        Sync live events for a match with full reconciliation.

        Adds new events, updates changed ones, deletes SOTA events
        that no longer exist. Manual events are protected.

        Returns dict with added/updated/deleted counts.
        """
        # Load all existing events
        result = await self.db.execute(
            select(GameEvent).where(GameEvent.game_id == game_id)
        )
        existing_events = list(result.scalars().all())

        # Separate SOTA events from manual events
        sota_events = [e for e in existing_events if e.source == "sota"]

        # Build list-based signature map for SOTA events
        sota_by_sig: dict[tuple, list[GameEvent]] = {}
        for e in sota_events:
            sig = self._event_signature(e.event_type.value, e.half, e.minute, e.player_id, e.player_name)
            sota_by_sig.setdefault(sig, []).append(e)

        # Get game for team IDs with eager loading of teams
        result = await self.db.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == game_id)
        )
        game = result.scalar_one_or_none()
        if not game:
            return {"added": 0, "updated": 0, "deleted": 0}

        sota_uuid = str(game.sota_id) if game.sota_id else None
        if not sota_uuid:
            logger.warning("Game %s has no sota_id, cannot fetch live events", game_id)
            return {"added": 0, "updated": 0, "deleted": 0}
        events_data = await self.client.get_live_match_events(sota_uuid)

        # Safety check: if SOTA returns empty but we have SOTA events, skip deletion
        skip_deletes = False
        if not events_data and sota_events:
            logger.warning(
                "Game %s: SOTA returned empty events but %d sota events exist in DB — skipping deletes",
                game_id, len(sota_events),
            )
            skip_deletes = True

        matcher = TeamNameMatcher.from_game(game)
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
            team_id_from_name = self._match_team_id(game, team_name, matcher=matcher)
            team_id = team_id_from_name

            player_id = await self._find_player_id(first_name1, last_name1, game_id, team_id)
            team_id_from_player = None
            if team_id is None and player_id:
                team_id_from_player = await self._infer_team_id_from_lineup(game_id, player_id)
                team_id = team_id_from_player

            # Collect assists into map
            if event_type == GameEventType.assist:
                key = (half, minute, team_id)
                assists_map[key] = {
                    "player_id": player_id,
                    "player_name": player_name,
                }
                continue

            player2_id = None
            player2_number = None
            player2_name_str = ""
            player2_team_name = ""
            team2_id = None

            if event_type == GameEventType.substitution:
                first_name2 = event_data.get("first_name2", "")
                last_name2 = event_data.get("last_name2", "")
                team2_name = event_data.get("team2", "")
                team2_id = self._match_team_id(game, team2_name, matcher=matcher)
                player2_id = await self._find_player_id(first_name2, last_name2, game_id, team2_id)
                player2_number = self._parse_number(event_data.get("number2"))
                player2_name_str = f"{first_name2} {last_name2}".strip()
                player2_team_name = team2_name

            if team_id is None:
                player2_candidate_team_id = None
                if event_type == GameEventType.substitution:
                    player2_candidate_team_id = await self._infer_team_id_from_lineup(game_id, player2_id)
                team_id = self._resolve_unambiguous_team_id(
                    team_id_from_player,
                    team2_id,
                    player2_candidate_team_id,
                )

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
                "player2_number": player2_number,
                "player2_name": player2_name_str,
                "player2_team_name": player2_team_name,
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
                matched_db_events.add(matched_event.id)
                changed = False
                for field, value in new_fields.items():
                    old_value = getattr(matched_event, field)
                    if old_value != value:
                        changed = True
                        setattr(matched_event, field, value)
                if changed:
                    updated += 1
                if event_type in (GameEventType.goal, GameEventType.penalty):
                    all_goal_events.append(matched_event)
            else:
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

        # Link assists to all goal events
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
                "Game %s live events: added=%d updated=%d deleted=%d assists=%d",
                game_id, added, updated, deleted, len(assists_map),
            )
        else:
            logger.debug("Game %s live events: no changes", game_id)

        return {"added": added, "updated": updated, "deleted": deleted}

    def _parse_number(self, value: Any) -> int | None:
        """Parse player number from various formats."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _resolve_unambiguous_team_id(self, *team_ids: int | None) -> int | None:
        candidates = {team_id for team_id in team_ids if team_id is not None}
        if len(candidates) == 1:
            return next(iter(candidates))
        return None

    def _match_team_id(
        self,
        game: Game,
        team_name: str | None,
        matcher: TeamNameMatcher | None = None,
    ) -> int | None:
        """Match team name to team ID using all language variants and safe aliases."""
        team_matcher = matcher or TeamNameMatcher.from_game(game)
        return team_matcher.match(team_name)

    async def _infer_team_id_from_lineup(self, game_id: int, player_id: int | None) -> int | None:
        """Infer event team_id by player lineup entry in this game."""
        if not player_id:
            return None

        result = await self.db.execute(
            select(GameLineup.team_id).where(
                GameLineup.game_id == game_id,
                GameLineup.player_id == player_id,
            )
        )
        team_ids = {team_id for team_id in result.scalars().all() if team_id is not None}
        if len(team_ids) == 1:
            return next(iter(team_ids))
        return None

    async def _find_player_id(
        self, first_name: str, last_name: str, game_id: int, team_id: int | None = None
    ) -> int | None:
        """Find player ID by name from game lineup."""
        if not first_name and not last_name:
            return None

        name_match = or_(
            and_(Player.first_name == first_name, Player.last_name == last_name),
            and_(Player.first_name_kz == first_name, Player.last_name_kz == last_name),
            and_(Player.first_name_en == first_name, Player.last_name_en == last_name),
        )

        query = (
            select(Player.id)
            .join(GameLineup, GameLineup.player_id == Player.id)
            .where(GameLineup.game_id == game_id, name_match)
        )

        if team_id:
            query = query.where(GameLineup.team_id == team_id)

        result = await self.db.execute(query)
        player_ids = {player_id for player_id in result.scalars().all() if player_id is not None}
        if len(player_ids) == 1:
            return next(iter(player_ids))
        return None

    async def start_live_tracking(self, game_id: int) -> dict:
        """Start live tracking for a game."""
        from app.utils.live_flag import set_live_flag

        game = await self.db.get(Game, game_id)
        if not game:
            return {"error": f"Game {game_id} not found"}

        if not game.has_lineup:
            await self.sync_pregame_lineup(game_id)

        game.status = GameStatus.live
        game.half1_started_at = datetime.utcnow()
        await self.db.commit()

        await set_live_flag()

        sync_result = await self.sync_live_events(game_id)

        return {
            "game_id": game_id,
            "is_live": True,
            "new_events_count": sync_result.get("added", 0),
        }

    async def stop_live_tracking(self, game_id: int) -> dict:
        """Stop live tracking for a game."""
        from app.utils.live_flag import clear_live_flag

        game = await self.db.get(Game, game_id)
        if not game:
            return {"error": f"Game {game_id} not found"}

        game.status = GameStatus.finished
        await self.db.commit()

        # Clear flag if no other live games remain
        remaining = await self.db.execute(
            select(func.count()).select_from(Game).where(Game.status == GameStatus.live)
        )
        if remaining.scalar() == 0:
            await clear_live_flag()

        return {
            "game_id": game_id,
            "is_live": False,
        }

    async def get_game_events(self, game_id: int) -> list[GameEvent]:
        """Get all events for a game."""
        result = await self.db.execute(
            select(GameEvent)
            .where(GameEvent.game_id == game_id)
            .order_by(GameEvent.half, GameEvent.minute)
        )
        return list(result.scalars().all())
