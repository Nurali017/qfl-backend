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

from sqlalchemy import delete, select, and_, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameEvent, GameEventType, GameLineup, GamePlayerStats, GameTeamStats, GameStatus, LineupType, Team, Player, PlayerTeam
from app.services.sota_client import SotaClient
from app.services.sync.lineup_sync import LineupSyncService
from app.services.telegram import send_telegram_message
from app.utils.game_event_assists import is_assist_supported_event_type, sync_event_assist
from app.utils.live_flag import get_redis
from app.utils.team_name_matcher import TeamNameMatcher
from app.utils.timestamps import combine_almaty_local_to_utc, ensure_utc, utcnow


# Mapping SOTA action names to our event types
ACTION_TYPE_MAP = {
    "ГОЛ": GameEventType.goal,
    "АВТОГОЛ": GameEventType.own_goal,
    "ПЕНАЛЬТИ": GameEventType.penalty,
    "НЕЗАБИТЫЙ ПЕНАЛЬТИ": GameEventType.missed_penalty,
    "НЕ ЗАБИТЫЙ ПЕНАЛЬТИ": GameEventType.missed_penalty,
    "ГОЛЕВОЙ ПАС": GameEventType.assist,
    "ЖК": GameEventType.yellow_card,
    "2ЖК": GameEventType.second_yellow,
    "КК": GameEventType.red_card,
    "ЗАМЕНА": GameEventType.substitution,
}

logger = logging.getLogger(__name__)


def _normalize_name(value: str | None) -> str:
    return (value or "").strip().casefold()


def _last_names_match(sota_last_name: str | None, player: Player) -> bool:
    """True if SOTA last_name matches any of the player's last_name variants."""
    target = _normalize_name(sota_last_name)
    if not target:
        return False
    candidates = (player.last_name, player.last_name_kz, player.last_name_en)
    return any(_normalize_name(c) == target for c in candidates)


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

    async def get_games_for_pregame_lineup(self) -> list[Game]:
        """Get games starting within 40 minutes that need positions/formations from SOTA."""
        now = datetime.now(ZoneInfo("Asia/Almaty"))
        today = now.date()
        current_time = now.time()
        latest_time = (now + timedelta(minutes=40)).time()

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.date == today,
                    Game.time.isnot(None),
                    Game.time >= current_time,
                    Game.time <= latest_time,
                    Game.status == GameStatus.created,
                    Game.sota_id.isnot(None),
                    Game.sync_disabled == False,
                    Game.is_schedule_tentative == False,
                    # Skip games where SOTA already synced positions
                    Game.lineup_source != "sota_live",
                )
            )
        )
        return list(result.scalars().all())

    async def get_active_games(self) -> list[Game]:
        """Get games that are currently live (excluding sync_disabled)."""
        result = await self.db.execute(
            select(Game).where(
                Game.status == GameStatus.live,
                Game.sync_disabled == False,
            )
        )
        return list(result.scalars().all())

    async def get_games_to_end(self) -> list[Game]:
        """Get live games that should have ended (started > 2h6m ago).

        Primary source: half1_started_at. Fallback: date + time.
        """
        cutoff_utc = utcnow() - timedelta(hours=2, minutes=6)
        result = await self.db.execute(
            select(Game).where(Game.status == GameStatus.live)
        )
        games = list(result.scalars().all())

        due_games: list[Game] = []
        for game in games:
            started_at = ensure_utc(game.half1_started_at)
            if started_at is None:
                started_at = combine_almaty_local_to_utc(
                    game.date,
                    game.time or dt_time(0, 0, 0),
                )
            if started_at <= cutoff_utc:
                due_games.append(game)

        return due_games

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

    async def sync_pregame_lineup(self, game_id: int, *, sota_only: bool = False) -> dict:
        """Sync pre-game lineup data for a match."""
        sync_result = await LineupSyncService(self.db, self.client).sync_pre_game_lineup(
            game_id, sota_only=sota_only,
        )
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

        If lineup_source is "fcms", only updates formations/positions on existing
        records — does not add or delete lineup entries.
        """
        game = await self.db.get(Game, game_id)
        if not game or not game.sota_id:
            return {"error": f"Game {game_id} not found or no sota_id"}

        fcms_protected = game.lineup_source == "fcms"
        sota_uuid = str(game.sota_id)
        total_lineup = 0
        synced_player_ids: dict[int, set[int]] = {}

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

            # Extract formation — always update
            formation = self._extract_formation(live_data)
            if formation:
                if side == "home":
                    game.home_formation = formation
                else:
                    game.away_formation = formation

            # Extract starters and substitutes using ОСНОВНЫЕ/ЗАПАСНЫЕ markers
            starters, substitutes = self._extract_players(live_data)

            if fcms_protected:
                # FCMS lineup is authoritative — only enrich existing records
                for player_data in starters + substitutes:
                    await self._enrich_fcms_lineup_from_sota(
                        game_id, team_id, player_data, game.season_id,
                    )
            else:
                team_players: set[int] = set()
                for player_data in starters:
                    pid = await self._save_player_lineup(
                        game_id, team_id, player_data, LineupType.starter,
                        season_id=game.season_id,
                    )
                    if pid:
                        team_players.add(pid)
                        total_lineup += 1
                for player_data in substitutes:
                    pid = await self._save_player_lineup(
                        game_id, team_id, player_data, LineupType.substitute,
                        season_id=game.season_id,
                    )
                    if pid:
                        team_players.add(pid)
                        total_lineup += 1
                synced_player_ids[team_id] = team_players

        if not fcms_protected:
            # Reconciliation: remove players no longer in SOTA lineup
            for team_id, player_ids in synced_player_ids.items():
                if player_ids:  # only if SOTA returned data for this team
                    await self.db.execute(
                        delete(GameLineup).where(
                            GameLineup.game_id == game_id,
                            GameLineup.team_id == team_id,
                            GameLineup.player_id.notin_(player_ids),
                        )
                    )
            game.lineup_source = "sota_live"
        await self.db.commit()

        return {
            "game_id": game_id,
            "home_formation": game.home_formation,
            "away_formation": game.away_formation,
            "lineup_count": total_lineup,
        }

    async def _enrich_fcms_lineup_from_sota(
        self,
        game_id: int,
        team_id: int,
        player_data: dict,
        season_id: int | None,
    ) -> None:
        """Update field_position/amplua on existing FCMS lineup record and backfill sota_id.

        Matches by team + shirt_number (primary) or team + name (fallback).
        Does NOT insert or delete lineup records.
        """
        shirt_number = player_data.get("number")
        if isinstance(shirt_number, str):
            try:
                shirt_number = int(shirt_number)
            except ValueError:
                shirt_number = None

        amplua = player_data.get("amplua") or None
        field_position = player_data.get("position") or None
        sota_id_raw = player_data.get("id")

        if not shirt_number:
            return

        # Find existing lineup record by shirt_number + team
        result = await self.db.execute(
            select(GameLineup)
            .where(
                GameLineup.game_id == game_id,
                GameLineup.team_id == team_id,
                GameLineup.shirt_number == shirt_number,
            )
            .limit(1)
        )
        lineup_row = result.scalar_one_or_none()
        if not lineup_row:
            return

        # Update field_position / amplua if SOTA provides them
        if amplua and not lineup_row.amplua:
            lineup_row.amplua = amplua
        if field_position and not lineup_row.field_position:
            lineup_row.field_position = field_position

        # Backfill sota_id on the Player record (only if not already set on another player)
        if sota_id_raw and lineup_row.player_id:
            try:
                sota_id = UUID(str(sota_id_raw))
            except (ValueError, TypeError):
                return
            # Check sota_id isn't already used by another player
            existing = await self.db.execute(
                select(Player.id).where(Player.sota_id == sota_id)
            )
            if existing.scalar_one_or_none() is not None:
                return
            player = await self.db.get(Player, lineup_row.player_id)
            if player and not player.sota_id:
                player.sota_id = sota_id
                logger.info(
                    "Backfilled sota_id for player %s %s (id=%d) from FCMS lineup",
                    player.first_name, player.last_name, player.id,
                )

    async def _save_player_lineup(
        self,
        game_id: int,
        team_id: int,
        player_data: dict,
        lineup_type: LineupType,
        season_id: int | None = None,
    ) -> int | None:
        """Save a single player lineup entry. Returns player_id or None."""
        shirt_number = player_data.get("number")
        if isinstance(shirt_number, str):
            try:
                shirt_number = int(shirt_number)
            except ValueError:
                shirt_number = None

        player_internal_id = await self._get_or_create_player_by_sota(
            player_data.get("id"),
            player_data.get("first_name"),
            player_data.get("last_name"),
            team_id=team_id,
            season_id=season_id,
            shirt_number=shirt_number,
        )
        if player_internal_id is None:
            return None

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
        return player_internal_id

    async def _get_or_create_player_by_sota(
        self,
        sota_id_raw: str | None,
        first_name: str | None,
        last_name: str | None,
        team_id: int | None = None,
        season_id: int | None = None,
        shirt_number: int | None = None,
    ) -> int | None:
        if not sota_id_raw:
            return None

        try:
            sota_id = UUID(str(sota_id_raw))
        except (ValueError, TypeError):
            return None

        # Step 1: поиск по sota_id
        result = await self.db.execute(select(Player).where(Player.sota_id == sota_id))
        player = result.scalar_one_or_none()

        if player is not None:
            return player.id

        # Step 2: поиск по имени в составе команды/сезона.
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
                logger.info("Linked sota_id to existing player %s (id=%s) by name", f"{first_name} {last_name}", existing.id)
                return existing.id

        # Step 3: поиск по номеру футболки + команда + сезон.
        # Номер уникален в рамках команды/сезона — самый надёжный фоллбэк
        # когда имена не совпадают (разная транслитерация FCMS vs SOTA).
        # Исключаем is_hidden контракты: после смены игрока под тем же номером
        # старый контракт может быть is_active=True, is_hidden=True — не линковать на него.
        #
        # Разрешаем перезапись существующего sota_id, если фамилия из SOTA
        # совпадает с last_name/last_name_kz/last_name_en у игрока — SOTA
        # периодически меняет UUID игроков (дубликаты на их стороне).
        if shirt_number and team_id and season_id:
            result = await self.db.execute(
                select(Player)
                .join(PlayerTeam, PlayerTeam.player_id == Player.id)
                .where(
                    PlayerTeam.team_id == team_id,
                    PlayerTeam.season_id == season_id,
                    PlayerTeam.number == shirt_number,
                    PlayerTeam.is_active == True,  # noqa: E712
                    PlayerTeam.is_hidden == False,  # noqa: E712
                )
            )
            existing = result.scalars().first()
            if existing is not None:
                if existing.sota_id is None:
                    existing.sota_id = sota_id
                    await self.db.flush()
                    logger.info(
                        "Linked sota_id to existing player %s (id=%s) by shirt number %s",
                        f"{existing.first_name} {existing.last_name}", existing.id, shirt_number,
                    )
                    return existing.id
                if existing.sota_id != sota_id and _last_names_match(last_name, existing):
                    old_sota_id = existing.sota_id
                    existing.sota_id = sota_id
                    await self.db.flush()
                    logger.warning(
                        "Rewrote sota_id for player %s (id=%s): %s → %s "
                        "(matched by team=%s season=%s number=%s + last_name)",
                        f"{existing.first_name} {existing.last_name}", existing.id,
                        old_sota_id, sota_id, team_id, season_id, shirt_number,
                    )
                    return existing.id

        # Unknown player — notify via Telegram (deduplicated by Redis)
        try:
            redis_client = await get_redis()
            redis_key = "qfl:notified_unknown_players"
            member = str(sota_id)
            added = await redis_client.sadd(redis_key, member)
            if added:
                await redis_client.expire(redis_key, 7200)  # 2 hours
                await send_telegram_message(
                    f"⚠️ Неизвестный игрок в SOTA lineup\n\n"
                    f"👤 {first_name} {last_name}\n"
                    f"🆔 SOTA ID: {sota_id}\n"
                    f"🏟 Team ID: {team_id}\n"
                    f"📅 Season ID: {season_id}\n"
                    f"👕 Номер: {shirt_number}\n\n"
                    f"Игрок не найден в БД. Добавьте вручную."
                )
        except Exception:
            logger.exception("Failed to send unknown player notification")
        logger.warning("Unknown player in SOTA: %s %s (sota_id=%s, number=%s)", first_name, last_name, sota_id, shirt_number)
        return None

    async def sync_live_time(self, game_id: int) -> dict:
        """Sync live match minute and half from SOTA /em/{sota_id}-time.json."""
        game = await self.db.get(Game, game_id)
        if not game or not game.sota_id:
            return {"error": f"Game {game_id} not found or no sota_id"}

        sota_uuid = str(game.sota_id)
        try:
            time_data = await self.client.get_live_match_time(sota_uuid)
        except Exception as exc:
            logger.warning("Failed to fetch live time for game %s: %s", game_id, exc)
            return {"error": str(exc)}

        if not time_data:
            return {
                "game_id": game_id,
                "live_minute": None,
                "live_half": None,
                "live_phase": game.live_phase,
            }

        status_raw = time_data.get("status")
        status_value = str(status_raw).strip().lower() if status_raw is not None else None

        if status_value == "finished":
            from app.services.game_lifecycle import GameLifecycleService

            result = await GameLifecycleService(self.db).finish_live(game_id)
            refreshed = await self.db.get(Game, game_id)
            return {
                "game_id": game_id,
                "live_minute": refreshed.live_minute if refreshed else None,
                "live_half": refreshed.live_half if refreshed else None,
                "live_phase": refreshed.live_phase if refreshed else None,
                "lifecycle_result": result,
            }

        half = time_data.get("half")
        actual_time = time_data.get("actual_time")

        if actual_time is not None:
            try:
                minute = int(actual_time) // 60000
            except (ValueError, TypeError):
                minute = None
        else:
            minute = None

        # SOTA resets timer to 0 each half — offset for 2nd half
        if minute is not None and half is not None:
            try:
                if int(half) == 2:
                    minute += 45
            except (ValueError, TypeError):
                pass

        if minute is not None:
            game.live_minute = minute
        if half is not None:
            try:
                game.live_half = int(half)
            except (ValueError, TypeError):
                pass

        if status_value == "halftime":
            game.live_phase = "halftime"
        elif status_value == "in_progress":
            game.live_phase = "in_progress"
        elif status_value:
            logger.warning("Unknown SOTA time status for game %s: %s", game_id, status_raw)
        elif game.live_phase == "halftime" and game.live_half == 2:
            # If the feed resumes with half=2 but omits an explicit status,
            # don't keep the match stuck in halftime forever.
            game.live_phase = "in_progress"

        await self.db.commit()

        return {
            "game_id": game_id,
            "live_minute": game.live_minute,
            "live_half": game.live_half,
            "live_phase": game.live_phase,
        }

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
        # Capture per-half breakdowns (_1..._5) into by_half dict.
        # 1=H1, 2=H2, 3=ET1, 4=ET2, 5=Shootout.
        import re
        metrics = {}
        by_half: dict[str, dict] = {"1": {}, "2": {}, "3": {}, "4": {}, "5": {}}
        for item in stats_data:
            metric = item.get("metric", "")
            if not metric or metric == "name":
                continue
            match = re.match(r"^(.+)_([1-5])$", metric)
            if match:
                base, half = match.groups()
                by_half[half][base] = {"home": item.get("home"), "away": item.get("away")}
                continue
            if re.match(r"^.+_\d+$", metric):
                continue  # any unexpected suffix
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
            extra = {}
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
                extra["by_half"] = side_by_half

            # Store new aggregate metrics from SOTA stat.json
            for extra_key in ("inside_pbox", "outside_pbox", "missed_penalty"):
                val = _parse_int(metrics.get(extra_key, {}).get(side))
                if val is not None:
                    extra[extra_key] = val

            if extra:
                values["extra_stats"] = extra

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

                ep_number = ep.get("number")
                if isinstance(ep_number, str):
                    try:
                        ep_number = int(ep_number)
                    except ValueError:
                        ep_number = None

                player_id = await self._get_or_create_player_by_sota(
                    sota_id_raw,
                    ep.get("first_name"),
                    ep.get("last_name"),
                    team_id=team_id,
                    season_id=game.season_id,
                    shirt_number=ep_number,
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

            # Final fallback: SOTA may omit team1 and the player may not be in the
            # synced lineup (e.g., card for a bench player). Match by name against
            # player_teams for the two game teams in this season.
            if team_id is None and (first_name1 or last_name1):
                team_id_from_roster, player_id_from_roster = await self._infer_team_id_by_name(
                    game, first_name1, last_name1,
                )
                if team_id_from_roster is not None:
                    team_id = team_id_from_roster
                    if player_id is None and player_id_from_roster is not None:
                        player_id = player_id_from_roster

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
                if not is_assist_supported_event_type(matched_event.event_type):
                    before_assist = (
                        matched_event.assist_player_id,
                        matched_event.assist_player_name,
                        matched_event.assist_manual_override,
                    )
                    sync_event_assist(matched_event, None)
                    after_assist = (
                        matched_event.assist_player_id,
                        matched_event.assist_player_name,
                        matched_event.assist_manual_override,
                    )
                    if before_assist != after_assist:
                        changed = True
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
            before_assist = (
                event.assist_player_id,
                event.assist_player_name,
                event.assist_manual_override,
            )
            sync_event_assist(event, assist_info)
            after_assist = (
                event.assist_player_id,
                event.assist_player_name,
                event.assist_manual_override,
            )
            if before_assist != after_assist:
                updated += 1

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

    async def _infer_team_id_by_name(
        self,
        game: Game,
        first_name: str,
        last_name: str,
    ) -> tuple[int | None, int | None]:
        """Fallback: match player by name against player_teams of the two game teams
        in this game's season. Returns (team_id, player_id) if unambiguous."""
        if (not first_name and not last_name) or not game.season_id:
            return None, None
        team_candidates = [tid for tid in (game.home_team_id, game.away_team_id) if tid]
        if not team_candidates:
            return None, None

        name_match = self._player_name_match_clause(first_name, last_name)
        result = await self.db.execute(
            select(PlayerTeam.team_id, PlayerTeam.player_id)
            .join(Player, Player.id == PlayerTeam.player_id)
            .where(
                PlayerTeam.season_id == game.season_id,
                PlayerTeam.team_id.in_(team_candidates),
                name_match,
            )
        )
        rows = result.all()
        team_ids = {row[0] for row in rows}
        if len(team_ids) == 1:
            team_id = next(iter(team_ids))
            player_ids = {row[1] for row in rows}
            player_id = next(iter(player_ids)) if len(player_ids) == 1 else None
            return team_id, player_id
        return None, None

    @staticmethod
    def _player_name_match_clause(first_name: str, last_name: str):
        """Match player on first_name AND last_name allowing mixed locales (SOTA
        sometimes sends KZ first name with RU last name or vice versa)."""
        first_match = or_(
            Player.first_name == first_name,
            Player.first_name_kz == first_name,
            Player.first_name_en == first_name,
        )
        last_match = or_(
            Player.last_name == last_name,
            Player.last_name_kz == last_name,
            Player.last_name_en == last_name,
        )
        return and_(first_match, last_match)

    async def _find_player_id(
        self, first_name: str, last_name: str, game_id: int, team_id: int | None = None
    ) -> int | None:
        """Find player ID by name from game lineup."""
        if not first_name and not last_name:
            return None

        name_match = self._player_name_match_clause(first_name, last_name)

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
        game.half1_started_at = utcnow()
        await self.db.commit()

        await set_live_flag()

        try:
            sync_result = await self.sync_live_events(game_id)
            new_events = sync_result.get("added", 0)
        except Exception:
            logger.warning("Initial event sync failed for game %s (SOTA may not have data yet)", game_id)
            new_events = 0

        return {
            "game_id": game_id,
            "is_live": True,
            "new_events_count": new_events,
        }

    async def stop_live_tracking(self, game_id: int) -> dict:
        """Stop live tracking for a game. Performs final sync before marking finished."""
        from app.utils.live_flag import clear_live_flag

        game = await self.db.get(Game, game_id)
        if not game:
            return {"error": f"Game {game_id} not found"}

        # Final sync before marking finished
        if game.sota_id and not game.sync_disabled:
            try:
                await self.sync_live_events(game_id)
                await self.sync_live_stats(game_id)
                await self.sync_live_player_stats(game_id)
            except Exception:
                logger.exception("Final sync failed for game %s", game_id)

        game.status = GameStatus.finished
        game.finished_at = utcnow()
        game.live_minute = None
        game.live_half = None
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
