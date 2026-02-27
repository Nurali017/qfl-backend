"""
Game sync service.

Handles synchronization of games, game statistics, and game events from SOTA API.
"""
import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, or_
from sqlalchemy.dialects.postgresql import insert

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
        return {"teams": team_count, "players": player_count}

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

    async def sync_game_events(self, game_id: int) -> dict:
        """
        Sync events for a specific game from SOTA /em/ endpoint.

        Works for both live and completed games.

        Args:
            game_id: Game int ID

        Returns:
            Dict with game_id and events_added count
        """
        game = await self.db.get(Game, game_id)
        if not game:
            return {"error": f"Game {game_id} not found", "events_added": 0}
        if not game.sota_id:
            return {"error": f"Game {game_id} has no sota_id", "events_added": 0}
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

        # Get existing events
        result = await self.db.execute(
            select(GameEvent).where(GameEvent.game_id == game_id)
        )
        existing_events = list(result.scalars().all())

        # Create deduplication set
        existing_signatures = set()
        for e in existing_events:
            normalized_name = e.player_name.strip().lower() if e.player_name else ""

            if e.player_id:
                sig_by_id = (e.half, e.minute, e.event_type.value, str(e.player_id))
                existing_signatures.add(sig_by_id)

            if normalized_name:
                sig_by_name = (e.half, e.minute, e.event_type.value, normalized_name)
                existing_signatures.add(sig_by_name)

        # Fetch events from SOTA
        events_data = await self.client.get_live_match_events(sota_uuid)

        # Action type mapping
        ACTION_TYPE_MAP = {
            "ГОЛ": GameEventType.goal,
            "ГОЛЕВОЙ ПАС": GameEventType.assist,
            "ЖК": GameEventType.yellow_card,
            "КК": GameEventType.red_card,
            "ЗАМЕНА": GameEventType.substitution,
        }

        events_added = 0
        for event_data in events_data:
            action = event_data.get("action", "")
            event_type = ACTION_TYPE_MAP.get(action)
            if not event_type:
                continue

            half = event_data.get("half", 1)
            minute = event_data.get("time", 0)
            first_name1 = event_data.get("first_name1", "")
            last_name1 = event_data.get("last_name1", "")
            player_name = f"{first_name1} {last_name1}".strip()

            # Determine team ID (normalized comparison)
            team_name = event_data.get("team1", "")
            team_name_normalized = team_name.strip().lower() if team_name else ""
            team_id = None
            if home_team and home_team.name and home_team.name.strip().lower() == team_name_normalized:
                team_id = game.home_team_id
            elif away_team and away_team.name and away_team.name.strip().lower() == team_name_normalized:
                team_id = game.away_team_id

            # Find player ID from lineup
            player_id = await self._find_player_id_from_lineup(
                game_id, first_name1, last_name1, team_id
            )

            # Check for duplicate using player_id and normalized name signatures.
            normalized_name = player_name.strip().lower() if player_name else ""

            is_duplicate = False
            if player_id:
                sig_by_id = (half, minute, event_type.value, str(player_id))
                if sig_by_id in existing_signatures:
                    is_duplicate = True

            if normalized_name and not is_duplicate:
                sig_by_name = (half, minute, event_type.value, normalized_name)
                if sig_by_name in existing_signatures:
                    is_duplicate = True

            if is_duplicate:
                continue

            # Find player2 ID
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

            # Create event
            event = GameEvent(
                game_id=game_id,
                half=half,
                minute=minute,
                event_type=event_type,
                team_id=team_id,
                team_name=team_name,
                player_id=player_id,
                player_number=self._parse_number(event_data.get("number1")),
                player_name=player_name,
                player2_id=player2_id,
                player2_number=self._parse_number(event_data.get("number2")),
                player2_name=f"{first_name2} {last_name2}".strip(),
                player2_team_name=team2_name,
            )

            self.db.add(event)
            if player_id:
                sig_by_id = (half, minute, event_type.value, str(player_id))
                existing_signatures.add(sig_by_id)
            if normalized_name:
                sig_by_name = (half, minute, event_type.value, normalized_name)
                existing_signatures.add(sig_by_name)
            events_added += 1

        if events_added > 0:
            await self.db.commit()
            logger.info(f"Game {game_id}: added {events_added} events")

        return {"game_id": game_id, "events_added": events_added}

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

        total_events = 0
        games_synced = 0
        errors = []

        for game in games:
            try:
                result = await self.sync_game_events(game.id)
                if "error" not in result:
                    total_events += result.get("events_added", 0)
                    games_synced += 1
                else:
                    errors.append({"game_id": game.id, "error": result["error"]})
            except Exception as e:
                logger.error(f"Failed to sync events for game {game.id}: {e}")
                errors.append({"game_id": game.id, "error": str(e)})

        return {
            "games_synced": games_synced,
            "total_events_added": total_events,
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
