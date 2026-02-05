"""
Live synchronization service for real-time match data from SOTA /em/ endpoints.

Handles:
- Pre-game lineup sync (30 min before match)
- Live event sync during match (goals, cards, substitutions)
- Formation extraction from lineup data
"""
from datetime import datetime, date, time as dt_time, timedelta
from uuid import UUID
from typing import Any

from sqlalchemy import select, and_, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameEvent, GameEventType, GameLineup, LineupType, Team, Player
from app.services.sota_client import SotaClient


# Mapping SOTA action names to our event types
ACTION_TYPE_MAP = {
    "ГОЛ": GameEventType.goal,
    "ГОЛЕВОЙ ПАС": GameEventType.assist,
    "ЖК": GameEventType.yellow_card,
    "КК": GameEventType.red_card,
    "ЗАМЕНА": GameEventType.substitution,
}


class LiveSyncService:
    """Service for syncing live match data from SOTA /em/ endpoints."""

    def __init__(self, db: AsyncSession, client: SotaClient):
        self.db = db
        self.client = client

    async def get_upcoming_games(self, minutes_ahead: int = 30) -> list[Game]:
        """
        Get games that start within the next N minutes.

        Used to trigger pre-game lineup sync.
        """
        now = datetime.now()
        today = now.date()
        current_time = now.time()
        cutoff_time = (now + timedelta(minutes=minutes_ahead)).time()

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.date == today,
                    Game.time >= current_time,
                    Game.time <= cutoff_time,
                    Game.is_live == False,
                    Game.has_lineup == False,
                )
            )
        )
        return list(result.scalars().all())

    async def get_active_games(self) -> list[Game]:
        """
        Get games that are currently live.

        Used to sync live events during match.
        """
        result = await self.db.execute(
            select(Game).where(Game.is_live == True)
        )
        return list(result.scalars().all())

    async def get_games_to_end(self) -> list[Game]:
        """
        Get live games that should have ended (started > 2 hours ago).

        Used to automatically end live tracking.
        """
        now = datetime.now()
        cutoff = now - timedelta(hours=2)

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.is_live == True,
                    Game.date <= cutoff.date(),
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
        """
        Extract starters and substitutes from lineup data.

        Returns (starters, substitutes) tuple.
        """
        starters = []
        substitutes = []
        current_section = None

        for item in lineup_data:
            number = item.get("number")

            # Detect section markers
            if number == "ОСНОВНЫЕ":
                current_section = "starters"
                continue
            elif number == "ЗАПАСНЫЕ":
                current_section = "substitutes"
                continue
            elif number in ("TEAM", "FORMATION", "COACH", "MAIN"):
                continue

            # Skip items without player ID
            if not item.get("id"):
                continue

            # Add to appropriate list
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

    async def sync_pregame_lineup(self, game_id: str) -> dict:
        """
        Sync pre-game lineup data for a match.

        Fetches lineup from /em/ endpoints and saves to database.
        Updates game with formation info.
        """
        game_uuid = UUID(game_id)

        # Get game to find team IDs
        result = await self.db.execute(
            select(Game).where(Game.id == game_uuid)
        )
        game = result.scalar_one_or_none()
        if not game:
            return {"error": f"Game {game_id} not found"}

        # Fetch lineup data from SOTA
        home_data = await self.client.get_live_team_lineup(game_id, "home")
        away_data = await self.client.get_live_team_lineup(game_id, "away")

        # Extract formations
        home_formation = self._extract_formation(home_data)
        away_formation = self._extract_formation(away_data)

        # Extract players
        home_starters, home_subs = self._extract_players(home_data)
        away_starters, away_subs = self._extract_players(away_data)

        # Save lineups to database
        lineup_count = 0

        for player_data in home_starters:
            lineup_count += await self._save_player_lineup(
                game_uuid, game.home_team_id, player_data, LineupType.starter
            )

        for player_data in home_subs:
            lineup_count += await self._save_player_lineup(
                game_uuid, game.home_team_id, player_data, LineupType.substitute
            )

        for player_data in away_starters:
            lineup_count += await self._save_player_lineup(
                game_uuid, game.away_team_id, player_data, LineupType.starter
            )

        for player_data in away_subs:
            lineup_count += await self._save_player_lineup(
                game_uuid, game.away_team_id, player_data, LineupType.substitute
            )

        # Update game with formations and lineup flag
        game.home_formation = home_formation
        game.away_formation = away_formation
        game.has_lineup = True

        await self.db.commit()

        return {
            "game_id": game_id,
            "home_formation": home_formation,
            "away_formation": away_formation,
            "lineup_count": lineup_count,
        }

    async def _save_player_lineup(
        self,
        game_id: UUID,
        team_id: int,
        player_data: dict,
        lineup_type: LineupType,
    ) -> int:
        """Save a single player lineup entry."""
        player_id = player_data.get("id")
        if not player_id:
            return 0

        try:
            player_uuid = UUID(player_id)
        except ValueError:
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
            player_id=player_uuid,
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
                # Preserve existing values if new data doesn't provide them.
                "amplua": func.coalesce(stmt.excluded.amplua, GameLineup.amplua),
                "field_position": func.coalesce(stmt.excluded.field_position, GameLineup.field_position),
            }
        )
        await self.db.execute(stmt)
        return 1

    async def sync_live_events(self, game_id: str) -> list[GameEvent]:
        """
        Sync live events for a match.

        Fetches events from /em/ endpoint and saves new ones to database.
        Returns list of newly added events (for WebSocket broadcast).
        """
        game_uuid = UUID(game_id)

        # Get existing events to avoid duplicates
        result = await self.db.execute(
            select(GameEvent).where(GameEvent.game_id == game_uuid)
        )
        existing_events = list(result.scalars().all())

        # Create set of existing event signatures for deduplication
        # Store both player_id and player_name based signatures to catch duplicates
        # even if one sync finds player_id and another doesn't
        existing_signatures = set()
        for e in existing_events:
            # Normalize player name (strip whitespace, lowercase)
            normalized_name = e.player_name.strip().lower() if e.player_name else ""

            # Add signature with player_id if available
            if e.player_id:
                sig_by_id = (e.half, e.minute, e.event_type.value, str(e.player_id))
                existing_signatures.add(sig_by_id)

            # Always add signature with normalized player name as fallback
            if normalized_name:
                sig_by_name = (e.half, e.minute, e.event_type.value, normalized_name)
                existing_signatures.add(sig_by_name)

        # Fetch events from SOTA
        events_data = await self.client.get_live_match_events(game_id)

        # Get game for team IDs with eager loading of teams
        result = await self.db.execute(
            select(Game)
            .options(selectinload(Game.home_team), selectinload(Game.away_team))
            .where(Game.id == game_uuid)
        )
        game = result.scalar_one_or_none()
        if not game:
            return []

        new_events = []
        # Track assists by (half, minute, scorer_name) for linking to goals
        assists_map: dict[tuple[int, int, str], dict] = {}

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

            # Determine team ID by matching team name (check all language variants)
            team_name = event_data.get("team1", "")
            team_name_normalized = team_name.strip().lower() if team_name else ""
            team_id = None
            if team_name_normalized:
                team_id = self._match_team_id(game, team_name_normalized)

            # Find player ID by name from lineup
            player_id = await self._find_player_id(first_name1, last_name1, game_uuid, team_id)

            # Check for duplicate using both player_id and normalized player_name
            # This prevents duplicates even if one sync finds player_id and another doesn't
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

            # Process player2 data only for relevant event types
            # - substitution: player2 = player coming ON (useful)
            # - assist: player2 = scorer (useful for linking)
            # - goal: player2 = opponent defender (NOT useful, skip)
            # - cards: no player2 needed
            player2_id = None
            player2_number = None
            player2_name = ""
            player2_team_name = ""

            if event_type in (GameEventType.substitution, GameEventType.assist):
                first_name2 = event_data.get("first_name2", "")
                last_name2 = event_data.get("last_name2", "")
                team2_name = event_data.get("team2", "")
                team2_id = None
                if team2_name:
                    team2_id = self._match_team_id(game, team2_name.strip().lower())
                player2_id = await self._find_player_id(first_name2, last_name2, game_uuid, team2_id)
                player2_number = self._parse_number(event_data.get("number2"))
                player2_name = f"{first_name2} {last_name2}".strip()
                player2_team_name = team2_name

            # Create event
            event = GameEvent(
                game_id=game_uuid,
                half=half,
                minute=minute,
                event_type=event_type,
                team_id=team_id,
                team_name=team_name,
                player_id=player_id,
                player_number=self._parse_number(event_data.get("number1")),
                player_name=player_name,
                player2_id=player2_id,
                player2_number=player2_number,
                player2_name=player2_name,
                player2_team_name=player2_team_name,
            )

            self.db.add(event)
            new_events.append(event)

            # Add signatures for this new event to prevent duplicates within this sync
            if player_id:
                sig_by_id = (half, minute, event_type.value, str(player_id))
                existing_signatures.add(sig_by_id)
            if normalized_name:
                sig_by_name = (half, minute, event_type.value, normalized_name)
                existing_signatures.add(sig_by_name)

            # Collect assist info for linking to goals
            # Assist event: player = assister, player2 = scorer
            if event_type == GameEventType.assist and player2_name:
                key = (half, minute, player2_name)
                assists_map[key] = {
                    "player_id": player_id,
                    "player_name": player_name,
                }

        # Link assists to goals
        for event in new_events:
            if event.event_type == GameEventType.goal:
                key = (event.half, event.minute, event.player_name)
                assist_info = assists_map.get(key)
                if assist_info:
                    event.assist_player_id = assist_info["player_id"]
                    event.assist_player_name = assist_info["player_name"]

        if new_events:
            await self.db.commit()

        return new_events

    def _parse_number(self, value: Any) -> int | None:
        """Parse player number from various formats."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _match_team_id(self, game: Game, team_name_normalized: str) -> int | None:
        """
        Match team name to team ID using all language variants.

        Checks name, name_kz, and name_en fields.
        """
        if not team_name_normalized:
            return None

        # Check home team (all language variants)
        if game.home_team:
            if (game.home_team.name and game.home_team.name.strip().lower() == team_name_normalized):
                return game.home_team_id
            if hasattr(game.home_team, 'name_kz') and game.home_team.name_kz:
                if game.home_team.name_kz.strip().lower() == team_name_normalized:
                    return game.home_team_id
            if hasattr(game.home_team, 'name_en') and game.home_team.name_en:
                if game.home_team.name_en.strip().lower() == team_name_normalized:
                    return game.home_team_id

        # Check away team (all language variants)
        if game.away_team:
            if (game.away_team.name and game.away_team.name.strip().lower() == team_name_normalized):
                return game.away_team_id
            if hasattr(game.away_team, 'name_kz') and game.away_team.name_kz:
                if game.away_team.name_kz.strip().lower() == team_name_normalized:
                    return game.away_team_id
            if hasattr(game.away_team, 'name_en') and game.away_team.name_en:
                if game.away_team.name_en.strip().lower() == team_name_normalized:
                    return game.away_team_id

        return None

    async def _find_player_id(
        self, first_name: str, last_name: str, game_id: UUID, team_id: int | None = None
    ) -> UUID | None:
        """
        Find player ID by name from game lineup.

        Searches players in the game's lineup matching the given name.
        Checks all language variants (RU, KZ, EN) since SOTA API may return
        names in different languages depending on the endpoint.
        If team_id is provided, filters by team as well.
        """
        if not first_name and not last_name:
            return None

        # Build query to find player in game lineup
        # Match against any language variant (RU, KZ, EN)
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
        return result.scalar_one_or_none()

    async def start_live_tracking(self, game_id: str) -> dict:
        """
        Start live tracking for a game.

        Sets is_live=True and syncs initial data.
        """
        game_uuid = UUID(game_id)

        result = await self.db.execute(
            select(Game).where(Game.id == game_uuid)
        )
        game = result.scalar_one_or_none()
        if not game:
            return {"error": f"Game {game_id} not found"}

        # Sync lineup if not already done
        if not game.has_lineup:
            await self.sync_pregame_lineup(game_id)

        game.is_live = True
        await self.db.commit()

        # Initial event sync
        events = await self.sync_live_events(game_id)

        return {
            "game_id": game_id,
            "is_live": True,
            "new_events_count": len(events),
        }

    async def stop_live_tracking(self, game_id: str) -> dict:
        """
        Stop live tracking for a game.

        Sets is_live=False.
        """
        game_uuid = UUID(game_id)

        result = await self.db.execute(
            select(Game).where(Game.id == game_uuid)
        )
        game = result.scalar_one_or_none()
        if not game:
            return {"error": f"Game {game_id} not found"}

        game.is_live = False
        await self.db.commit()

        return {
            "game_id": game_id,
            "is_live": False,
        }

    async def get_game_events(self, game_id: str) -> list[GameEvent]:
        """Get all events for a game."""
        game_uuid = UUID(game_id)

        result = await self.db.execute(
            select(GameEvent)
            .where(GameEvent.game_id == game_uuid)
            .order_by(GameEvent.half, GameEvent.minute)
        )
        return list(result.scalars().all())
