"""
Team of the Week sync service.

Fetches team-of-week data from SOTA API and upserts into the local database.
Enriches players with local DB data (person_id, photo, per-tour stats).
"""
import logging
from typing import Any

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.game import Game
from app.models.game_event import GameEvent, GameEventType
from app.models.game_player_stats import GamePlayerStats
from app.models.player import Player
from app.models.player_season_stats import PlayerSeasonStats
from app.models.team_of_week import TeamOfWeek
from app.services.sync.base import BaseSyncService
from app.utils.file_urls import resolve_file_url

logger = logging.getLogger(__name__)

LOCALES = ("ru", "kz")

# SOTA amplua → frontend position group
AMPLUA_TO_GROUP: dict[str, str] = {
    "Gk": "GK",
    "D": "DEF",
    "DM": "MID",
    "M": "MID",
    "AM": "MID",
    "F": "FWD",
}

# player_season_stats column → frontend stat key (field players) — season-level fallback
FIELD_STATS_MAP: dict[str, str] = {
    "games_played": "games_played",
    "minutes_played": "minutes_played",
    "goals": "goals",
    "assists": "assists",
    "shots": "shots",
    "shots_on_goal": "shots_on_goal",
    "passes": "passes",
    "pass_accuracy": "pass_accuracy",
    "key_passes": "key_passes",
    "pass_cross": "crosses",
    "pass_cross_acc": "cross_accuracy",
    "dribble": "dribbles",
    "dribble_success": "dribble_success",
    "tackle": "tackles",
    "interception": "interceptions",
    "duels_won": "duels_won",
    "aerial_duel_success": "aerial_won",
    "foul": "fouls",
    "foul_taken": "fouls_drawn",
    "offside": "offsides",
    "yellow_cards": "yellow_cards",
    "red_cards": "red_cards",
    "xg": "xg",
}

# player_season_stats column → frontend stat key (goalkeepers) — season-level fallback
GK_STATS_MAP: dict[str, str] = {
    "games_played": "games_played",
    "minutes_played": "minutes_played",
    "save_shot": "saves",
    "goals_conceded": "goals_conceded",
    "dry_match": "clean_sheets",
    "save_shot_ratio": "save_percentage",
    "save_shot_penalty_success": "penalty_saves",
    "passes": "passes",
    "pass_accuracy": "pass_accuracy",
    "yellow_cards": "yellow_cards",
    "red_cards": "red_cards",
}

# ── Per-game stat mappings (tour-level enrichment) ──

# GamePlayerStats column → frontend stat key (field players)
GAME_FIELD_STATS_MAP: dict[str, str] = {
    "minutes_played": "minutes_played",
    "shots": "shots",
    "shots_on_goal": "shots_on_goal",
    "passes": "passes",
    "pass_accuracy": "pass_accuracy",
    "tackle": "tackles",
    "foul": "fouls",
    "offside": "offsides",
    "yellow_cards": "yellow_cards",
    "red_cards": "red_cards",
}

# extra_stats v2 keys → frontend stat key (field players, supplement)
GAME_FIELD_V2_MAP: dict[str, str] = {
    "key_pass": "key_passes",
    "pass_cross": "crosses",
    "pass_cross_acc": "cross_accuracy",
    "dribble": "dribbles",
    "dribble_success": "dribble_success",
    "interception": "interceptions",
    "duel_success": "duels_won",
    "aerial_duel_success": "aerial_won",
    "foul_taken": "fouls_drawn",
    "xg": "xg",
}

# extra_stats v2 keys → frontend stat key (goalkeepers)
GAME_GK_V2_MAP: dict[str, str] = {
    "save_shot": "saves",
    "goals_conceded": "goals_conceded",
    "dry_match": "clean_sheets",
    "save_shot_ratio": "save_percentage",
    "save_shot_penalty_success": "penalty_saves",
}


class TeamOfWeekSyncService(BaseSyncService):
    """Sync team-of-week data from SOTA API."""

    async def _discover_tour_keys(self, season_id: int) -> list[str]:
        """Auto-discover tour keys from the games table max tour number."""
        result = await self.db.execute(
            select(func.max(Game.tour)).where(Game.season_id == season_id)
        )
        max_tour = result.scalar()
        if not max_tour or max_tour < 1:
            return []
        return [f"tour_{i}" for i in range(1, max_tour + 1)]

    @staticmethod
    def _parse_tour_number(tour_key: str) -> int | None:
        """Extract numeric tour number from a tour key like 'tour_5'."""
        if tour_key.startswith("tour_"):
            try:
                return int(tour_key.split("_", 1)[1])
            except (IndexError, ValueError):
                return None
        return None

    async def _build_enrichment_cache(
        self, season_id: int, sota_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Build a cache mapping SOTA player IDs to local data (person_id, photo, stats).

        Uses season-level PlayerSeasonStats.
        Returns {sota_id: {"person_id": int, "photo": str, "stats": dict}}.
        """
        if not sota_ids:
            return {}

        # 1. Fetch local players by SOTA IDs
        result = await self.db.execute(
            select(Player).where(Player.sota_id.in_(sota_ids))
        )
        players = {str(p.sota_id): p for p in result.scalars().all()}

        # 2. Fetch season stats for matched player IDs
        player_ids = [p.id for p in players.values()]
        stats_by_player: dict[int, PlayerSeasonStats] = {}
        if player_ids:
            result = await self.db.execute(
                select(PlayerSeasonStats).where(
                    PlayerSeasonStats.player_id.in_(player_ids),
                    PlayerSeasonStats.season_id == season_id,
                )
            )
            for s in result.scalars().all():
                stats_by_player[s.player_id] = s

        # 3. Build cache
        cache: dict[str, dict[str, Any]] = {}
        for sota_id, player in players.items():
            pss = stats_by_player.get(player.id)
            # photo_url is stored as object name, resolve to full URL
            photo = resolve_file_url(player.photo_url) if player.photo_url else None

            cache[sota_id] = {
                "person_id": player.id,
                "photo": photo,
                "stats": self._extract_stats(pss, player),
                # Localized names for enrichment
                "first_name": player.first_name,
                "first_name_kz": player.first_name_kz,
                "last_name": player.last_name,
                "last_name_kz": player.last_name_kz,
            }
        return cache

    async def _build_tour_enrichment_cache(
        self, season_id: int, tour_number: int, sota_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Build enrichment cache using per-game stats for a specific tour.

        Fetches GamePlayerStats + GameEvent data for games in the given tour.
        Returns {sota_id: {"person_id": int, "photo": str, "stats": dict}}.
        """
        if not sota_ids:
            return {}

        # 1. Fetch local players by SOTA IDs
        result = await self.db.execute(
            select(Player).where(Player.sota_id.in_(sota_ids))
        )
        players = {str(p.sota_id): p for p in result.scalars().all()}
        if not players:
            return {}

        player_ids = [p.id for p in players.values()]

        # 2. Find game IDs for this tour
        result = await self.db.execute(
            select(Game.id).where(
                Game.season_id == season_id,
                Game.tour == tour_number,
            )
        )
        game_ids = [row[0] for row in result.all()]
        if not game_ids:
            logger.debug(f"No games found for season={season_id}, tour={tour_number}")
            return await self._build_enrichment_cache(season_id, sota_ids)

        # 3. Fetch GamePlayerStats for those games + players
        result = await self.db.execute(
            select(GamePlayerStats).where(
                GamePlayerStats.game_id.in_(game_ids),
                GamePlayerStats.player_id.in_(player_ids),
            )
        )
        # player_id → GamePlayerStats (one game per tour per player typically)
        gps_by_player: dict[int, GamePlayerStats] = {}
        for gps in result.scalars().all():
            gps_by_player[gps.player_id] = gps

        # 4. Count goals from GameEvent (goal + penalty events)
        result = await self.db.execute(
            select(
                GameEvent.player_id,
                func.count(GameEvent.id),
            ).where(
                GameEvent.game_id.in_(game_ids),
                GameEvent.player_id.in_(player_ids),
                GameEvent.event_type.in_([GameEventType.goal, GameEventType.penalty]),
            ).group_by(GameEvent.player_id)
        )
        goals_by_player: dict[int, int] = {row[0]: row[1] for row in result.all()}

        # 5. Count assists from GameEvent
        result = await self.db.execute(
            select(
                GameEvent.assist_player_id,
                func.count(GameEvent.id),
            ).where(
                GameEvent.game_id.in_(game_ids),
                GameEvent.assist_player_id.in_(player_ids),
                GameEvent.event_type.in_([GameEventType.goal, GameEventType.penalty]),
            ).group_by(GameEvent.assist_player_id)
        )
        assists_by_player: dict[int, int] = {row[0]: row[1] for row in result.all()}

        # 6. Build cache
        cache: dict[str, dict[str, Any]] = {}
        for sota_id, player in players.items():
            photo = resolve_file_url(player.photo_url) if player.photo_url else None
            gps = gps_by_player.get(player.id)
            goals = goals_by_player.get(player.id, 0)
            assists = assists_by_player.get(player.id, 0)

            cache[sota_id] = {
                "person_id": player.id,
                "photo": photo,
                "stats": self._extract_game_stats(gps, player, goals, assists),
                "first_name": player.first_name,
                "first_name_kz": player.first_name_kz,
                "last_name": player.last_name,
                "last_name_kz": player.last_name_kz,
            }
        return cache

    @staticmethod
    def _extract_stats(
        pss: "PlayerSeasonStats | None",
        player: "Player",
    ) -> dict[str, Any]:
        """Extract stats dict from PlayerSeasonStats, using the appropriate mapping."""
        if pss is None:
            return {}

        # Choose mapping based on player role
        is_gk = (player.top_role or "").lower() in ("gk", "goalkeeper", "вратарь", "қақпашы")
        mapping = GK_STATS_MAP if is_gk else FIELD_STATS_MAP

        stats: dict[str, Any] = {}
        for db_col, fe_key in mapping.items():
            val = getattr(pss, db_col, None)
            if val is not None:
                stats[fe_key] = val
        return stats

    @staticmethod
    def _extract_game_stats(
        gps: "GamePlayerStats | None",
        player: "Player",
        goals: int,
        assists: int,
    ) -> dict[str, Any]:
        """Extract stats from per-game data (GamePlayerStats + event counts)."""
        stats: dict[str, Any] = {}

        # Always add goals/assists from events
        if goals:
            stats["goals"] = goals
        if assists:
            stats["assists"] = assists

        if gps is None:
            return stats

        is_gk = (player.top_role or "").lower() in ("gk", "goalkeeper", "вратарь", "қақпашы")

        # Common columns from GamePlayerStats
        for db_col, fe_key in GAME_FIELD_STATS_MAP.items():
            val = getattr(gps, db_col, None)
            if val is not None:
                stats[fe_key] = val

        # V2 extra_stats
        extra = gps.extra_stats or {}
        if is_gk:
            v2_map = GAME_GK_V2_MAP
        else:
            v2_map = GAME_FIELD_V2_MAP

        for v2_key, fe_key in v2_map.items():
            val = extra.get(v2_key)
            if val is not None:
                stats[fe_key] = val

        return stats

    def _map_sota_response(
        self,
        sota_data: dict[str, Any],
        enrichment: dict[str, dict[str, Any]],
        locale: str = "ru",
        ru_names: dict[str, dict[str, str]] | None = None,
    ) -> tuple[str | None, list[dict]]:
        """Map SOTA response to local payload format with enrichment.

        Returns (scheme, payload) tuple.
        """
        data = sota_data.get("data", {})
        scheme = data.get("placement_schema_name")
        players = data.get("players", [])

        payload = []
        for p in players:
            team = p.get("team") or {}
            sota_id = p.get("id")
            raw_amplua = p.get("amplua")
            normalized_amplua = AMPLUA_TO_GROUP.get(raw_amplua, raw_amplua)

            enrich = enrichment.get(str(sota_id), {}) if sota_id else {}

            # Use Russian SOTA names as base for non-ru locales (SOTA returns English for kz)
            ru_fallback = (ru_names or {}).get(str(sota_id), {}) if locale != "ru" else {}
            first_name = ru_fallback.get("first_name") or p.get("first_name", "")
            last_name = ru_fallback.get("last_name") or p.get("last_name", "")
            full_name = ru_fallback.get("full_name") or p.get("full_name", "")

            if enrich:
                suffix = f"_{locale}" if locale != "ru" else ""
                local_first = enrich.get(f"first_name{suffix}") or enrich.get("first_name")
                local_last = enrich.get(f"last_name{suffix}") or enrich.get("last_name")
                if local_first:
                    first_name = local_first
                if local_last:
                    last_name = local_last
                if local_first or local_last:
                    full_name = f"{first_name} {last_name}".strip()

            payload.append({
                "id": sota_id,
                "person_id": enrich.get("person_id"),
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "team": team,
                "number": p.get("number"),
                "captain": p.get("captain", False),
                "amplua": raw_amplua,
                "position": p.get("position"),
                "ordering": p.get("ordering"),
                "photo": enrich.get("photo"),
                "command_logo": team.get("image"),
                "command_id": team.get("id"),
                "stats": enrich.get("stats") if enrich else None,
            })

        return scheme, payload

    async def sync_team_of_week(
        self,
        season_id: int,
        tour_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """Sync team-of-week data for a season.

        Args:
            season_id: Local season ID.
            tour_keys: Explicit list of tour keys (e.g. ["tour_1", "tour_2"]).
                       If None, auto-discovers from games table.

        Returns:
            Dict with sync stats.
        """
        sota_season_id = await self.get_sota_season_id(season_id)

        if not tour_keys:
            tour_keys = await self._discover_tour_keys(season_id)
            if not tour_keys:
                logger.warning(f"Season {season_id}: no tours found in games table")
                return {"tours_synced": 0, "tours_skipped": 0, "tours_empty": 0}

        logger.info(
            f"Syncing team-of-week for season {season_id} "
            f"(SOTA {sota_season_id}), {len(tour_keys)} tours"
        )

        tours_synced = 0
        tours_skipped = 0
        tours_empty = 0

        # Phase 1: fetch all SOTA data, collect SOTA IDs per tour_key
        fetched: dict[tuple[str, str], dict] = {}
        sota_ids_by_tour: dict[str, set[str]] = {}

        for tour_key in tour_keys:
            for locale in LOCALES:
                try:
                    sota_data = await self.client.get_team_of_week(
                        sota_season_id, tour_key, language=locale
                    )
                except Exception:
                    logger.warning(
                        f"Failed to fetch team-of-week: "
                        f"season={sota_season_id}, tour={tour_key}, locale={locale}",
                        exc_info=True,
                    )
                    tours_skipped += 1
                    continue

                fetched[(tour_key, locale)] = sota_data
                for p in (sota_data.get("data", {}).get("players", [])):
                    pid = p.get("id")
                    if pid:
                        sota_ids_by_tour.setdefault(tour_key, set()).add(str(pid))

        # Phase 2: build enrichment cache per tour_key
        enrichment_by_tour: dict[str, dict[str, dict[str, Any]]] = {}
        for tour_key, ids in sota_ids_by_tour.items():
            tour_num = self._parse_tour_number(tour_key)
            if tour_num is not None:
                enrichment_by_tour[tour_key] = await self._build_tour_enrichment_cache(
                    season_id, tour_num, list(ids),
                )
            else:
                enrichment_by_tour[tour_key] = await self._build_enrichment_cache(
                    season_id, list(ids),
                )

        # Phase 3: build Russian name fallbacks per tour
        ru_names_by_tour: dict[str, dict[str, dict[str, str]]] = {}
        for tour_key in tour_keys:
            ru_data = fetched.get((tour_key, "ru"))
            if ru_data:
                names: dict[str, dict[str, str]] = {}
                for p in ru_data.get("data", {}).get("players", []):
                    pid = p.get("id")
                    if pid:
                        names[str(pid)] = {
                            "first_name": p.get("first_name", ""),
                            "last_name": p.get("last_name", ""),
                            "full_name": p.get("full_name", ""),
                        }
                ru_names_by_tour[tour_key] = names

        # Phase 4: map and upsert
        for (tour_key, locale), sota_data in fetched.items():
            enrichment = enrichment_by_tour.get(tour_key, {})
            ru_names = ru_names_by_tour.get(tour_key) if locale != "ru" else None
            scheme, payload = self._map_sota_response(sota_data, enrichment, locale=locale, ru_names=ru_names)

            if not payload:
                tours_empty += 1
                continue

            stmt = pg_insert(TeamOfWeek).values(
                season_id=season_id,
                tour_key=tour_key,
                locale=locale,
                scheme=scheme,
                payload=payload,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_team_of_week_season_tour_locale",
                set_={
                    "scheme": stmt.excluded.scheme,
                    "payload": stmt.excluded.payload,
                },
            )
            await self.db.execute(stmt)
            tours_synced += 1

        await self.db.commit()
        result = {
            "tours_synced": tours_synced,
            "tours_skipped": tours_skipped,
            "tours_empty": tours_empty,
        }
        logger.info(f"Team-of-week sync complete for season {season_id}: {result}")
        return result
