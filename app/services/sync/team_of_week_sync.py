"""
Team of the Week sync service.

Fetches team-of-week data from SOTA API and upserts into the local database.
Enriches players with local DB data (person_id, photo, per-tour stats).
Stats are fetched directly from SOTA v2 /players/{id}/game_stats/ endpoint.
"""
import asyncio
import logging
from typing import Any

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from app.models.game import Game
from app.models.player import Player
from app.models.player_season_stats import PlayerSeasonStats
from app.models.player_team import PlayerTeam
from app.models.team import Team
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

# ── SOTA v2 /players/{id}/game_stats/ key → frontend stat key ──

V2_TO_FRONTEND: dict[str, str] = {
    # Field player stats
    "goal": "goals",
    "assist": "assists",
    "shot": "shots",
    "shots_on_goal": "shots_on_goal",
    "pass": "passes",
    "key_pass": "key_passes",
    "pass_cross": "crosses",
    "pass_cross_acc": "cross_accuracy",
    "dribble": "dribbles",
    "dribble_success": "dribble_success",
    "tackle": "tackles",
    "interception": "interceptions",
    "duel_success": "duels_won",
    "aerial_duel_success": "aerial_won",
    "foul": "fouls",
    "foul_taken": "fouls_drawn",
    "offside": "offsides",
    "yellow_card": "yellow_cards",
    "red_card": "red_cards",
    "xg": "xg",
    "time_on_field_total": "minutes_played",
    "pass_ratio": "pass_accuracy",
    # Goalkeeper stats
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

        # 3. Fetch team names via PlayerTeam → Team
        team_by_player: dict[int, Team] = {}
        if player_ids:
            result = await self.db.execute(
                select(PlayerTeam)
                .options(selectinload(PlayerTeam.team))
                .where(
                    PlayerTeam.player_id.in_(player_ids),
                    PlayerTeam.season_id == season_id,
                    PlayerTeam.is_active == True,
                )
            )
            for pt in result.scalars().all():
                team_by_player[pt.player_id] = pt.team

        # 4. Build cache
        cache: dict[str, dict[str, Any]] = {}
        for sota_id, player in players.items():
            pss = stats_by_player.get(player.id)
            # photo_url is stored as object name, resolve to full URL
            photo = resolve_file_url(player.photo_url) if player.photo_url else None
            team = team_by_player.get(player.id)

            cache[sota_id] = {
                "person_id": player.id,
                "photo": photo,
                "stats": self._extract_stats(pss, player),
                # Localized names for enrichment
                "first_name": player.first_name,
                "first_name_kz": player.first_name_kz,
                "last_name": player.last_name,
                "last_name_kz": player.last_name_kz,
                "team_name": team.name if team else None,
                "team_name_kz": team.name_kz if team else None,
            }
        return cache

    async def _build_tour_enrichment_cache(
        self, season_id: int, sota_season_id: int, tour_number: int, sota_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Build enrichment cache by fetching stats directly from SOTA v2.

        Uses SOTA v2 /players/{id}/game_stats/?season_id=&tour= for each player.
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

        # 2. Fetch team names via PlayerTeam → Team
        player_ids = [p.id for p in players.values()]
        team_by_player: dict[int, Team] = {}
        if player_ids:
            result = await self.db.execute(
                select(PlayerTeam)
                .options(selectinload(PlayerTeam.team))
                .where(
                    PlayerTeam.player_id.in_(player_ids),
                    PlayerTeam.season_id == season_id,
                    PlayerTeam.is_active == True,
                )
            )
            for pt in result.scalars().all():
                team_by_player[pt.player_id] = pt.team

        # 3. Call SOTA v2 for each player using tour param (no DB lookups needed)
        v2_stats_by_player: dict[int, dict] = {}
        for player in players.values():
            if not player.sota_id:
                continue
            try:
                v2_data = await self.client.get_player_game_stats_v2_by_tour(
                    str(player.sota_id), sota_season_id, tour_number
                )
                v2_stats_by_player[player.id] = v2_data
            except Exception as e:
                logger.warning(f"v2 stats failed for player {player.id}: {e}")
            await asyncio.sleep(0.15)

        # 4. Build cache using v2 data
        cache: dict[str, dict[str, Any]] = {}
        for sota_id, player in players.items():
            photo = resolve_file_url(player.photo_url) if player.photo_url else None
            v2_data = v2_stats_by_player.get(player.id, {})
            team = team_by_player.get(player.id)

            cache[sota_id] = {
                "person_id": player.id,
                "photo": photo,
                "stats": self._map_v2_stats(v2_data),
                "first_name": player.first_name,
                "first_name_kz": player.first_name_kz,
                "last_name": player.last_name,
                "last_name_kz": player.last_name_kz,
                "team_name": team.name if team else None,
                "team_name_kz": team.name_kz if team else None,
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

        # Extract both field and GK stats for all players
        stats: dict[str, Any] = {}
        for db_col, fe_key in FIELD_STATS_MAP.items():
            val = getattr(pss, db_col, None)
            if val is not None:
                stats[fe_key] = val
        for db_col, fe_key in GK_STATS_MAP.items():
            val = getattr(pss, db_col, None)
            if val is not None:
                stats[fe_key] = val
        return stats

    @staticmethod
    def _map_v2_stats(v2_data: dict) -> dict[str, Any]:
        """Map SOTA v2 game_stats response keys to frontend stat keys."""
        stats: dict[str, Any] = {}
        for v2_key, fe_key in V2_TO_FRONTEND.items():
            val = v2_data.get(v2_key)
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
            team = dict(p.get("team") or {})
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

            # Enrich team name with localized version from local DB
            if enrich:
                suffix = f"_{locale}" if locale != "ru" else ""
                local_team_name = enrich.get(f"team_name{suffix}") or enrich.get("team_name")
                if local_team_name:
                    team["name"] = local_team_name

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
                    season_id, sota_season_id, tour_num, list(ids),
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
            scheme, payload = self._map_sota_response(
                sota_data, enrichment, locale=locale, ru_names=ru_names,
            )

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
