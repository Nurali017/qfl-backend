"""
Reference data sync service.

Handles synchronization of seasons and teams from SOTA API.
"""
import logging
import re
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Season, Team, Championship
from app.services.file_storage import FileStorageService
from app.services.sync.base import BaseSyncService
from app.utils.file_urls import to_object_name

logger = logging.getLogger(__name__)


class ReferenceSyncService(BaseSyncService):
    """
    Service for syncing reference data: seasons, teams.

    These are the foundational entities that other data depends on.
    """

    async def _build_sota_tournament_to_championship_map(self) -> dict[int, int]:
        """
        Build a mapping from SOTA tournament_id to local championship.id.

        Uses Championship.sota_ids field which stores SOTA tournament IDs
        as semicolon-separated values (e.g. "7" or "74;75;139").
        """
        result = await self.db.execute(
            select(Championship).where(Championship.sota_ids.isnot(None))
        )
        championships = result.scalars().all()

        mapping: dict[int, int] = {}
        for champ in championships:
            for raw_id in champ.sota_ids.split(";"):
                raw_id = raw_id.strip()
                if raw_id.isdigit():
                    mapping[int(raw_id)] = champ.id
        return mapping

    async def sync_seasons(self) -> int:
        """
        Check mapped seasons against SOTA API without overwriting local reference data.

        Only updates seasons that have a sota_season_id mapping and sync_enabled=True.
        Does NOT create new seasons — seasons are managed manually via admin/migrations.

        Returns:
            Number of seasons confirmed in SOTA
        """
        # Fetch Russian data only; local season labels and dates are authoritative.
        seasons_ru = await self.client.get_seasons(language="ru")

        # Build lookup dict by SOTA season id
        ru_by_id = {s["id"]: s for s in seasons_ru}

        # Find all local seasons that have SOTA mapping and sync enabled
        result = await self.db.execute(
            select(Season).where(
                Season.sota_season_id.isnot(None),
                Season.sync_enabled == True,
            )
        )
        local_seasons = result.scalars().all()

        count = 0
        for local in local_seasons:
            sota_id = local.sota_season_id
            s_ru = ru_by_id.get(sota_id)
            if s_ru is None:
                logger.warning(
                    "Season %d (sota_season_id=%d): not found in SOTA API, skipping",
                    local.id, sota_id,
                )
                continue

            # Names and dates are managed locally; we only verify the season still exists in SOTA.
            count += 1

        await self.db.commit()
        logger.info(f"Confirmed {count} mapped seasons in SOTA")
        return count

    async def sync_teams(self) -> int:
        """
        Sync teams from SOTA API with all 3 languages.

        Returns:
            Number of teams synced
        """
        # Fetch data in all 3 languages
        teams_ru = await self.client.get_teams(language="ru")
        teams_kz = await self.client.get_teams(language="kk")
        teams_en = await self.client.get_teams(language="en")

        # Build lookup dicts by team id
        kz_by_id = {t["id"]: t for t in teams_kz}
        en_by_id = {t["id"]: t for t in teams_en}

        count = 0
        for t in teams_ru:
            team_id = t["id"]
            t_kz = kz_by_id.get(team_id, {})
            t_en = en_by_id.get(team_id, {})

            stmt = insert(Team).values(
                id=team_id,
                name=t["name"],  # Russian as default
                name_kz=t_kz.get("name"),
                name_en=t_en.get("name"),
                updated_at=datetime.utcnow(),
            )
            update_dict = {
                "name": stmt.excluded.name,
                "name_kz": stmt.excluded.name_kz,
                "name_en": stmt.excluded.name_en,
                "updated_at": stmt.excluded.updated_at,
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_=update_dict,
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        logger.info(f"Synced {count} teams")
        return count

    async def sync_team_logos(self) -> int:
        """
        Sync team logos from MinIO storage to database.

        Returns:
            Number of logos updated
        """
        # Mapping for team name normalization (team name -> logo name in MinIO)
        LOGO_NAME_MAP = {
            "jenis": "zhenis",
            "kairat": "kayrat",
            "ulytau": "ulytai",
            "atyrau": "atyrai",
            "elimai": "elimai",
        }

        def normalize_name(name: str) -> str:
            """Normalize team name for matching."""
            name = name.strip()
            name = re.sub(r"\s*[-]?\s*(M|М|W|Zhastar|Жастар)$", "", name, flags=re.IGNORECASE)
            name = re.sub(r"\s+", "-", name.lower()).strip("-")
            return name

        # Get all logos from MinIO
        logos = await FileStorageService.list_team_logos()
        logo_map = {logo["team_name"].lower(): logo["object_name"] for logo in logos}

        # Get all teams from DB
        result = await self.db.execute(select(Team))
        teams = result.scalars().all()

        count = 0
        for team in teams:
            normalized = normalize_name(team.name)
            # Check if we need to map the name
            mapped_name = LOGO_NAME_MAP.get(normalized, normalized)

            logo_url = logo_map.get(mapped_name)
            # team.logo_url returns resolved full URL via FileUrlType;
            # compare against object_name to avoid unnecessary updates
            if logo_url and logo_url != to_object_name(team.logo_url):
                team.logo_url = logo_url
                team.logo_updated_at = datetime.utcnow()
                count += 1

        await self.db.commit()
        logger.info(f"Updated {count} team logos")
        return count

    async def sync_all(self) -> dict[str, int]:
        """
        Sync all reference data in the correct order.

        Returns:
            Dict with counts for each entity type
        """
        results = {
            "seasons": await self.sync_seasons(),
            "teams": await self.sync_teams(),
        }
        return results
