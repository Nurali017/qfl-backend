"""
Reference data sync service.

Handles synchronization of tournaments, seasons, and teams from SOTA API.
"""
import logging
import re
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Tournament, Season, Team
from app.services.file_storage import FileStorageService
from app.services.sync.base import BaseSyncService, parse_date
from app.utils.file_urls import to_object_name

logger = logging.getLogger(__name__)


class ReferenceSyncService(BaseSyncService):
    """
    Service for syncing reference data: tournaments, seasons, teams.

    These are the foundational entities that other data depends on.
    """

    async def sync_tournaments(self) -> int:
        """
        Sync tournaments from SOTA API with all 3 languages.

        Returns:
            Number of tournaments synced
        """
        # Fetch data in all 3 languages
        tournaments_ru = await self.client.get_tournaments(language="ru")
        tournaments_kz = await self.client.get_tournaments(language="kk")
        tournaments_en = await self.client.get_tournaments(language="en")

        # Build lookup dicts
        kz_by_id = {t["id"]: t for t in tournaments_kz}
        en_by_id = {t["id"]: t for t in tournaments_en}

        count = 0
        for t in tournaments_ru:
            t_id = t["id"]
            t_kz = kz_by_id.get(t_id, {})
            t_en = en_by_id.get(t_id, {})

            stmt = insert(Tournament).values(
                id=t_id,
                name=t["name"],  # Russian as default
                name_kz=t_kz.get("name"),
                name_en=t_en.get("name"),
                country_code=t.get("country_code"),
                country_name=t.get("country_name"),  # Russian as default
                country_name_kz=t_kz.get("country_name"),
                country_name_en=t_en.get("country_name"),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "name": stmt.excluded.name,
                    "name_kz": stmt.excluded.name_kz,
                    "name_en": stmt.excluded.name_en,
                    "country_code": stmt.excluded.country_code,
                    "country_name": stmt.excluded.country_name,
                    "country_name_kz": stmt.excluded.country_name_kz,
                    "country_name_en": stmt.excluded.country_name_en,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        logger.info(f"Synced {count} tournaments")
        return count

    async def sync_seasons(self) -> int:
        """
        Sync seasons from SOTA API with all 3 languages.

        Returns:
            Number of seasons synced
        """
        # Fetch data in all 3 languages
        seasons_ru = await self.client.get_seasons(language="ru")
        seasons_kz = await self.client.get_seasons(language="kk")
        seasons_en = await self.client.get_seasons(language="en")

        # Build lookup dicts
        kz_by_id = {s["id"]: s for s in seasons_kz}
        en_by_id = {s["id"]: s for s in seasons_en}

        count = 0
        for s in seasons_ru:
            s_id = s["id"]
            s_kz = kz_by_id.get(s_id, {})
            s_en = en_by_id.get(s_id, {})

            stmt = insert(Season).values(
                id=s_id,
                name=s["name"],  # Russian as default
                name_kz=s_kz.get("name"),
                name_en=s_en.get("name"),
                tournament_id=s.get("tournament_id"),
                date_start=parse_date(s.get("date_start")),
                date_end=parse_date(s.get("date_end")),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "name": stmt.excluded.name,
                    "name_kz": stmt.excluded.name_kz,
                    "name_en": stmt.excluded.name_en,
                    "tournament_id": stmt.excluded.tournament_id,
                    "date_start": stmt.excluded.date_start,
                    "date_end": stmt.excluded.date_end,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        logger.info(f"Synced {count} seasons")
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
                logo_url=t.get("logo"),
                logo_updated_at=datetime.utcnow() if t.get("logo") else None,
                city=t.get("city"),  # Russian as default
                city_kz=t_kz.get("city"),
                city_en=t_en.get("city"),
                updated_at=datetime.utcnow(),
            )
            update_dict = {
                "name": stmt.excluded.name,
                "name_kz": stmt.excluded.name_kz,
                "name_en": stmt.excluded.name_en,
                "city": stmt.excluded.city,
                "city_kz": stmt.excluded.city_kz,
                "city_en": stmt.excluded.city_en,
                "updated_at": stmt.excluded.updated_at,
            }

            if t.get("logo"):
                update_dict["logo_url"] = stmt.excluded.logo_url
                update_dict["logo_updated_at"] = stmt.excluded.logo_updated_at

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
            # Keep in sync with FileStorageService.upload_team_logo() naming:
            # - lowercase
            # - spaces -> hyphens
            # - strip common suffixes for reserve/youth teams
            #
            # NOTE: SOTA / legacy sources sometimes use Cyrillic "М" for reserve teams.
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
            "tournaments": await self.sync_tournaments(),
            "seasons": await self.sync_seasons(),
            "teams": await self.sync_teams(),
        }
        return results
