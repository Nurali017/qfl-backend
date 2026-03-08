"""
Reference data sync service.

Handles verification of season mappings against SOTA API.
Teams and logos are managed locally — no longer synced from SOTA.
"""
import logging

from sqlalchemy import select

from app.models import Season, Championship
from app.services.sync.base import BaseSyncService

logger = logging.getLogger(__name__)


class ReferenceSyncService(BaseSyncService):
    """
    Service for verifying reference data against SOTA.

    Only sync_seasons remains as a read-only verification of SOTA mapping.
    Teams and logos are managed locally.
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
