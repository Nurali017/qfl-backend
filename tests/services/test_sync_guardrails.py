from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models import Championship, Player, PlayerTeam, Season, Team
from app.services.sync.reference_sync import ReferenceSyncService
import app.services.sync.reference_sync as reference_sync_module


class DummyReferenceClient:
    def __init__(self, seasons_by_language: dict[str, list[dict]]):
        self.seasons_by_language = seasons_by_language

    async def get_seasons(self, language: str = "ru") -> list[dict]:
        return self.seasons_by_language[language]


@pytest.mark.asyncio
async def test_reference_sync_verifies_season_mapping(test_session):
    championship = Championship(id=1, name="Premier League")
    season = Season(
        id=200,
        name="Премьер-Лига 2026",
        name_kz="Премьер-Лига 2026",
        name_en="Premier League 2026",
        championship_id=1,
        date_start=date(2026, 3, 7),
        date_end=date(2026, 11, 30),
        sota_season_id=173,
        sync_enabled=True,
    )
    test_session.add_all([championship, season])
    await test_session.commit()

    client = DummyReferenceClient(
        seasons_by_language={
            "ru": [{"id": 173, "name": "2026", "start_date": "2026-03-07", "end_date": "2026-11-30"}],
        },
    )

    service = ReferenceSyncService(test_session, client)

    assert await service.sync_seasons() == 1

    await test_session.refresh(season)

    # Local fields are preserved (not overwritten by SOTA)
    assert season.name == "Премьер-Лига 2026"
    assert season.name_kz == "Премьер-Лига 2026"
    assert season.name_en == "Premier League 2026"
    assert season.date_start == date(2026, 3, 7)
    assert season.date_end == date(2026, 11, 30)
