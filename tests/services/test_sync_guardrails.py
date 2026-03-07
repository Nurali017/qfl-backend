from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models import Championship, Player, PlayerTeam, Season, Team
from app.services.sync.player_sync import PlayerSyncService
from app.services.sync.reference_sync import ReferenceSyncService
from app.utils.file_urls import to_object_name
import app.services.sync.player_sync as player_sync_module
import app.services.sync.reference_sync as reference_sync_module


class DummyReferenceClient:
    def __init__(self, seasons_by_language: dict[str, list[dict]], teams_by_language: dict[str, list[dict]]):
        self.seasons_by_language = seasons_by_language
        self.teams_by_language = teams_by_language

    async def get_seasons(self, language: str = "ru") -> list[dict]:
        return self.seasons_by_language[language]

    async def get_teams(self, language: str = "ru") -> list[dict]:
        return self.teams_by_language[language]


class DummyPlayerClient:
    def __init__(self, players_by_language: dict[str, list[dict]]):
        self.players_by_language = players_by_language
        self.requested_season_ids: list[int] = []

    async def get_players(self, season_id: int, language: str = "ru") -> list[dict]:
        self.requested_season_ids.append(season_id)
        return self.players_by_language[language]


@pytest.mark.asyncio
async def test_reference_sync_preserves_local_season_fields_and_team_metadata(test_session, monkeypatch):
    monkeypatch.setattr(reference_sync_module, "insert", sqlite_insert)

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
    team = Team(
        id=91,
        name="Астана",
        name_kz="Астана",
        name_en="Astana",
        logo_url="teams/astana/logo.png",
        city="Астана",
        city_kz="Астана",
        city_en="Astana",
    )
    test_session.add_all([championship, season, team])
    await test_session.commit()

    client = DummyReferenceClient(
        seasons_by_language={
            "ru": [{"id": 173, "name": "2026", "start_date": "2026-03-07", "end_date": "2026-11-30"}],
        },
        teams_by_language={
            "ru": [{"id": 91, "name": "ФК Астана"}],
            "kk": [{"id": 91, "name": "Астана ФК"}],
            "en": [{"id": 91, "name": "FC Astana"}],
        },
    )

    service = ReferenceSyncService(test_session, client)

    assert await service.sync_seasons() == 1
    assert await service.sync_teams() == 1

    await test_session.refresh(season)
    await test_session.refresh(team)

    assert season.name == "Премьер-Лига 2026"
    assert season.name_kz == "Премьер-Лига 2026"
    assert season.name_en == "Premier League 2026"
    assert season.date_start == date(2026, 3, 7)
    assert season.date_end == date(2026, 11, 30)

    assert team.name == "ФК Астана"
    assert team.name_kz == "Астана ФК"
    assert team.name_en == "FC Astana"
    assert to_object_name(team.logo_url) == "teams/astana/logo.png"
    assert team.city == "Астана"
    assert team.city_kz == "Астана"
    assert team.city_en == "Astana"


@pytest.mark.asyncio
async def test_player_sync_ignores_team_payload_and_preserves_local_player_teams(test_session, monkeypatch):
    monkeypatch.setattr(player_sync_module, "insert", sqlite_insert)

    championship = Championship(id=1, name="Premier League")
    season = Season(
        id=200,
        name="Премьер-Лига 2026",
        championship_id=1,
        sota_season_id=173,
        sync_enabled=True,
    )
    home_team = Team(id=91, name="Astana")
    remote_team = Team(id=13, name="Kairat")
    player_sota_id = uuid4()
    player = Player(
        sota_id=player_sota_id,
        first_name="Local",
        last_name="Player",
        birthday=date(1995, 1, 15),
        player_type="halfback",
        top_role="AM",
        top_role_en="Attacking midfielder",
    )
    test_session.add_all([championship, season, home_team, remote_team, player])
    await test_session.commit()
    await test_session.refresh(player)

    player_team = PlayerTeam(
        player_id=player.id,
        team_id=home_team.id,
        season_id=season.id,
        number=8,
    )
    test_session.add(player_team)
    await test_session.commit()

    payload = {
        "id": str(player_sota_id),
        "first_name": "Remote",
        "last_name": "Player",
        "birthday": "1995-01-15",
        "type": "halfback",
        "top_role": "CF",
        "team": {"id": remote_team.id, "name": "Ignore Me"},
        "teams": [remote_team.id],
    }
    client = DummyPlayerClient(
        players_by_language={
            "ru": [payload],
            "kk": [],
            "en": [],
        }
    )

    service = PlayerSyncService(test_session, client)

    assert await service.sync_players(season.id) == 1
    assert client.requested_season_ids == [173]

    await test_session.refresh(player)
    assert player.first_name == "Local"
    assert player.last_name == "Player"
    assert player.birthday == date(1995, 1, 15)
    assert player.player_type == "halfback"
    assert player.top_role == "CF"
    assert player.top_role_en == "Attacking midfielder"

    player_teams = list((await test_session.execute(select(PlayerTeam))).scalars())
    assert len(player_teams) == 1
    assert player_teams[0].player_id == player.id
    assert player_teams[0].team_id == home_team.id
    assert player_teams[0].season_id == season.id
    assert player_teams[0].number == 8
