import pytest
from datetime import date, timedelta

from httpx import AsyncClient

from app.models import Championship, Season, Tournament


@pytest.mark.asyncio
class TestChampionshipsFrontMapAPI:
    async def test_get_front_map_empty(self, client: AsyncClient):
        response = await client.get('/api/v1/championships/front-map')
        assert response.status_code == 200
        data = response.json()

        assert set(data['items'].keys()) == {'pl', '1l', 'cup', '2l', 'el'}
        assert data['items']['pl']['season_id'] is None
        assert data['items']['2l']['season_id'] is None

    async def test_get_front_map_resolves_current_and_second_league_stages(
        self,
        client: AsyncClient,
        test_session,
    ):
        today = date.today()

        championships = [
            Championship(id=101, name='Premier League', slug='premier-league', is_active=True),
            Championship(id=102, name='First League', slug='first-league', is_active=True),
            Championship(id=103, name='Kazakhstan Cup', slug='cup', is_active=True),
            Championship(id=104, name='Second League', slug='second-league', is_active=True),
            Championship(id=105, name='Women League', slug='women-league', is_active=True),
        ]
        test_session.add_all(championships)

        tournaments = [
            Tournament(id=201, name='Premier', championship_id=101),
            Tournament(id=202, name='First', championship_id=102),
            Tournament(id=203, name='Cup', championship_id=103),
            Tournament(id=204, name='Second', championship_id=104),
            Tournament(id=205, name='Women', championship_id=105),
        ]
        test_session.add_all(tournaments)

        seasons = [
            Season(
                id=61,
                name='PL 2026',
                tournament_id=201,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
            ),
            Season(
                id=85,
                name='1L 2026',
                tournament_id=202,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
            ),
            Season(
                id=71,
                name='Cup 2026',
                tournament_id=203,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
            ),
            Season(
                id=84,
                name='Women 2026',
                tournament_id=205,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
            ),
            Season(
                id=80,
                name='Second League Group A',
                tournament_id=204,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
            ),
            Season(
                id=81,
                name='Second League Group B',
                tournament_id=204,
                date_start=today - timedelta(days=120),
                date_end=today - timedelta(days=60),
            ),
            Season(
                id=157,
                name='Second League Final',
                tournament_id=204,
                date_start=today + timedelta(days=31),
                date_end=today + timedelta(days=120),
            ),
        ]
        test_session.add_all(seasons)
        await test_session.commit()

        response = await client.get('/api/v1/championships/front-map')
        assert response.status_code == 200
        data = response.json()['items']

        assert data['pl']['season_id'] == 61
        assert data['1l']['season_id'] == 85
        assert data['cup']['season_id'] == 71
        assert data['el']['season_id'] == 84

        assert data['2l']['season_id'] == 80
        assert data['2l']['stages'] == {'a': 80, 'b': 81, 'final': 157}
