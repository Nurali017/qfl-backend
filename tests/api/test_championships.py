import pytest
from datetime import date, timedelta

from httpx import AsyncClient

from app.models import Championship, Season


@pytest.mark.asyncio
class TestChampionshipsFrontMapAPI:
    async def test_get_front_map_empty(self, client: AsyncClient):
        response = await client.get('/api/v1/championships/front-map')
        assert response.status_code == 200
        data = response.json()

        # No seasons with frontend_code set → empty items
        assert data['items'] == {}

    async def test_get_front_map_resolves_from_frontend_code(
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

        seasons = [
            Season(
                id=61,
                name='PL 2026',
                championship_id=101,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
                frontend_code='pl',
                tournament_type='league',
                tournament_format='round_robin',
                has_table=True,
                sort_order=1,
            ),
            Season(
                id=85,
                name='1L 2026',
                championship_id=102,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
                frontend_code='1l',
                tournament_type='league',
                tournament_format='round_robin',
                has_table=True,
                sort_order=2,
            ),
            Season(
                id=71,
                name='Cup 2026',
                championship_id=103,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
                frontend_code='cup',
                tournament_type='cup',
                tournament_format='knockout',
                has_bracket=True,
                sort_order=3,
            ),
            Season(
                id=84,
                name='Women 2026',
                championship_id=105,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
                frontend_code='el',
                tournament_type='league',
                tournament_format='round_robin',
                has_table=True,
                sort_order=5,
            ),
            Season(
                id=80,
                name='Second League 2026',
                championship_id=104,
                date_start=today - timedelta(days=30),
                date_end=today + timedelta(days=30),
                frontend_code='2l',
                tournament_type='league',
                tournament_format='round_robin',
                has_table=True,
                final_stage_ids=[301, 302],
                sort_order=4,
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

        # New fields present
        assert data['pl']['has_table'] is True
        assert data['cup']['has_bracket'] is True
        assert data['2l']['tournament_type'] == 'league'
        assert data['2l']['final_stage_ids'] == [301, 302]
