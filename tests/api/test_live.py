import pytest
from httpx import AsyncClient

from app.models import GameEvent, GameEventType, Player


@pytest.mark.asyncio
async def test_get_live_events_returns_numeric_player_ids(
    client: AsyncClient,
    test_session,
    sample_game,
    sample_player,
):
    assist_player = Player(first_name="Assist", last_name="Player")
    test_session.add(assist_player)
    await test_session.flush()

    event = GameEvent(
        game_id=sample_game.id,
        half=1,
        minute=12,
        event_type=GameEventType.goal,
        team_id=sample_game.home_team_id,
        player_id=sample_player.id,
        player_name="Test Player",
        player2_id=assist_player.id,
        player2_name="Assist Player",
        assist_player_id=assist_player.id,
        assist_player_name="Assist Player",
    )
    test_session.add(event)
    await test_session.commit()

    response = await client.get(f"/api/v1/live/events/{sample_game.id}")
    assert response.status_code == 200

    payload = response.json()
    assert payload["total"] == 1
    assert isinstance(payload["events"][0]["player_id"], int)
    assert payload["events"][0]["player_id"] == sample_player.id
    assert isinstance(payload["events"][0]["player2_id"], int)
    assert payload["events"][0]["player2_id"] == assist_player.id
    assert isinstance(payload["events"][0]["assist_player_id"], int)
    assert payload["events"][0]["assist_player_id"] == assist_player.id
