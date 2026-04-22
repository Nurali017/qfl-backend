from datetime import date
from uuid import uuid4

import pytest

from app.api.admin.games import update_event
from app.models import GameEvent, GameEventType, Player
from app.schemas.admin.games import AdminEventUpdateRequest


@pytest.mark.asyncio
async def test_update_event_can_set_and_clear_assist(test_session, sample_game):
    scorer = Player(
        sota_id=uuid4(),
        first_name="Ivan",
        last_name="Scorer",
        birthday=date(1997, 5, 10),
        player_type="forward",
        top_role="CF",
    )
    assister = Player(
        sota_id=uuid4(),
        first_name="Pavel",
        last_name="Assistant",
        birthday=date(1998, 7, 12),
        player_type="halfback",
        top_role="AM",
    )
    event = GameEvent(
        game_id=sample_game.id,
        half=1,
        minute=18,
        event_type=GameEventType.goal,
        team_id=sample_game.home_team_id,
        player_id=None,
        player_name="Ivan Scorer",
    )

    test_session.add_all([scorer, assister, event])
    await test_session.commit()
    await test_session.refresh(event)

    updated = await update_event(
        sample_game.id,
        event.id,
        AdminEventUpdateRequest(
            assist_player_id=assister.id,
            assist_player_name="Pavel Assistant",
        ),
        test_session,
    )

    assert updated.assist_player_id == assister.id
    assert updated.assist_player_name == "Pavel Assistant"
    assert event.assist_manual_override is True

    cleared = await update_event(
        sample_game.id,
        event.id,
        AdminEventUpdateRequest(
            assist_player_id=None,
            assist_player_name=None,
        ),
        test_session,
    )

    assert cleared.assist_player_id is None
    assert cleared.assist_player_name is None

    await test_session.refresh(event)
    assert event.assist_player_id is None
    assert event.assist_player_name is None
    assert event.assist_manual_override is True
