from app.models import GameEvent, GameEventType
from app.utils.game_event_assists import sync_event_assist


def test_sync_event_assist_updates_non_overridden_goal():
    event = GameEvent(
        game_id=1,
        half=1,
        minute=15,
        event_type=GameEventType.goal,
        assist_manual_override=False,
    )

    sync_event_assist(
        event,
        {"player_id": 77, "player_name": "Manual Free Source"},
    )

    assert event.assist_player_id == 77
    assert event.assist_player_name == "Manual Free Source"
    assert event.assist_manual_override is False


def test_sync_event_assist_preserves_manual_override():
    event = GameEvent(
        game_id=1,
        half=1,
        minute=15,
        event_type=GameEventType.goal,
        assist_player_id=10,
        assist_player_name="Saved Assist",
        assist_manual_override=True,
    )

    sync_event_assist(
        event,
        {"player_id": 99, "player_name": "Feed Assist"},
    )

    assert event.assist_player_id == 10
    assert event.assist_player_name == "Saved Assist"
    assert event.assist_manual_override is True


def test_sync_event_assist_clears_override_for_non_goal_events():
    event = GameEvent(
        game_id=1,
        half=1,
        minute=15,
        event_type=GameEventType.yellow_card,
        assist_player_id=10,
        assist_player_name="Should Clear",
        assist_manual_override=True,
    )

    sync_event_assist(event, {"player_id": 99, "player_name": "Feed Assist"})

    assert event.assist_player_id is None
    assert event.assist_player_name is None
    assert event.assist_manual_override is False
