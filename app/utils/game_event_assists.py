from typing import Any

from app.models.game_event import GameEvent, GameEventType


ASSIST_EVENT_TYPES = {
    GameEventType.goal,
    GameEventType.penalty,
}


def is_assist_supported_event_type(event_type: GameEventType | str | None) -> bool:
    if event_type is None:
        return False
    if isinstance(event_type, str):
        try:
            event_type = GameEventType(event_type)
        except ValueError:
            return False
    return event_type in ASSIST_EVENT_TYPES


def sync_event_assist(
    event: GameEvent,
    assist_info: dict[str, Any] | None,
) -> None:
    """Apply assist data from an external feed while honoring manual overrides."""
    if not is_assist_supported_event_type(event.event_type):
        event.assist_player_id = None
        event.assist_player_name = None
        event.assist_manual_override = False
        return

    if event.assist_manual_override:
        return

    event.assist_player_id = assist_info["player_id"] if assist_info else None
    event.assist_player_name = assist_info["player_name"] if assist_info else None
