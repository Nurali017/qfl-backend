"""Game grouping and player name fallback utilities."""

from collections import defaultdict
from datetime import date as date_type

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Game, GameEvent
from app.utils.date_helpers import format_match_date
from app.utils.game_status import compute_game_status


async def get_player_names_fallback(
    db: AsyncSession,
    game_id: int,
    player_ids: list[int]
) -> dict[int, tuple[str | None, str | None]]:
    """
    Get player names with fallback to GameEvent.player_name.

    If Player.first_name or Player.last_name is NULL,
    attempts to get name from GameEvent.player_name.

    Returns: dict {player_id: (first_name, last_name)}
    """
    # Get names from GameEvent for all players in one query
    result = await db.execute(
        select(GameEvent.player_id, GameEvent.player_name)
        .where(
            GameEvent.game_id == game_id,
            GameEvent.player_id.in_(player_ids),
            GameEvent.player_name.isnot(None)
        )
        .distinct(GameEvent.player_id)
    )

    # Create lookup dict with name parsing
    event_names = {}
    for row in result:
        if row.player_name:
            # Parse "First Last" format
            parts = row.player_name.strip().split(maxsplit=1)
            first = parts[0] if len(parts) > 0 else None
            last = parts[1] if len(parts) > 1 else None
            event_names[row.player_id] = (first, last)

    return event_names


def group_games_by_date(
    games: list[Game],
    lang: str = "ru",
    today: date_type | None = None
) -> list[dict]:
    """
    Group games by date with formatted labels.

    Args:
        games: List of Game objects to group
        lang: Language for date formatting (kz, ru, en)
        today: Current date for status computation (defaults to today)

    Returns:
        List of dicts with date, date_label, and games
    """
    if today is None:
        today = date_type.today()

    grouped = defaultdict(list)

    for game in games:
        if game.date:
            # Build game dict with status
            game_dict = {
                "id": game.id,
                "date": game.date,
                "time": game.time,
                "tour": game.tour,
                "season_id": game.season_id,
                "stage_id": game.stage_id,
                "home_score": game.home_score,
                "away_score": game.away_score,
                "home_penalty_score": game.home_penalty_score,
                "away_penalty_score": game.away_penalty_score,
                "is_live": game.is_live,
                "has_stats": game.has_stats,
                "has_lineup": game.has_lineup,
                "is_technical": game.is_technical,
                "is_schedule_tentative": game.is_schedule_tentative,
                "visitors": game.visitors,
                "status": compute_game_status(game, today),
                "has_score": game.home_score is not None and game.away_score is not None,
                "ticket_url": getattr(game, "ticket_url", None),
                "video_url": game.video_url,
                "protocol_url": game.protocol_url,
                # Teams and stadium will be added by caller
                "game_obj": game,  # Keep reference for relationship access
            }
            grouped[game.date].append(game_dict)

    result = []
    for game_date in sorted(grouped.keys()):
        date_label = format_match_date(game_date, lang)
        result.append({
            "date": game_date,
            "date_label": date_label,
            "games": grouped[game_date]
        })

    return result
