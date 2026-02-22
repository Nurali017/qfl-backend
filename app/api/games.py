import logging
from datetime import date as date_type, datetime, timedelta
from collections import defaultdict
import re
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.models import (
    Game,
    GameTeamStats,
    GamePlayerStats,
    GameEvent,
    GameEventType,
    Team,
    Player,
    GameLineup,
    GameReferee,
    Coach,
    TeamCoach,
    Stadium,
    Referee,
    Season,
    SeasonParticipant,
    Championship,
)
from app.schemas.game import (
    GameResponse,
    GameListResponse,
    GameDetailResponse,
    MatchCenterGame,
    MatchCenterDateGroup,
    MatchCenterResponse,
    StadiumInfo,
    TeamInMatchCenter,
)
from app.schemas.stats import (
    GameStatsResponse,
    GameTeamStatsResponse,
    GamePlayerStatsResponse,
)
from app.schemas.team import TeamInGame
from app.services.sota_client import SotaClient, get_sota_client
from app.services.sync.lineup_sync import LineupSyncService
from app.utils.date_helpers import format_match_date, get_localized_field
from app.utils.numbers import to_finite_float
from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/games", tags=["games"])


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


def compute_game_status(game: Game, today: date_type | None = None) -> str:
    """
    Compute game status based on data.

    Returns:
        "live" - Game is currently in progress
        "finished" - Game has ended
        "upcoming" - Game is scheduled for the future
    """
    if today is None:
        today = date_type.today()

    if game.is_live:
        return "live"
    elif game.home_score is not None and game.away_score is not None:
        return "finished"
    elif game.date and game.date < today:
        return "finished"  # Past game, treat as finished even without score
    else:
        return "upcoming"


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


SUPPORTED_FORMATIONS = {
    "4-4-2", "4-3-3", "4-2-3-1", "3-5-2", "5-3-2", "3-4-3", "4-5-1",
    "4-1-4-1", "4-4-1-1", "3-4-1-2", "5-4-1", "4-3-2-1"
}

VALID_AMPLUA_VALUES = {"Gk", "D", "DM", "M", "AM", "F"}
VALID_FIELD_POSITION_VALUES = {"L", "LC", "C", "RC", "R"}
LINEUP_FIELD_ALLOWED_CHAMPIONSHIPS = {1, 2, 3}
LINEUP_FIELD_CUTOFF_DATE = date_type(2025, 6, 1)
VALID_LINEUP_SOURCES = {"team_squad", "sota_api", "vsporte_api", "matches_players", "none"}

POSITION_CODE_TO_AMPLUA = {
    # Goalkeepers
    "GK": "Gk",
    "G": "Gk",
    "ВР": "Gk",
    "ГК": "Gk",
    "ВРТ": "Gk",
    # Defenders
    "CD": "D",
    "LD": "D",
    "RD": "D",
    "LB": "D",
    "RB": "D",
    "CB": "D",
    "D": "D",
    "ЛЗ": "D",
    "ПЗ": "D",
    "ЦЗ": "D",
    "ЗЩ": "D",
    "ЗАЩ": "D",
    # Defensive mid
    "DM": "DM",
    "ОП": "DM",
    # Midfield
    "CM": "M",
    "M": "M",
    "LM": "M",
    "RM": "M",
    "LW": "M",
    "RW": "M",
    "ЦП": "M",
    "ЛП": "M",
    "ПП": "M",
    # Attacking mid
    "AM": "AM",
    "АП": "AM",
    # Forwards
    "CF": "F",
    "ST": "F",
    "FW": "F",
    "F": "F",
    "ЦН": "F",
    "НП": "F",
    "ЦФ": "F",
    "НАП": "F",
    "ЛН": "F",
    "ПН": "F",
}


def _normalize_stage_ids(raw: object) -> list[int]:
    if not isinstance(raw, list):
        return []

    stage_ids: list[int] = []
    for value in raw:
        try:
            stage_ids.append(int(value))
        except (TypeError, ValueError):
            continue
    return stage_ids


async def get_group_team_ids_for_season(
    db: AsyncSession, season_id: int, group: str
) -> list[int]:
    result = await db.execute(
        select(SeasonParticipant.team_id).where(
            SeasonParticipant.season_id == season_id,
            SeasonParticipant.group_name == group,
        )
    )
    return [row[0] for row in result.all()]


async def get_final_stage_ids_for_season(db: AsyncSession, season_id: int) -> list[int]:
    result = await db.execute(select(Season.final_stage_ids).where(Season.id == season_id))
    row = result.first()
    if row is None:
        return []
    return _normalize_stage_ids(row[0])


def normalize_amplua_value(amplua: str | None) -> str | None:
    if not isinstance(amplua, str):
        return None
    value = amplua.strip()
    return value if value in VALID_AMPLUA_VALUES else None


def normalize_field_position_value(field_position: str | None) -> str | None:
    if not isinstance(field_position, str):
        return None
    value = field_position.strip().upper()
    return value if value in VALID_FIELD_POSITION_VALUES else None


def infer_amplua_from_role_hint(role_hint: str | None) -> str | None:
    if not isinstance(role_hint, str) or not role_hint.strip():
        return None

    normalized = role_hint.strip().upper()
    token = (normalized.split(maxsplit=1)[0] if normalized else "").strip()
    code = re.sub(r"[^A-ZА-ЯЁ0-9]", "", token)

    mapped = POSITION_CODE_TO_AMPLUA.get(code)
    if mapped:
        return mapped

    if "ВРАТ" in normalized:
        return "Gk"
    if "ОПОР" in normalized:
        return "DM"
    if "ПОЛУЗАЩ" in normalized:
        return "M"
    if "ЗАЩИТ" in normalized or "ЗАЩ" in normalized:
        return "D"
    if "НАПАД" in normalized or "НАП" in normalized:
        return "F"

    return None


def infer_field_position_from_role_hint(role_hint: str | None) -> str | None:
    if not isinstance(role_hint, str) or not role_hint.strip():
        return None

    normalized = role_hint.strip().upper()
    token = (normalized.split(maxsplit=1)[0] if normalized else "").strip()
    code = re.sub(r"[^A-ZА-ЯЁ0-9]", "", token)

    if code in {"L", "LB", "LD", "LM", "LW", "ЛЗ", "ЛП", "ЛН"}:
        return "L"
    if code in {"R", "RB", "RD", "RM", "RW", "ПЗ", "ПП", "ПН"}:
        return "R"
    if code in {"LC", "ЛЦ", "ЛЦЗ"}:
        return "LC"
    if code in {"RC", "ПЦ", "ПЦЗ"}:
        return "RC"
    if code in {"C", "CB", "CD", "CM", "CF", "ST", "DM", "AM", "ЦЗ", "ЦП", "ЦН", "ЦФ", "ОП"}:
        return "C"

    has_left = "ЛЕВ" in normalized
    has_right = "ПРАВ" in normalized
    has_center = "ЦЕНТР" in normalized or "CENTER" in normalized
    if has_left and has_center:
        return "LC"
    if has_right and has_center:
        return "RC"
    if has_left:
        return "L"
    if has_right:
        return "R"
    if has_center:
        return "C"

    return None


def resolve_lineup_position_fallback(
    amplua: str | None,
    field_position: str | None,
    role_hint: str | None,
) -> tuple[str | None, str | None]:
    resolved_amplua = normalize_amplua_value(amplua) or infer_amplua_from_role_hint(role_hint)
    resolved_field_position = normalize_field_position_value(field_position) or infer_field_position_from_role_hint(role_hint)

    if resolved_amplua == "Gk" and resolved_field_position is None:
        resolved_field_position = "C"

    if resolved_amplua is not None and resolved_field_position is None:
        resolved_field_position = "C"

    if resolved_amplua is None and resolved_field_position is not None:
        resolved_amplua = "M"

    if resolved_amplua is None and resolved_field_position is None:
        resolved_amplua = "M"
        resolved_field_position = "C"

    return resolved_amplua, resolved_field_position


def normalize_formation(formation: str | None) -> str | None:
    """
    Normalize formation string from SOTA.
    Removes suffixes like ' down', ' up', extra spaces, etc.
    Returns None if formation is invalid.
    """
    if not formation:
        return None

    # Remove common suffixes and clean up
    cleaned = formation.lower().replace(" down", "").replace(" up", "").strip()

    # Extract just the numbers with dashes (e.g., "4-3-3")
    match = re.match(r'^[\d]+-[\d]+(?:-[\d]+)*', cleaned)
    if match:
        return match.group(0)

    return None


def detect_formation(positions: list[str | None]) -> str | None:
    """
    Detect team formation from player positions (amplua).
    Returns formation string like "4-2-3-1", "4-4-2", etc.

    Amplua codes from SOTA: Gk, D, DM, M, AM, F
    """
    if not positions or len(positions) < 10:
        return None

    defenders = 0
    defensive_mids = 0  # DM
    central_mids = 0    # M
    attacking_mids = 0  # AM
    forwards = 0

    for pos in positions[:11]:  # Only first 11 players
        if not pos:
            continue
        pos_upper = pos.upper()

        if pos_upper in ("GK",):
            continue  # Goalkeeper doesn't count
        elif pos_upper == "D":
            defenders += 1
        elif pos_upper == "DM":
            defensive_mids += 1
        elif pos_upper == "M":
            central_mids += 1
        elif pos_upper == "AM":
            attacking_mids += 1
        elif pos_upper == "F":
            forwards += 1

    total_outfield = defenders + defensive_mids + central_mids + attacking_mids + forwards
    if total_outfield < 10:
        return None

    # Determine formation based on composition
    total_mids = defensive_mids + central_mids + attacking_mids

    # 4-X-1 formations (4 defenders, 1 forward, varying midfield)
    if defenders == 4 and forwards == 1 and total_mids == 5:
        return "4-2-3-1"  # Standard modern formation
    elif defenders == 4 and forwards == 2:
        return "4-4-2"
    elif defenders == 4 and forwards == 3:
        return "4-3-3"
    elif defenders == 3 and forwards == 2:
        return "3-5-2"
    elif defenders == 5 and forwards == 2:
        return "5-3-2"
    else:
        # Fallback: simple D-M-F format
        return f"{defenders}-{total_mids}-{forwards}"


def _resolve_championship_gate_id(game: Game) -> int | None:
    season = game.season
    championship = season.championship if season else None
    if championship is None:
        return None
    return championship.legacy_id if championship.legacy_id is not None else championship.id


def _is_field_allowed_by_rules(game: Game) -> bool:
    championship_gate_id = _resolve_championship_gate_id(game)
    if game.date is None:
        return False
    return (
        championship_gate_id in LINEUP_FIELD_ALLOWED_CHAMPIONSHIPS
        and game.date >= LINEUP_FIELD_CUTOFF_DATE
    )


def _team_has_valid_field_data(team_lineup: dict) -> bool:
    starters = team_lineup.get("starters") or []
    if len(starters) < 11:
        return False
    for player in starters[:11]:
        amplua = player.get("amplua")
        field_position = player.get("field_position")
        if amplua not in VALID_AMPLUA_VALUES:
            return False
        if field_position not in VALID_FIELD_POSITION_VALUES:
            return False
    return True


def _has_any_lineup_data(home_lineup: dict, away_lineup: dict) -> bool:
    home_total = len(home_lineup.get("starters", [])) + len(home_lineup.get("substitutes", []))
    away_total = len(away_lineup.get("starters", [])) + len(away_lineup.get("substitutes", []))
    return (home_total + away_total) > 0


def _normalize_lineup_source(raw_source: str | None, has_data: bool) -> str:
    if isinstance(raw_source, str) and raw_source in VALID_LINEUP_SOURCES:
        return raw_source
    if has_data:
        # Backward compatibility for historical data that was synced before source tracking.
        return "matches_players"
    return "none"


@router.get("")
async def get_games(
    season_id: int | None = Query(default=None),
    group: str | None = Query(default=None, description="Filter by group name (e.g. 'A', 'B')"),
    final: bool = Query(default=False, description="Show only final stage matches"),
    team_id: int | None = None,
    team_ids: list[int] | None = Query(default=None),
    tour: int | None = None,
    tours: list[int] | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    year: int | None = None,
    date_from: date_type | None = None,
    date_to: date_type | None = None,
    status: str | None = Query(default=None, pattern="^(upcoming|finished|live|all)$"),
    hide_past: bool = False,
    group_by_date: bool = False,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
):
    """
    Get games with comprehensive filtering and optional date grouping.

    Filters:
    - season_id: Filter by season (defaults to current season)
    - group: Filter by group name using season_participants
    - final: Show only games from season.final_stage_ids
    - team_id: Filter by single team (home or away)
    - team_ids: Filter by multiple teams (home or away) - use ?team_ids=1&team_ids=5
    - tour: Filter by single tour/round number
    - tours: Filter by multiple tours - use ?tours=18&tours=19&tours=20
    - month: Filter by month (1-12, requires year)
    - year: Year for month filter
    - date_from: Start of date range
    - date_to: End of date range
    - status: Filter by match status (upcoming, finished, live, all)
    - hide_past: Hide matches before today
    - group_by_date: Group results by date with formatted labels
    - lang: Language for localized fields (kz, ru, en)

    `group` and `final=true` cannot be used together.
    """
    if season_id is None:
        season_id = settings.current_season_id

    if group and final:
        raise HTTPException(status_code=400, detail="group and final filters are mutually exclusive")

    today = date_type.today()
    group_team_ids: list[int] | None = None
    if group:
        group_team_ids = await get_group_team_ids_for_season(db, season_id, group)
        if not group_team_ids:
            return {"groups": [], "total": 0} if group_by_date else {"items": [], "total": 0}

    final_stage_ids: list[int] | None = None
    if final:
        final_stage_ids = await get_final_stage_ids_for_season(db, season_id)
        if not final_stage_ids:
            return {"groups": [], "total": 0} if group_by_date else {"items": [], "total": 0}

    # Build base query
    query = select(Game).where(Game.season_id == season_id)
    if group_team_ids is not None:
        query = query.where(
            Game.home_team_id.in_(group_team_ids),
            Game.away_team_id.in_(group_team_ids),
        )
    if final_stage_ids is not None:
        query = query.where(Game.stage_id.in_(final_stage_ids))

    # Team filtering
    if team_id:
        query = query.where(
            or_(Game.home_team_id == team_id, Game.away_team_id == team_id)
        )
    elif team_ids:
        query = query.where(
            or_(
                Game.home_team_id.in_(team_ids),
                Game.away_team_id.in_(team_ids)
            )
        )

    # Tour filtering
    if tour is not None:
        query = query.where(Game.tour == tour)
    elif tours:
        query = query.where(Game.tour.in_(tours))

    # Date range filtering
    if month is not None and year is not None:
        # Calculate first and last day of the month
        from calendar import monthrange
        first_day = date_type(year, month, 1)
        last_day_num = monthrange(year, month)[1]
        last_day = date_type(year, month, last_day_num)
        query = query.where(Game.date >= first_day, Game.date <= last_day)
    else:
        if date_from:
            query = query.where(Game.date >= date_from)
        if date_to:
            query = query.where(Game.date <= date_to)

    # Hide past matches
    if hide_past:
        query = query.where(Game.date >= today)

    # Status filtering
    if status and status != "all":
        if status == "upcoming":
            query = query.where(
                or_(
                    Game.date > today,
                    (Game.date == today) & (Game.home_score.is_(None))
                )
            )
        elif status == "finished":
            query = query.where(
                or_(
                    Game.home_score.is_not(None),
                    Game.date < today
                )
            )
        elif status == "live":
            query = query.where(Game.is_live == True)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Get paginated results with eager loading
    query = (
        query.options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
            selectinload(Game.stadium_rel),
            selectinload(Game.stage),
        )
        .order_by(Game.date.asc(), Game.time.asc())
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(query)
    games = result.scalars().all()

    # Build response with localized fields
    def build_team_dict(team: Team | None, score: int | None) -> dict | None:
        if not team:
            return None

        return {
            "id": team.id,
            "name": get_localized_field(team, "name", lang),
            "name_kz": team.name_kz,
            "name_en": team.name_en,
            "logo_url": team.logo_url,
            "primary_color": team.primary_color,
            "secondary_color": team.secondary_color,
            "accent_color": team.accent_color,
        }

    def build_stadium_dict(stadium: Stadium | None) -> dict | None:
        if not stadium:
            return None

        return {
            "id": stadium.id,
            "name": get_localized_field(stadium, "name", lang),
            "city": get_localized_field(stadium, "city", lang),
            "capacity": stadium.capacity,
            "address": get_localized_field(stadium, "address", lang),
            "photo_url": stadium.photo_url,
        }

    # Return grouped format if requested
    if group_by_date:
        grouped = group_games_by_date(games, lang, today)

        # Add team and stadium info to each game
        for group in grouped:
            for game_dict in group["games"]:
                game_obj = game_dict.pop("game_obj")
                game_dict["home_team"] = build_team_dict(game_obj.home_team, game_obj.home_score)
                game_dict["away_team"] = build_team_dict(game_obj.away_team, game_obj.away_score)
                game_dict["stadium"] = build_stadium_dict(game_obj.stadium_rel)

        return {"groups": grouped, "total": total}

    # Standard list format
    items = []
    for g in games:
        game_status = compute_game_status(g, today)

        items.append({
            "id": g.id,
            "date": g.date.isoformat() if g.date else None,
            "time": g.time.isoformat() if g.time else None,
            "tour": g.tour,
            "season_id": g.season_id,
            "stage_id": g.stage_id,
            "stage_name": get_localized_field(g.stage, "name", lang) if g.stage else None,
            "home_score": g.home_score,
            "away_score": g.away_score,
            "home_penalty_score": g.home_penalty_score,
            "away_penalty_score": g.away_penalty_score,
            "has_stats": g.has_stats,
            "has_lineup": g.has_lineup,
            "is_live": g.is_live,
            "is_technical": g.is_technical,
            "stadium": g.stadium,  # Legacy field
            "visitors": g.visitors,
            "status": game_status,
            "has_score": g.home_score is not None and g.away_score is not None,
            "ticket_url": getattr(g, "ticket_url", None),
            "video_url": g.video_url,
            "protocol_url": g.protocol_url,
            "home_team": build_team_dict(g.home_team, g.home_score),
            "away_team": build_team_dict(g.away_team, g.away_score),
            "stadium_info": build_stadium_dict(g.stadium_rel),
            "season_name": get_localized_field(g.season, "name", lang) if g.season else None,
        })

    return {"items": items, "total": total}


@router.get("/{game_id}")
async def get_game(
    game_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
):
    """Get game by ID."""
    result = await db.execute(
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season),
            selectinload(Game.stadium_rel),
            selectinload(Game.stage),
            selectinload(Game.referees).selectinload(GameReferee.referee),
        )
    )
    game = result.scalar_one_or_none()

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    home_team = None
    away_team = None
    if game.home_team:
        home_team = {
            "id": game.home_team.id,
            "name": game.home_team.name,
            "logo_url": game.home_team.logo_url,
            "score": game.home_score,
            "primary_color": game.home_team.primary_color,
            "secondary_color": game.home_team.secondary_color,
            "accent_color": game.home_team.accent_color,
        }
    if game.away_team:
        away_team = {
            "id": game.away_team.id,
            "name": game.away_team.name,
            "logo_url": game.away_team.logo_url,
            "score": game.away_score,
            "primary_color": game.away_team.primary_color,
            "secondary_color": game.away_team.secondary_color,
            "accent_color": game.away_team.accent_color,
        }

    # Build stadium object
    stadium_dict = None
    if game.stadium_rel:
        stadium_dict = {
            "id": game.stadium_rel.id,
            "name": get_localized_field(game.stadium_rel, "name", lang),
            "city": get_localized_field(game.stadium_rel, "city", lang),
            "capacity": game.stadium_rel.capacity,
            "address": get_localized_field(game.stadium_rel, "address", lang),
            "photo_url": game.stadium_rel.photo_url,
        }

    # Get main referee name
    referee_name = None
    if game.referees:
        main_referee = next((gr for gr in game.referees if gr.role.value == "main"), None)
        if main_referee and main_referee.referee:
            ref = main_referee.referee
            if lang == "kz":
                first_name = ref.first_name_kz or ref.first_name
                last_name = ref.last_name_kz or ref.last_name
            elif lang == "en":
                first_name = ref.first_name_en or ref.first_name
                last_name = ref.last_name_en or ref.last_name
            else:
                first_name = ref.first_name
                last_name = ref.last_name
            referee_name = f"{first_name} {last_name}".strip()

    # Compute game status
    today = date_type.today()
    game_status = compute_game_status(game, today)

    return {
        "id": game.id,
        "date": game.date.isoformat() if game.date else None,
        "time": game.time.isoformat() if game.time else None,
        "tour": game.tour,
        "season_id": game.season_id,
        "stage_id": game.stage_id,
        "stage_name": get_localized_field(game.stage, "name", lang) if game.stage else None,
        "home_score": game.home_score,
        "away_score": game.away_score,
        "home_penalty_score": game.home_penalty_score,
        "away_penalty_score": game.away_penalty_score,
        "has_stats": game.has_stats,
        "has_lineup": game.has_lineup,
        "is_live": game.is_live,
        "is_technical": game.is_technical,
        "stadium": stadium_dict,
        "referee": referee_name,
        "visitors": game.visitors,
        "ticket_url": game.ticket_url,
        "video_url": game.video_url,
        "protocol_url": game.protocol_url,
        "status": game_status,
        "has_score": game.home_score is not None and game.away_score is not None,
        "home_team": home_team,
        "away_team": away_team,
        "season_name": game.season.name if game.season else None,
    }


@router.get("/{game_id}/stats")
async def get_game_stats(game_id: int, db: AsyncSession = Depends(get_db)):
    """Get statistics for a game."""
    # Early return for technical wins — no real stats exist
    tech_result = await db.execute(
        select(Game.is_technical).where(Game.id == game_id)
    )
    is_technical = tech_result.scalar()
    if is_technical is None:
        raise HTTPException(status_code=404, detail="Game not found")
    if is_technical:
        return {
            "game_id": game_id,
            "is_technical": True,
            "team_stats": [],
            "player_stats": [],
            "events": [],
        }

    # Get team stats
    team_stats_result = await db.execute(
        select(GameTeamStats)
        .where(GameTeamStats.game_id == game_id)
        .options(selectinload(GameTeamStats.team))
    )
    team_stats = team_stats_result.scalars().all()

    team_stats_response = []
    for ts in team_stats:
        team_stats_response.append({
            "team_id": ts.team_id,
            "team_name": ts.team.name if ts.team else None,
            "logo_url": ts.team.logo_url if ts.team else None,
            "primary_color": ts.team.primary_color if ts.team else None,
            "secondary_color": ts.team.secondary_color if ts.team else None,
            "accent_color": ts.team.accent_color if ts.team else None,
            "possession": to_finite_float(ts.possession),
            "possession_percent": ts.possession_percent,
            "shots": ts.shots,
            "shots_on_goal": ts.shots_on_goal,
            "passes": ts.passes,
            "pass_accuracy": to_finite_float(ts.pass_accuracy),
            "fouls": ts.fouls,
            "yellow_cards": ts.yellow_cards,
            "red_cards": ts.red_cards,
            "corners": ts.corners,
            "offsides": ts.offsides,
            "extra_stats": ts.extra_stats,
        })

    # Get goals and assists from game_events (single source of truth)
    goals_result = await db.execute(
        select(GameEvent.player_id, func.count(GameEvent.id).label("count"))
        .where(GameEvent.game_id == game_id, GameEvent.event_type == GameEventType.goal)
        .group_by(GameEvent.player_id)
    )
    player_goals = {row.player_id: row.count for row in goals_result}

    assists_result = await db.execute(
        select(GameEvent.player_id, func.count(GameEvent.id).label("count"))
        .where(GameEvent.game_id == game_id, GameEvent.event_type == GameEventType.assist)
        .group_by(GameEvent.player_id)
    )
    player_assists = {row.player_id: row.count for row in assists_result}

    # Get player stats
    player_stats_result = await db.execute(
        select(GamePlayerStats)
        .where(GamePlayerStats.game_id == game_id)
        .options(
            selectinload(GamePlayerStats.player).selectinload(Player.country),
            selectinload(GamePlayerStats.team),
        )
        .order_by(GamePlayerStats.team_id, GamePlayerStats.started.desc())
    )
    player_stats = player_stats_result.scalars().all()

    # Get fallback names from GameEvent
    player_ids = [ps.player_id for ps in player_stats]
    fallback_names = await get_player_names_fallback(db, game_id, player_ids)

    player_stats_response = []
    for ps in player_stats:
        # Determine names with fallback
        if ps.player and ps.player.first_name and ps.player.last_name:
            first_name = ps.player.first_name
            last_name = ps.player.last_name
        elif ps.player_id in fallback_names:
            first_name, last_name = fallback_names[ps.player_id]
        else:
            first_name = ps.player.first_name if ps.player else None
            last_name = ps.player.last_name if ps.player else None

        # Build country data
        country_data = None
        if ps.player and ps.player.country:
            country_data = {
                "id": ps.player.country.id,
                "code": ps.player.country.code,
                "name": ps.player.country.name,
                "flag_url": ps.player.country.flag_url,
            }

        player_stats_response.append({
            "player_id": ps.player_id,
            "first_name": first_name,
            "last_name": last_name,
            "country": country_data,
            "team_id": ps.team_id,
            "team_name": ps.team.name if ps.team else None,
            "team_primary_color": ps.team.primary_color if ps.team else None,
            "team_secondary_color": ps.team.secondary_color if ps.team else None,
            "team_accent_color": ps.team.accent_color if ps.team else None,
            "position": ps.position,
            "minutes_played": ps.minutes_played,
            "started": ps.started,
            "goals": player_goals.get(ps.player_id, 0),
            "assists": player_assists.get(ps.player_id, 0),
            "shots": ps.shots,
            "passes": ps.passes,
            "pass_accuracy": to_finite_float(ps.pass_accuracy),
            "yellow_cards": ps.yellow_cards,
            "red_cards": ps.red_cards,
            "extra_stats": ps.extra_stats,
        })

    # Get game events
    events_result = await db.execute(
        select(GameEvent)
        .where(GameEvent.game_id == game_id)
        .order_by(GameEvent.half, GameEvent.minute)
    )
    events = events_result.scalars().all()

    events_response = []
    for e in events:
        events_response.append({
            "id": e.id,
            "half": e.half,
            "minute": e.minute,
            "event_type": e.event_type.value,
            "team_id": e.team_id,
            "team_name": e.team_name,
            "player_id": e.player_id,
            "player_name": e.player_name,
            "player_number": e.player_number,
            "player2_id": e.player2_id,
            "player2_name": e.player2_name,
            "player2_number": e.player2_number,
        })

    return {
        "game_id": game_id,
        "is_technical": False,
        "team_stats": team_stats_response,
        "player_stats": player_stats_response,
        "events": events_response,
    }


@router.get("/{game_id}/lineup")
async def get_game_lineup(
    game_id: int,
    lang: str = Query(default="ru", pattern="^(kz|ru|en)$"),
    db: AsyncSession = Depends(get_db),
    client: SotaClient = Depends(get_sota_client),
):
    """
    Get pre-game lineup data for a game.
    Includes referees, coaches for both teams, and player lineups.
    """
    # Get game to know home/away team IDs
    game_result = await db.execute(
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.season)
            .selectinload(Season.championship),
        )
    )
    game = game_result.scalar_one_or_none()

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    # Early return for technical wins — no lineup data exists
    if game.is_technical:
        return {
            "game_id": game_id,
            "has_lineup": False,
            "rendering": {
                "mode": "hidden",
                "source": None,
                "field_allowed_by_rules": False,
                "field_data_valid": False,
            },
            "referees": [],
            "coaches": {
                "home_team": [],
                "away_team": [],
            },
            "lineups": {
                "home_team": {"team_id": game.home_team_id, "team_name": game.home_team.name if game.home_team else None, "formation": None, "kit_color": None, "starters": [], "substitutes": []},
                "away_team": {"team_id": game.away_team_id, "team_name": game.away_team.name if game.away_team else None, "formation": None, "kit_color": None, "starters": [], "substitutes": []},
            },
        }

    # Read-through refresh from live feed for active games.
    if game.is_live:
        ttl_seconds = max(0, settings.lineup_live_refresh_ttl_seconds)
        last_sync = game.lineup_live_synced_at
        is_stale = (
            last_sync is None
            or (datetime.utcnow() - last_sync) >= timedelta(seconds=ttl_seconds)
        )
        if is_stale:
            try:
                await LineupSyncService(db, client).sync_live_positions_and_kits(
                    game_id,
                    mode="live_read",
                    timeout_seconds=settings.lineup_live_refresh_timeout_seconds,
                )
                refreshed_game_result = await db.execute(
                    select(Game)
                    .where(Game.id == game_id)
                    .options(
                        selectinload(Game.home_team),
                        selectinload(Game.away_team),
                        selectinload(Game.season)
                        .selectinload(Season.championship),
                    )
                )
                refreshed_game = refreshed_game_result.scalar_one_or_none()
                if refreshed_game is not None:
                    game = refreshed_game
            except Exception as exc:  # noqa: BLE001
                logger.warning("Live lineup read-refresh failed for game %s: %s", game_id, exc)
                await db.rollback()

    # Get referees for this game
    referees_result = await db.execute(
        select(GameReferee)
        .where(GameReferee.game_id == game_id)
        .options(selectinload(GameReferee.referee))
    )
    game_referees = referees_result.scalars().all()

    referees_response = []
    for gr in game_referees:
        ref = gr.referee
        if ref:
            # Select name based on lang
            if lang == "kz":
                first_name = ref.first_name_kz or ref.first_name
                last_name = ref.last_name_kz or ref.last_name
            elif lang == "en":
                first_name = ref.first_name_en or ref.first_name
                last_name = ref.last_name_en or ref.last_name
            else:
                first_name = ref.first_name
                last_name = ref.last_name

            referees_response.append({
                "id": ref.id,
                "first_name": first_name,
                "last_name": last_name,
                "role": gr.role.value,
                "photo_url": ref.photo_url,
                "country": ref.country,
            })

    # Get coaches for home and away teams
    async def get_team_coaches(team_id: int) -> list:
        coaches_result = await db.execute(
            select(TeamCoach)
            .where(TeamCoach.team_id == team_id, TeamCoach.season_id == game.season_id)
            .options(selectinload(TeamCoach.coach))
        )
        team_coaches = coaches_result.scalars().all()

        coaches_list = []
        for tc in team_coaches:
            coach = tc.coach
            if coach:
                if lang == "kz":
                    first_name = coach.first_name_kz or coach.first_name
                    last_name = coach.last_name_kz or coach.last_name
                elif lang == "en":
                    first_name = coach.first_name_en or coach.first_name
                    last_name = coach.last_name_en or coach.last_name
                else:
                    first_name = coach.first_name
                    last_name = coach.last_name

                coaches_list.append({
                    "id": coach.id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "role": tc.role.value,
                    "photo_url": coach.photo_url,
                    "country": coach.country,
                })
        return coaches_list

    home_coaches = await get_team_coaches(game.home_team_id) if game.home_team_id else []
    away_coaches = await get_team_coaches(game.away_team_id) if game.away_team_id else []

    # Position order for sorting: line (GK→DEF→MID→FWD) + side (L→C→R)
    POSITION_ORDER = {
        # Goalkeeper
        ('Gk', 'C'): 0, ('Gk', None): 0,
        # Defenders: L → LC → C → RC → R (positions 1-5)
        ('D', 'L'): 1,
        ('D', 'LC'): 2,
        ('D', 'C'): 3,
        ('D', 'RC'): 4,
        ('D', 'R'): 5,
        ('D', None): 3,
        # Holding line: DM first, then M with central/right positions
        ('DM', 'L'): 6,
        ('DM', 'LC'): 6,
        ('DM', 'C'): 6,
        ('DM', 'RC'): 7,
        ('DM', 'R'): 7,
        ('DM', None): 6,
        ('M', 'C'): 7,
        ('M', 'RC'): 7,
        ('M', 'R'): 8,
        # Attacking line: AM L first, then M LC as center, then AM R
        ('AM', 'L'): 9,
        ('AM', 'LC'): 9,
        ('M', 'L'): 10,
        ('M', 'LC'): 10,
        ('M', None): 10,
        ('AM', 'C'): 10,
        ('AM', 'RC'): 11,
        ('AM', 'R'): 11,
        ('AM', None): 10,
        # Forwards: L → C → R
        ('F', 'L'): 12,
        ('F', 'LC'): 12,
        ('F', 'C'): 13,
        ('F', 'RC'): 14,
        ('F', 'R'): 14,
        ('F', None): 13,
    }

    def get_position_order(amplua: str | None, field_pos: str | None) -> int:
        """Get sort order for a position (GK→DEF→MID→FWD, L→C→R)."""
        if not amplua:
            return 99
        return POSITION_ORDER.get((amplua, field_pos), POSITION_ORDER.get((amplua, None), 99))

    # Get lineups for home and away teams
    async def get_team_lineup(
        team_id: int,
        team_name: str | None,
        formation: str | None,
        kit_color: str | None,
    ) -> dict:
        lineup_result = await db.execute(
            select(GameLineup)
            .where(GameLineup.game_id == game_id, GameLineup.team_id == team_id)
            .options(selectinload(GameLineup.player).selectinload(Player.country))
            .order_by(GameLineup.lineup_type)
        )
        lineup_entries = lineup_result.scalars().all()

        starters = []
        substitutes = []

        for entry in lineup_entries:
            player = entry.player
            # Use match-specific position from GameLineup, fallback to player's general position
            role_hint = player.top_role if player else None
            position = entry.amplua or role_hint
            resolved_amplua, resolved_field_position = resolve_lineup_position_fallback(
                entry.amplua,
                entry.field_position,
                role_hint,
            )

            # Build country data
            country_data = None
            if player and player.country:
                country_data = {
                    "id": player.country.id,
                    "code": player.country.code,
                    "name": player.country.name,
                    "flag_url": player.country.flag_url,
                }

            player_data = {
                "player_id": entry.player_id,
                "first_name": player.first_name if player else None,
                "last_name": player.last_name if player else None,
                "country": country_data,
                "shirt_number": entry.shirt_number,
                "is_captain": entry.is_captain,
                "position": position,
                "amplua": resolved_amplua,
                "field_position": resolved_field_position,
                "photo_url": player.photo_url if player else None,
                "_sort_order": get_position_order(resolved_amplua, resolved_field_position),
            }

            if entry.lineup_type.value == "starter":
                starters.append(player_data)
            else:
                substitutes.append(player_data)

        # Sort starters by position (GK first, then DEF L→R, then MID L→R, then FWD L→R)
        starters.sort(key=lambda x: x.get("_sort_order", 99))

        # Remove internal sort field before returning
        for p in starters:
            p.pop("_sort_order", None)
        for p in substitutes:
            p.pop("_sort_order", None)

        # Prefer formation synced from SOTA /em feed and persisted in Game.
        raw_formation = formation.strip() if isinstance(formation, str) and formation.strip() else None
        if raw_formation is not None:
            final_formation = raw_formation
        else:
            positions = [p.get("amplua") for p in starters]
            final_formation = detect_formation(positions)

        return {
            "team_id": team_id,
            "team_name": team_name,
            "formation": final_formation,
            "kit_color": kit_color,
            "starters": starters,
            "substitutes": substitutes,
        }

    # Use formations from game (synced from SOTA)
    home_lineup = (
        await get_team_lineup(
            game.home_team_id,
            game.home_team.name if game.home_team else None,
            game.home_formation,
            game.home_kit_color,
        )
        if game.home_team_id
        else {"team_id": None, "team_name": None, "formation": None, "kit_color": None, "starters": [], "substitutes": []}
    )
    away_lineup = (
        await get_team_lineup(
            game.away_team_id,
            game.away_team.name if game.away_team else None,
            game.away_formation,
            game.away_kit_color,
        )
        if game.away_team_id
        else {"team_id": None, "team_name": None, "formation": None, "kit_color": None, "starters": [], "substitutes": []}
    )

    has_lineup_data = _has_any_lineup_data(home_lineup, away_lineup)
    field_allowed_by_rules = _is_field_allowed_by_rules(game)
    field_data_valid = _team_has_valid_field_data(home_lineup) and _team_has_valid_field_data(away_lineup)

    if not has_lineup_data:
        rendering_mode = "hidden"
    elif field_allowed_by_rules and field_data_valid:
        rendering_mode = "field"
    else:
        rendering_mode = "list"

    source = _normalize_lineup_source(game.lineup_source, has_lineup_data)

    return {
        "game_id": game_id,
        "has_lineup": has_lineup_data,
        "rendering": {
            "mode": rendering_mode,
            "source": source,
            "field_allowed_by_rules": field_allowed_by_rules,
            "field_data_valid": field_data_valid,
        },
        "referees": referees_response,
        "coaches": {
            "home_team": home_coaches,
            "away_team": away_coaches,
        },
        "lineups": {
            "home_team": home_lineup,
            "away_team": away_lineup,
        },
    }
