import logging
import re

from app.utils.game_status import compute_game_status
from app.models import Game, Stage
from app.schemas.cup import CupGameBrief, CupRound, CupTeamBrief
from app.schemas.playoff_bracket import (
    ROUND_LABELS,
    BracketGameBrief,
    BracketGameTeam,
    PlayoffBracketEntry,
    PlayoffBracketResponse,
    PlayoffRound,
)
from app.utils.localization import get_localized_field


logger = logging.getLogger(__name__)

PLAYOFF_ROUND_ORDER = ["1_32", "1_16", "1_8", "1_4", "1_2", "3rd_place", "final"]


def build_cup_game(game: Game, lang: str) -> CupGameBrief:
    """Build a CupGameBrief from a Game ORM object."""
    home_team = None
    away_team = None
    if game.home_team:
        home_team = CupTeamBrief(
            id=game.home_team.id,
            name=get_localized_field(game.home_team, "name", lang),
            logo_url=game.home_team.logo_url,
        )
    if game.away_team:
        away_team = CupTeamBrief(
            id=game.away_team.id,
            name=get_localized_field(game.away_team, "name", lang),
            logo_url=game.away_team.logo_url,
        )

    stage_name = None
    if game.stage:
        stage_name = get_localized_field(game.stage, "name", lang)

    status = compute_game_status(game)
    return CupGameBrief(
        id=game.id,
        date=game.date,
        time=game.time,
        stage_name=stage_name,
        home_team=home_team,
        away_team=away_team,
        home_score=game.home_score,
        away_score=game.away_score,
        home_penalty_score=game.home_penalty_score,
        away_penalty_score=game.away_penalty_score,
        status=status,
        is_live=game.is_live,
    )


def infer_round_key(stage: Stage) -> str:
    """
    Infer round key from stage names in RU/KZ/EN.

    Examples:
    - "1/8 финала" -> "1_8"
    - "Quarter-final" -> "1_4"
    - "Финал"/"Final" -> "final"
    """
    names = [
        (stage.name or "").lower().strip(),
        (stage.name_kz or "").lower().strip(),
        (stage.name_en or "").lower().strip(),
    ]
    text = " ".join([n for n in names if n])

    # Numeric rounds: 1/32, 1/16, 1/8, 1/4, 1/2
    fraction_match = re.search(r"1\s*/\s*(32|16|8|4|2)\b", text)
    if fraction_match:
        return f"1_{fraction_match.group(1)}"

    # EN aliases
    if "round of 64" in text:
        return "1_32"
    if "round of 32" in text:
        return "1_16"
    if "round of 16" in text:
        return "1_8"
    if "quarter" in text:
        return "1_4"
    if "semi" in text:
        return "1_2"

    # RU aliases
    if "1/32" in text:
        return "1_32"
    if "1/16" in text:
        return "1_16"
    if "1/8" in text:
        return "1_8"
    if "четверть" in text:
        return "1_4"
    if "полуфин" in text:
        return "1_2"

    # KZ aliases
    if "1/32" in text:
        return "1_32"
    if "1/16" in text:
        return "1_16"
    if "1/8" in text:
        return "1_8"
    if "ширек" in text:
        return "1_4"
    if "жартылай" in text:
        return "1_2"

    # Third place
    if (
        "3rd" in text
        or "third place" in text
        or "за 3" in text
        or ("3" in text and "мест" in text)
        or "үшінші орын" in text
    ):
        return "3rd_place"

    # Final (after semi checks)
    if "финал" in text or re.search(r"\bfinal\b", text):
        return "final"

    # Group/tour round keys
    tour_match = re.search(r"(тур|tour|round)\s*(\d+)", text)
    if tour_match:
        return f"group_{tour_match.group(2)}"

    group_match = re.search(r"(группа|group|топ)\s*([a-zа-я0-9]+)", text)
    if group_match:
        return f"group_{group_match.group(2)}"

    # Fallback slug from primary stage name.
    slug = re.sub(r"[^a-z0-9]+", "_", (stage.name or "").lower()).strip("_")
    return slug or f"stage_{stage.id}"


def determine_current_round(rounds: list[CupRound]) -> CupRound | None:
    """Pick current round: live > first incomplete > last with games."""
    for round_item in rounds:
        if any(game.is_live for game in round_item.games):
            return round_item

    for round_item in rounds:
        if round_item.total_games > 0 and round_item.played_games < round_item.total_games:
            return round_item

    for round_item in reversed(rounds):
        if round_item.total_games > 0:
            return round_item

    return None


def build_schedule_rounds(
    games: list[Game],
    stages: list[Stage],
    lang: str,
    include_games: bool = True,
) -> list[CupRound]:
    """Group games by stage into CupRound objects."""
    stage_ids = {stage.id for stage in stages}
    games_by_stage: dict[int, list[Game]] = {}
    orphan_games: list[Game] = []

    for game in games:
        if game.stage_id is not None and game.stage_id in stage_ids:
            games_by_stage.setdefault(game.stage_id, []).append(game)
        else:
            orphan_games.append(game)

    rounds: list[CupRound] = []
    sorted_stages = sorted(stages, key=lambda stage: (stage.sort_order, stage.id))

    for stage in sorted_stages:
        stage_games = games_by_stage.get(stage.id, [])
        played_games = sum(1 for game in stage_games if compute_game_status(game) == "finished")
        round_games = [build_cup_game(game, lang) for game in stage_games] if include_games else []

        rounds.append(
            CupRound(
                stage_id=stage.id,
                round_name=get_localized_field(stage, "name", lang) or f"Stage {stage.id}",
                round_key=infer_round_key(stage),
                is_current=False,
                total_games=len(stage_games),
                played_games=played_games,
                games=round_games,
            )
        )

    if orphan_games:
        logger.warning(
            "Cup schedule contains %s games without valid stage_id; mapped to 'other' bucket",
            len(orphan_games),
        )
        played_games = sum(1 for game in orphan_games if compute_game_status(game) == "finished")
        round_games = [build_cup_game(game, lang) for game in orphan_games] if include_games else []
        rounds.append(
            CupRound(
                stage_id=None,
                round_name="Other",
                round_key="other",
                is_current=False,
                total_games=len(orphan_games),
                played_games=played_games,
                games=round_games,
            )
        )

    return rounds


def _is_playoff_round_key(round_key: str) -> bool:
    if round_key in {"final", "3rd_place"}:
        return True

    if round_key.startswith("1_"):
        try:
            denominator = int(round_key.split("_", 1)[1])
            return denominator in {2, 4, 8, 16, 32}
        except ValueError:
            return False

    return False


def _build_bracket_game(game: CupGameBrief) -> BracketGameBrief:
    home_team = None
    away_team = None
    if game.home_team:
        home_team = BracketGameTeam(
            id=game.home_team.id,
            name=game.home_team.name,
            logo_url=game.home_team.logo_url,
        )
    if game.away_team:
        away_team = BracketGameTeam(
            id=game.away_team.id,
            name=game.away_team.name,
            logo_url=game.away_team.logo_url,
        )

    return BracketGameBrief(
        id=game.id,
        date=game.date,
        time=game.time,
        home_team=home_team,
        away_team=away_team,
        home_score=game.home_score,
        away_score=game.away_score,
        home_penalty_score=game.home_penalty_score,
        away_penalty_score=game.away_penalty_score,
        status=game.status,
    )


def build_playoff_bracket_from_rounds(
    season_id: int,
    rounds: list[CupRound],
) -> PlayoffBracketResponse | None:
    """Build a bracket response from schedule rounds without playoff_brackets table."""
    playoff_rounds = [round_item for round_item in rounds if _is_playoff_round_key(round_item.round_key)]
    if not playoff_rounds:
        return None

    rounds_by_key: dict[str, list[PlayoffBracketEntry]] = {}
    fallback_round_order: list[str] = []
    synthetic_id = 1

    for round_item in playoff_rounds:
        if round_item.round_key not in fallback_round_order:
            fallback_round_order.append(round_item.round_key)

        entries: list[PlayoffBracketEntry] = []
        side_counters: dict[str, int] = {"left": 0, "right": 0, "center": 0}
        for index, game in enumerate(round_item.games):
            if round_item.round_key in {"final", "3rd_place"}:
                side = "center"
            else:
                side = "left" if index % 2 == 0 else "right"

            side_counters[side] += 1
            entries.append(
                PlayoffBracketEntry(
                    id=synthetic_id,
                    round_name=round_item.round_key,
                    side=side,
                    sort_order=side_counters[side],
                    is_third_place=round_item.round_key == "3rd_place",
                    game=_build_bracket_game(game),
                )
            )
            synthetic_id += 1

        rounds_by_key.setdefault(round_item.round_key, []).extend(entries)

    response_rounds: list[PlayoffRound] = []
    for round_key in PLAYOFF_ROUND_ORDER:
        if round_key in rounds_by_key:
            response_rounds.append(
                PlayoffRound(
                    round_name=round_key,
                    round_label=ROUND_LABELS.get(round_key, round_key),
                    entries=rounds_by_key[round_key],
                )
            )

    for round_key in fallback_round_order:
        if round_key not in PLAYOFF_ROUND_ORDER and round_key in rounds_by_key:
            response_rounds.append(
                PlayoffRound(
                    round_name=round_key,
                    round_label=ROUND_LABELS.get(round_key, round_key),
                    entries=rounds_by_key[round_key],
                )
            )

    if not response_rounds:
        return None

    return PlayoffBracketResponse(season_id=season_id, rounds=response_rounds)

