"""Automatic cup bracket winner advancement.

When a cup match finishes, the winner is inserted into the next round's
CupDraw so they appear in the bracket without manual admin intervention.
"""
import logging
from math import ceil

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified

from app.models import CupDraw, Game, Season
from app.services.cup_rounds import infer_round_key

logger = logging.getLogger(__name__)

ADVANCEMENT_MAP: dict[str, str] = {
    "1_32": "1_16",
    "1_16": "1_8",
    "1_8": "1_4",
    "1_4": "1_2",
    "1_2": "final",
}

LOSERS_ROUND: dict[str, str] = {
    "1_2": "3rd_place",
}


def _determine_winner_loser(game: Game) -> tuple[int | None, int | None]:
    """Return (winner_team_id, loser_team_id) or (None, None) if undetermined."""
    if game.home_score is None or game.away_score is None:
        return None, None
    if not game.home_team_id or not game.away_team_id:
        return None, None

    if game.home_score > game.away_score:
        return game.home_team_id, game.away_team_id
    if game.away_score > game.home_score:
        return game.away_team_id, game.home_team_id

    # Draw — check penalties
    hp = game.home_penalty_score
    ap = game.away_penalty_score
    if hp is not None and ap is not None:
        if hp > ap:
            return game.home_team_id, game.away_team_id
        if ap > hp:
            return game.away_team_id, game.home_team_id

    return None, None


def _find_pair_for_teams(
    pairs: list[dict], team1_id: int, team2_id: int
) -> dict | None:
    """Find existing pair that contains both teams."""
    team_set = {team1_id, team2_id}
    for p in pairs:
        if {p.get("team1_id"), p.get("team2_id")} == team_set:
            return p
    return None


def _insert_team_into_next_round(
    draw: CupDraw,
    team_id: int,
    next_sort_order: int,
    next_side: str,
    is_home: bool,
) -> bool:
    """Insert team into the next round's draw pair.

    is_home=True → team1_id (home), is_home=False → team2_id (away).
    Returns True if changed.
    """
    pairs = draw.pairs or []

    # Find existing pair at this slot
    target = None
    for p in pairs:
        if p.get("sort_order") == next_sort_order and p.get("side") == next_side:
            target = p
            break

    slot_key = "team1_id" if is_home else "team2_id"

    if target:
        # Already has this team?
        if target.get("team1_id") == team_id or target.get("team2_id") == team_id:
            return False  # idempotent

        # Fill the correct slot based on bracket position
        if target.get(slot_key) is None:
            target[slot_key] = team_id
        else:
            # Slot taken — try the other one
            other_key = "team2_id" if is_home else "team1_id"
            if target.get(other_key) is None:
                target[other_key] = team_id
            else:
                logger.warning(
                    "Next round pair already full: sort_order=%s side=%s draw_id=%s",
                    next_sort_order, next_side, draw.id,
                )
                return False
    else:
        # Create new pair with team in the correct slot
        pairs.append({
            "team1_id": team_id if is_home else None,
            "team2_id": None if is_home else team_id,
            "sort_order": next_sort_order,
            "side": next_side,
            "is_published": True,
        })

    draw.pairs = pairs
    flag_modified(draw, "pairs")
    return True


async def advance_cup_winner(db: AsyncSession, game: Game) -> dict:
    """Advance the winner (and loser for semi-finals) to the next round.

    Idempotent: safe to call multiple times for the same game.
    """
    result: dict = {"game_id": game.id, "advanced": False}

    # Must be finished
    if game.status.value != "finished" if hasattr(game.status, 'value') else game.status != "finished":
        return result

    # Must have a stage
    if not game.stage:
        # Eagerly load stage if not loaded
        await db.refresh(game, ["stage"])
    if not game.stage:
        return result

    round_key = infer_round_key(game.stage)
    next_round_key = ADVANCEMENT_MAP.get(round_key)
    losers_round_key = LOSERS_ROUND.get(round_key)

    if not next_round_key and not losers_round_key:
        return result  # final or non-playoff round

    # Check season is a cup
    season = await db.get(Season, game.season_id)
    if not season or season.frontend_code != "cup":
        return result

    # Determine winner/loser
    winner_id, loser_id = _determine_winner_loser(game)
    if winner_id is None:
        logger.info("Cannot determine winner for game %s (draw without penalties?)", game.id)
        return result

    # Find current round draw to get sort_order/side
    current_draw = await _get_draw(db, game.season_id, round_key)
    current_sort_order = 1
    current_side = "left"

    if current_draw and current_draw.pairs:
        pair = _find_pair_for_teams(
            current_draw.pairs, game.home_team_id, game.away_team_id
        )
        if pair:
            current_sort_order = pair.get("sort_order", 1)
            current_side = pair.get("side", "left")

    # Advance winner to next round
    if next_round_key:
        next_sort_order = ceil(current_sort_order / 2)
        next_side = current_side if next_round_key != "final" else "center"
        # Odd sort_order → home (team1), even → away (team2)
        is_home = current_sort_order % 2 == 1

        next_draw = await _get_or_create_draw(db, game.season_id, next_round_key)
        changed = _insert_team_into_next_round(
            next_draw, winner_id, next_sort_order, next_side, is_home
        )
        if changed:
            result["advanced"] = True
            result["next_round"] = next_round_key
            result["winner_team_id"] = winner_id
            logger.info(
                "Advanced team %s to %s (sort_order=%s, side=%s) from game %s",
                winner_id, next_round_key, next_sort_order, next_side, game.id,
            )

    # Advance loser to 3rd place match (semi-finals only)
    if losers_round_key and loser_id:
        loser_sort_order = 1  # 3rd place is always sort_order 1
        loser_side = "center"

        loser_draw = await _get_or_create_draw(db, game.season_id, losers_round_key)
        loser_is_home = current_sort_order % 2 == 1
        loser_changed = _insert_team_into_next_round(
            loser_draw, loser_id, loser_sort_order, loser_side, loser_is_home
        )
        if loser_changed:
            result["loser_advanced"] = True
            result["losers_round"] = losers_round_key
            logger.info(
                "Advanced loser team %s to %s from game %s",
                loser_id, losers_round_key, game.id,
            )

    if result.get("advanced") or result.get("loser_advanced"):
        await db.commit()

    return result


async def _get_draw(
    db: AsyncSession, season_id: int, round_key: str
) -> CupDraw | None:
    result = await db.execute(
        select(CupDraw).where(
            CupDraw.season_id == season_id,
            CupDraw.round_key == round_key,
        )
    )
    return result.scalar_one_or_none()


async def _get_or_create_draw(
    db: AsyncSession, season_id: int, round_key: str
) -> CupDraw:
    draw = await _get_draw(db, season_id, round_key)
    if draw is None:
        draw = CupDraw(
            season_id=season_id,
            round_key=round_key,
            status="active",
            pairs=[],
        )
        db.add(draw)
        await db.flush()
    return draw
