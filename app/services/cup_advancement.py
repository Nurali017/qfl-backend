"""Automatic cup bracket winner advancement.

When a cup match finishes, the winner is inserted into the next round's
CupDraw so they appear in the bracket without manual admin intervention.
"""
import logging
from datetime import time as time_type
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


def _is_finished(game: Game) -> bool:
    status = game.status.value if hasattr(game.status, "value") else game.status
    return status == "finished"


def _aggregate_winner_loser(legs: list[Game]) -> tuple[int | None, int | None]:
    """Winner/loser across a two-legged tie.

    Decided by aggregate goals (extra time is already in the return leg's score);
    a level aggregate is broken by the return leg's penalty shootout. No away-goals rule.
    """
    ordered = sorted(legs, key=lambda g: (g.date, g.time or time_type.min))
    first = ordered[0]
    last = ordered[-1]
    top_id = first.home_team_id
    bottom_id = first.away_team_id
    if not top_id or not bottom_id:
        return None, None

    top = 0
    bottom = 0
    for leg in ordered:
        if leg.home_score is None or leg.away_score is None:
            return None, None
        if leg.home_team_id == top_id:
            top += leg.home_score
            bottom += leg.away_score
        else:
            top += leg.away_score
            bottom += leg.home_score

    if top > bottom:
        return top_id, bottom_id
    if bottom > top:
        return bottom_id, top_id

    hp = last.home_penalty_score
    ap = last.away_penalty_score
    if hp is None or ap is None:
        return None, None
    if last.home_team_id == top_id:
        top_pen, bottom_pen = hp, ap
    else:
        top_pen, bottom_pen = ap, hp
    if top_pen > bottom_pen:
        return top_id, bottom_id
    if bottom_pen > top_pen:
        return bottom_id, top_id
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

    # Eagerly load stage (lazy loading fails in async context)
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

    # Gather all legs of this tie within the stage. Two-legged ties have two
    # games between the same team pair; advancement waits until both finish.
    legs = [game]
    if game.stage_id and game.home_team_id and game.away_team_id:
        sib_result = await db.execute(
            select(Game).where(
                Game.stage_id == game.stage_id,
                Game.id != game.id,
            )
        )
        pair = {game.home_team_id, game.away_team_id}
        for sibling in sib_result.scalars().all():
            if {sibling.home_team_id, sibling.away_team_id} == pair:
                legs.append(sibling)

    # Determine winner/loser (aggregate for two-legged ties)
    if len(legs) >= 2:
        if not all(_is_finished(leg) for leg in legs):
            logger.info(
                "Two-legged tie for game %s not complete; deferring advancement", game.id
            )
            return result
        winner_id, loser_id = _aggregate_winner_loser(legs)
    else:
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
    elif game.stage_id:
        # No draw — compute position from tie (team-pair) order within the stage.
        # Mirrors build_playoff_bracket_from_rounds, which groups legs into ties.
        stage_games = await db.execute(
            select(Game)
            .where(Game.stage_id == game.stage_id)
            .order_by(Game.date, Game.time, Game.id)
        )
        pair_order: list[frozenset] = []
        seen: set[frozenset] = set()
        for g in stage_games.scalars().all():
            if not g.home_team_id or not g.away_team_id:
                continue
            key = frozenset({g.home_team_id, g.away_team_id})
            if key not in seen:
                seen.add(key)
                pair_order.append(key)
        my_key = frozenset({game.home_team_id, game.away_team_id})
        if my_key in pair_order:
            index = pair_order.index(my_key)
            current_side = "left" if index % 2 == 0 else "right"
            current_sort_order = index // 2 + 1  # 1-based within each side

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
