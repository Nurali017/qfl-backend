"""Lineup embargo: Telegram notification + public API visibility timing.

- 90 min before kickoff: send lineup to Telegram (once, resend on changes)
- 60 min before kickoff: make lineup visible on public API
"""

import hashlib
import html
import logging
from datetime import datetime, timedelta

from sqlalchemy import select, exists
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Game, GameStatus, GameLineup
from app.models.game_lineup import LineupType
from app.services.telegram import send_telegram_message
from app.utils.timestamps import combine_almaty_local_to_utc, utcnow, ALMATY_TZ

logger = logging.getLogger(__name__)

TELEGRAM_WINDOW_MINUTES = 90
PUBLIC_WINDOW_MINUTES = 60


def is_lineup_embargoed(game: Game, *, now_utc: datetime | None = None) -> bool:
    """True if lineup should be hidden on public API.

    Embargo applies only to 'created' games with a scheduled time,
    when current time is more than 60 min before kickoff.
    """
    if game.status != GameStatus.created or game.time is None:
        return False
    kickoff = combine_almaty_local_to_utc(game.date, game.time)
    now = now_utc or utcnow()
    return now < kickoff - timedelta(minutes=PUBLIC_WINDOW_MINUTES)


def should_send_telegram(
    game: Game,
    current_hash: str,
    *,
    now_utc: datetime | None = None,
) -> bool:
    """Whether Telegram notification should be sent for this game.

    Sends if:
    - Game is 'created' with a scheduled time
    - Kickoff is within 90 min
    - Never sent before, OR lineup hash changed
    """
    if game.status != GameStatus.created or game.time is None:
        return False

    kickoff = combine_almaty_local_to_utc(game.date, game.time)
    now = now_utc or utcnow()

    # Must be within 90 min window
    if now < kickoff - timedelta(minutes=TELEGRAM_WINDOW_MINUTES):
        return False
    # Don't send after kickoff
    if now > kickoff:
        return False

    # Never sent — always send
    if game.lineup_telegram_sent_at is None:
        return True

    # Already sent with same hash — skip
    return game.lineup_telegram_hash != current_hash


def format_lineup_telegram_message(
    game: Game,
    home_lineups: list[GameLineup],
    away_lineups: list[GameLineup],
    *,
    is_update: bool = False,
) -> str:
    """Build HTML-formatted Telegram message for lineup notification."""

    header = "\U0001f4cb СОСТАВЫ ОБНОВЛЕНЫ" if is_update else "\U0001f4cb СОСТАВЫ"

    home_name = html.escape(game.home_team.name) if game.home_team else "Home"
    away_name = html.escape(game.away_team.name) if game.away_team else "Away"

    # Format date/time in Almaty
    kickoff = combine_almaty_local_to_utc(game.date, game.time)
    almaty_dt = kickoff.astimezone(ALMATY_TZ)
    date_str = almaty_dt.strftime("%d.%m.%Y, %H:%M")

    lines = [
        f"<b>{header}</b>",
        "",
        f"{home_name} — {away_name}",
        date_str,
    ]

    def format_team_section(
        team_name: str,
        formation: str | None,
        lineups: list[GameLineup],
        emoji: str,
    ) -> list[str]:
        section = []
        form_str = f" ({html.escape(formation)})" if formation else ""
        section.append(f"\n{emoji} <b>{team_name}{form_str}</b>")

        starters = sorted(
            [e for e in lineups if e.lineup_type == LineupType.starter],
            key=lambda e: (e.shirt_number or 0, e.player_id),
        )
        subs = sorted(
            [e for e in lineups if e.lineup_type == LineupType.substitute],
            key=lambda e: (e.shirt_number or 0, e.player_id),
        )

        if starters:
            section.append("Основной:")
            for i, entry in enumerate(starters, 1):
                section.append(_format_player_line(i, entry))

        if subs:
            section.append("Запасные:")
            for i, entry in enumerate(subs, len(starters) + 1):
                section.append(_format_player_line(i, entry))

        return section

    lines.extend(format_team_section(
        home_name, game.home_formation, home_lineups, "\U0001f3e0",
    ))
    lines.extend(format_team_section(
        away_name, game.away_formation, away_lineups, "\U0001f3c3",
    ))

    return "\n".join(lines)


def _format_player_line(index: int, entry: GameLineup) -> str:
    """Format a single player line for Telegram."""
    parts = [f"  {index}."]

    if entry.shirt_number is not None:
        parts.append(f"#{entry.shirt_number}")

    if entry.player:
        first = html.escape(entry.player.first_name or "")
        last = html.escape(entry.player.last_name or "")
        name = f"{first} {last}".strip()
        parts.append(name)
    else:
        parts.append(f"ID:{entry.player_id}")

    if entry.amplua:
        parts.append(f"({html.escape(entry.amplua)})")

    if entry.is_captain:
        parts.append("©")

    return " ".join(parts)


def _compute_message_hash(message: str) -> str:
    """SHA-256 hash of the message text."""
    return hashlib.sha256(message.encode("utf-8")).hexdigest()


async def process_lineup_embargo(
    db: AsyncSession,
    *,
    now_utc: datetime | None = None,
) -> dict:
    """Process lineup embargo for upcoming games.

    For each game starting within 90 min:
    1. Build Telegram message, compute hash
    2. If first send or hash changed → send to Telegram
    3. Commit per-game to avoid rollback cascading

    Returns summary dict.
    """
    now = now_utc or utcnow()

    # Convert to Almaty for date/time comparison
    now_almaty = now.astimezone(ALMATY_TZ)
    today = now_almaty.date()
    current_time = now_almaty.time()

    # Compute latest time we care about (90 min from now)
    latest_almaty = now_almaty + timedelta(minutes=TELEGRAM_WINDOW_MINUTES)
    latest_time = latest_almaty.time()

    # Handle midnight crossing: if latest_time < current_time, we cross midnight
    # In that case we need to handle differently, but for simplicity
    # we fetch all created games for today and tomorrow, then filter in Python
    from sqlalchemy import or_, and_

    # Query games with actual lineup rows (EXISTS subquery)
    has_lineup_rows = exists(
        select(GameLineup.id).where(GameLineup.game_id == Game.id)
    )

    result = await db.execute(
        select(Game)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.lineups).selectinload(GameLineup.player),
        )
        .where(
            Game.status == GameStatus.created,
            Game.time.isnot(None),
            has_lineup_rows,
        )
    )
    games = list(result.unique().scalars().all())

    sent = 0
    skipped = 0
    errors = 0

    for game in games:
        try:
            kickoff = combine_almaty_local_to_utc(game.date, game.time)

            # Check if within 90 min window
            time_until_kickoff = kickoff - now
            if time_until_kickoff > timedelta(minutes=TELEGRAM_WINDOW_MINUTES):
                skipped += 1
                continue
            if time_until_kickoff < timedelta(0):
                skipped += 1
                continue

            # Split lineups by team
            home_lineups = [e for e in game.lineups if e.team_id == game.home_team_id]
            away_lineups = [e for e in game.lineups if e.team_id == game.away_team_id]

            # Always compute hash from canonical message (is_update=False)
            # so same lineup data always produces same hash
            canonical_message = format_lineup_telegram_message(
                game, home_lineups, away_lineups, is_update=False,
            )
            current_hash = _compute_message_hash(canonical_message)

            if not should_send_telegram(game, current_hash, now_utc=now):
                skipped += 1
                continue

            # Build actual message with correct header
            is_update = game.lineup_telegram_sent_at is not None
            message = format_lineup_telegram_message(
                game, home_lineups, away_lineups, is_update=is_update,
            ) if is_update else canonical_message

            await send_telegram_message(message)

            game.lineup_telegram_sent_at = now
            game.lineup_telegram_hash = current_hash
            await db.commit()

            sent += 1
            logger.info(
                "Lineup %s for game %s (%s)",
                "resent" if is_update else "sent",
                game.id,
                "hash changed" if is_update else "first send",
            )
        except Exception:
            await db.rollback()
            errors += 1
            logger.exception("Failed to process lineup embargo for game %s", game.id)
            continue

    return {"sent": sent, "skipped": skipped, "errors": errors}
