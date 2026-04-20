"""Auto-publish public match events to t.me/kffleague Telegram channel.

Each post function is idempotent: reads current state, checks dedup flag,
composes HTML, calls send_public_telegram_message, persists the flag on HTTP 200.

Language: Kazakh (KZ). Format: inline custom emoji from KazakhstanFootballClubs
pack (Team.tg_custom_emoji_id) when available; otherwise plain emoji fallback.
"""
from __future__ import annotations

import hashlib
import html
import logging
from pathlib import Path
from collections import OrderedDict, defaultdict
from datetime import date, datetime, time, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Broadcaster, Game, GameBroadcaster, GameEvent, GameLineup, Player, Team
from app.models.game import GameStatus
from app.models.game_event import GameEventType
from app.models.game_lineup import LineupType
from app.models.season import Season
from app.models.championship import Championship
from app.models.stadium import Stadium
from app.services.telegram_user_client import send_public_user_message as send_public_telegram_message
from app.utils.timestamps import utcnow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------- #
#  Constants                                                              #
# ---------------------------------------------------------------------- #

BROADCASTER_DEFAULT_PREFIX: dict[str, str] = {
    "youtube": "📱",
    "tv":      "📺",
}
FALLBACK_BROADCASTER_PREFIX = "💻"

# LIVE custom emoji from QQQTest pack — set to doc_id once user picks which sticker.
# If LIVE_EMOJI_DOC_ID is empty, plain 📺 is used.
LIVE_EMOJI_DOC_ID: str = "5341462672107931269"
LIVE_EMOJI_HTML: str = (
    f'<tg-emoji emoji-id="{LIVE_EMOJI_DOC_ID}">📺</tg-emoji>'
    if LIVE_EMOJI_DOC_ID
    else "📺"
)
YOUTUBE_EMOJI_DOC_ID: str = "5334681713316479679"
YOUTUBE_EMOJI_HTML: str = (
    f'<tg-emoji emoji-id="{YOUTUBE_EMOJI_DOC_ID}">📱</tg-emoji>'
    if YOUTUBE_EMOJI_DOC_ID
    else "📱"
)
GOAL_EMOJI_DOC_ID: str = "5373101763442255191"
GOAL_EMOJI_HTML: str = (
    f'<tg-emoji emoji-id="{GOAL_EMOJI_DOC_ID}">⚽</tg-emoji>'
    if GOAL_EMOJI_DOC_ID
    else "⚽"
)

CLOCK_EMOJI_FULL: dict[int, str] = {
    1: "🕐", 2: "🕑", 3: "🕒", 4: "🕓", 5: "🕔", 6: "🕕",
    7: "🕖", 8: "🕗", 9: "🕘", 10: "🕙", 11: "🕚", 12: "🕛",
}
CLOCK_EMOJI_HALF: dict[int, str] = {
    1: "🕜", 2: "🕝", 3: "🕞", 4: "🕟", 5: "🕠", 6: "🕡",
    7: "🕢", 8: "🕣", 9: "🕤", 10: "🕥", 11: "🕦", 12: "🕧",
}

# Amplua code (SOTA) → line bucket.
AMPLUA_LINE: dict[str, str] = {
    "Gk": "GK",
    "D":  "DEF",
    "DM": "MID",
    "M":  "MID",
    "AM": "MID",
    "F":  "FWD",
}
LINE_ORDER = ("GK", "DEF", "MID", "FWD")
LINE_PREFIX = {"GK": "🧤", "DEF": "🛡", "MID": "🎯", "FWD": "⚔"}


# ---------------------------------------------------------------------- #
#  Helpers                                                                #
# ---------------------------------------------------------------------- #

def _esc(s: str | None) -> str:
    return html.escape(s, quote=False) if s else ""


def _team_emoji_for_event(event: GameEvent, game: Game) -> str:
    """Return custom emoji of the team that event.team_id refers to."""
    if event.team_id == game.home_team_id:
        return _team_emoji(game.home_team)
    if event.team_id == game.away_team_id:
        return _team_emoji(game.away_team)
    return "⚽"


def _team_emoji(team: Team | None) -> str:
    """Custom emoji from KazakhstanFootballClubs pack or fallback ⚽."""
    if team is None:
        return "⚽"
    if team.tg_custom_emoji_id:
        return (
            f'<tg-emoji emoji-id="{html.escape(team.tg_custom_emoji_id, quote=True)}">'
            f'⚽</tg-emoji>'
        )
    return "⚽"


def _team_name_kz(team: Team | None) -> str:
    if team is None:
        return ""
    return team.name_kz or team.name or ""


def _surname(full_name: str | None) -> str:
    """Take the last whitespace-separated token as surname."""
    if not full_name:
        return ""
    tokens = full_name.strip().split()
    return tokens[-1] if tokens else ""


def _score(h: int | None, a: int | None) -> str:
    return f"{h if h is not None else 0}:{a if a is not None else 0}"


def _score_block(game: Game) -> str:
    """ «Хозяева» 1:0 «Гости» with emoji — one string. """
    home_emoji = _team_emoji(game.home_team)
    away_emoji = _team_emoji(game.away_team)
    return (
        f"{home_emoji}«{_esc(_team_name_kz(game.home_team))}» "
        f"{_score(game.home_score, game.away_score)} "
        f"«{_esc(_team_name_kz(game.away_team))}»{away_emoji}"
    )


def _clock_emoji(t: time | None) -> str:
    if t is None:
        return "🕒"
    hour12 = t.hour % 12 or 12
    if t.minute >= 30:
        return CLOCK_EMOJI_HALF[hour12]
    return CLOCK_EMOJI_FULL[hour12]


def _broadcast_lines(game: Game) -> list[str]:
    """Compose '{prefix}{name}' lines from game.broadcasters relationship.

    If broadcaster.type=youtube and game.youtube_live_url set, wraps the
    name in <a href=...> so it renders as a clickable stream link.
    """
    assignments: list[GameBroadcaster] = list(game.broadcasters or [])
    assignments.sort(key=lambda gb: (gb.sort_order, gb.id))
    out: list[str] = []
    for gb in assignments:
        br: Broadcaster | None = gb.broadcaster
        if br is None or not br.is_active:
            continue
        prefix = br.telegram_prefix or BROADCASTER_DEFAULT_PREFIX.get(
            (br.type or "").lower(), FALLBACK_BROADCASTER_PREFIX
        )
        name_html = _esc(br.name)
        if (br.type or "").lower() == "youtube" and game.youtube_live_url:
            name_html = f'<a href="{_esc(game.youtube_live_url)}">{name_html}</a>'
        out.append(f"{prefix}{name_html}")
    return out


def _competition_short_kz(season: Season | None) -> str:
    if season is None:
        return ""
    champ = season.championship if season.championship else None
    if champ and champ.short_name_kz:
        return champ.short_name_kz
    if champ and champ.name_kz:
        return champ.name_kz
    return season.name_kz or season.name or ""


def _league_emoji(season: Season | None) -> str:
    """League/competition custom emoji or empty if not configured."""
    if season is None or not season.tg_custom_emoji_id:
        return ""
    return (
        f'<tg-emoji emoji-id="{html.escape(season.tg_custom_emoji_id, quote=True)}">'
        f'🏆</tg-emoji>'
    )


def _goal_tag(event: GameEvent) -> str:
    """Tag in parens after minute for goal-family events."""
    if event.event_type == GameEventType.penalty:
        return " (пен.)"
    if event.event_type == GameEventType.own_goal:
        return " (автогол)"
    if event.assist_player_name:
        return f" (ассист: {_esc(_surname(event.assist_player_name))})"
    return ""


def _scorer_summary(events: list[GameEvent]) -> str:
    """
    "Шушеначев 12', 67' · Петров 45' (пен.)"
    Groups by scorer, tags attached per minute.
    """
    by_scorer: "OrderedDict[str, list[str]]" = OrderedDict()
    sorted_events = sorted(
        [e for e in events if e.event_type in (
            GameEventType.goal, GameEventType.penalty, GameEventType.own_goal,
        )],
        key=lambda e: (e.half, e.minute),
    )
    for ev in sorted_events:
        name = _surname(ev.player_name) or "—"
        piece = f"{ev.minute}'"
        if ev.event_type == GameEventType.penalty:
            piece += " (пен.)"
        elif ev.event_type == GameEventType.own_goal:
            piece += " (автогол)"
        by_scorer.setdefault(name, []).append(piece)
    return " · ".join(
        f"{_esc(name)} {', '.join(minutes)}" for name, minutes in by_scorer.items()
    )


def _hash_lineup(starters: list[GameLineup]) -> str:
    key = ",".join(str(ln.player_id) for ln in sorted(starters, key=lambda x: x.player_id))
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _format_lineup_block(
    team: Team | None,
    formation: str | None,
    lineups: list[GameLineup],
    players_by_id: dict[int, Player],
) -> str:
    """One team block for pregame lineup post."""
    starters = [l for l in lineups if l.lineup_type == LineupType.starter]
    subs = [l for l in lineups if l.lineup_type == LineupType.substitute]

    lines: list[str] = []
    header = f"{_team_emoji(team)}«{_esc(_team_name_kz(team))}»"
    if formation:
        header += f" ({_esc(formation)})"
    lines.append(header)

    buckets: dict[str, list[GameLineup]] = defaultdict(list)
    for ln in starters:
        bucket = AMPLUA_LINE.get((ln.amplua or "").strip(), "MID")
        buckets[bucket].append(ln)

    for line_key in LINE_ORDER:
        entries = buckets.get(line_key, [])
        if not entries:
            continue
        entries.sort(key=lambda x: (x.shirt_number or 99))
        parts: list[str] = []
        for ln in entries:
            player = players_by_id.get(ln.player_id)
            surname = _player_surname(player)
            num = ln.shirt_number or ""
            tag = " (К)" if ln.is_captain else ""
            token = f"{num}. {_esc(surname)}{tag}" if num else f"{_esc(surname)}{tag}"
            parts.append(token)
        lines.append(f"{LINE_PREFIX[line_key]} " + " · ".join(parts))

    if subs:
        subs.sort(key=lambda x: (x.shirt_number or 99))
        sub_parts: list[str] = []
        for ln in subs:
            player = players_by_id.get(ln.player_id)
            surname = _player_surname(player)
            num = ln.shirt_number or ""
            sub_parts.append(f"{num}. {_esc(surname)}" if num else _esc(surname))
        lines.append("Запас: " + " · ".join(sub_parts))

    return "\n".join(lines)


def _player_surname(player) -> str:
    """Prefer KZ last_name, fallback to RU, then full name string."""
    if player is None:
        return ""
    return (
        (player.last_name_kz or "").strip()
        or (player.last_name or "").strip()
        or _surname(getattr(player, "first_name", None))
    )


# ---------------------------------------------------------------------- #
#  Scenario 0 — Tour announcement (evening before)                        #
# ---------------------------------------------------------------------- #

async def post_tour_announcement(
    db: AsyncSession, season_id: int, tour: int, for_date: date
) -> bool:
    """One post per (season, tour, date). Groups all matches of that tour day."""
    q = (
        select(Game)
        .where(
            Game.season_id == season_id,
            Game.tour == tour,
            Game.date == for_date,
            Game.status == GameStatus.created,
        )
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stadium_rel),
            selectinload(Game.season).selectinload(Season.championship),
            selectinload(Game.broadcasters).selectinload(GameBroadcaster.broadcaster),
        )
        .order_by(Game.time.nullslast(), Game.id)
    )
    games = (await db.execute(q)).scalars().all()
    if not games:
        return False
    if any(g.announce_telegram_sent_at is not None for g in games):
        logger.info(
            "Tour announce dedup: some games already announced for %s/%s/%s",
            season_id, tour, for_date,
        )
        return False

    header_comp = _esc(_competition_short_kz(games[0].season))
    header_emoji = _league_emoji(games[0].season)
    lines: list[str] = [
        f"⚡{header_comp}{header_emoji}. {tour}-тур. Ертең өтетін матчтарды қайдан көруге болады?"
    ]

    for g in games:
        block: list[str] = [""]
        home_emoji = _team_emoji(g.home_team)
        away_emoji = _team_emoji(g.away_team)
        block.append(
            f"{home_emoji}{_esc(_team_name_kz(g.home_team))} 🆚 "
            f"{_esc(_team_name_kz(g.away_team))}{away_emoji}"
        )
        if g.time:
            block.append(f"{_clock_emoji(g.time)}{g.time.strftime('%H:%M')}")
        stadium = g.stadium_rel
        if stadium:
            city = _esc(stadium.city_kz or stadium.city or "")
            name = _esc(stadium.name_kz or stadium.name or "")
            block.append("🏟️" + (f"{city}, {name}" if city else name))
        for bl in _broadcast_lines(g):
            block.append(bl)
        lines.append("\n".join(block))

    text = "\n".join(lines)

    ok = await send_public_telegram_message(text)
    if not ok:
        return False

    now = utcnow()
    for g in games:
        g.announce_telegram_sent_at = now
    await db.commit()
    return True


# ---------------------------------------------------------------------- #
#  Scenario 1 — Match start                                               #
# ---------------------------------------------------------------------- #

async def post_match_start(db: AsyncSession, game_id: int) -> bool:
    from app.config import get_settings
    if not get_settings().telegram_match_start_enabled:
        return False
    q = (
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stadium_rel),
            selectinload(Game.season).selectinload(Season.championship),
        )
    )
    game = (await db.execute(q)).scalar_one_or_none()
    if game is None or game.start_telegram_sent_at is not None:
        return False

    comp = _esc(_competition_short_kz(game.season))
    stadium = game.stadium_rel
    stadium_line = ""
    if stadium:
        stadium_line = _esc(stadium.name_kz or stadium.name or "")

    parts: list[str] = [
        "🔴 ТІКЕЛЕЙ ЭФИР",
        "",
        f"{_team_emoji(game.home_team)}«{_esc(_team_name_kz(game.home_team))}» — "
        f"«{_esc(_team_name_kz(game.away_team))}»{_team_emoji(game.away_team)}",
        "",
    ]
    if comp:
        tour_suffix = f" · {game.tour}-тур" if game.tour else ""
        parts.append(f"🏆 {comp}{tour_suffix}")
    if stadium_line:
        parts.append(f"🏟 {stadium_line}")
    parts.append("⏱ Ойын басталды!")
    if game.youtube_live_url:
        parts.append("")
        parts.append(f"📺 {_esc(game.youtube_live_url)}")

    text = "\n".join(parts)

    ok = await send_public_telegram_message(text)
    if not ok:
        return False

    game.start_telegram_sent_at = utcnow()
    await db.commit()
    return True


# ---------------------------------------------------------------------- #
#  Scenario 2/3 — Goal and red card                                       #
# ---------------------------------------------------------------------- #

GOAL_TYPES = {GameEventType.goal, GameEventType.penalty, GameEventType.own_goal}
RED_TYPES = {GameEventType.red_card, GameEventType.second_yellow}
POSTABLE_EVENT_TYPES = GOAL_TYPES | RED_TYPES


async def post_game_event(db: AsyncSession, event_id: int) -> bool:
    q = (
        select(GameEvent)
        .where(GameEvent.id == event_id)
        .options(
            selectinload(GameEvent.game).selectinload(Game.home_team),
            selectinload(GameEvent.game).selectinload(Game.away_team),
        )
    )
    event = (await db.execute(q)).scalar_one_or_none()
    if event is None or event.telegram_sent_at is not None:
        return False
    if event.event_type not in POSTABLE_EVENT_TYPES:
        return False

    game = event.game
    if game is None:
        return False
    # Tight score variant for goal/card cards: "{H} «H» 1:0«A» {A}"
    home_e = _team_emoji(game.home_team)
    away_e = _team_emoji(game.away_team)
    score = (
        f"{home_e} «{_esc(_team_name_kz(game.home_team))}» "
        f"{_score(game.home_score, game.away_score)}"
        f"«{_esc(_team_name_kz(game.away_team))}»{away_e}"
    )
    scorer_team_emoji = _team_emoji_for_event(event, game)

    surname = _esc(_surname(event.player_name))

    if event.event_type in GOAL_TYPES:
        tag = _goal_tag(event)
        text = (
            f"{GOAL_EMOJI_HTML}ГООООЛ\n\n"
            f"{scorer_team_emoji} {surname} {event.minute}'{tag}\n\n"
            f"{score}"
        )
    else:  # red / second_yellow
        text = (
            f"🟥ҚЫЗЫЛ\n\n"
            f"{scorer_team_emoji} {surname} {event.minute}'\n\n"
            f"{score}"
        )

    # Reply-thread the goal/card under the match's lineup post.
    reply_to = game.lineup_telegram_message_id

    # Send text immediately. Video, when available, is attached later as a
    # reply to this message via post_goal_video().
    msg_id = await send_public_telegram_message(text, reply_to=reply_to)
    if not msg_id:
        return False

    event.telegram_sent_at = utcnow()
    event.telegram_message_id = msg_id
    await db.commit()

    # If video already present, queue the video attach on its dedicated worker.
    if getattr(event, "video_url", None):
        try:
            from app.tasks.telegram_tasks import post_goal_video_task

            post_goal_video_task.delay(event.id)
        except Exception:
            logger.exception("failed to enqueue goal video for event %s", event.id)

    return True


async def _load_goal_video_event(db: AsyncSession, event_id: int) -> GameEvent | None:
    q = (
        select(GameEvent)
        .where(GameEvent.id == event_id)
        .options(
            selectinload(GameEvent.game).selectinload(Game.home_team),
            selectinload(GameEvent.game).selectinload(Game.away_team),
        )
    )
    return (await db.execute(q)).scalar_one_or_none()


def _goal_video_caption_html(event: GameEvent, game: Game) -> str:
    home_e = _team_emoji(game.home_team)
    away_e = _team_emoji(game.away_team)
    score = (
        f"{home_e}«{_esc(_team_name_kz(game.home_team))}» "
        f"{_score(game.home_score, game.away_score)}"
        f"«{_esc(_team_name_kz(game.away_team))}»{away_e}"
    )
    scorer_team_emoji = _team_emoji_for_event(event, game)
    surname = _esc(_surname(event.player_name))
    tag = _goal_tag(event)
    return (
        f"{GOAL_EMOJI_HTML}ГООООЛ\n\n"
        f"{scorer_team_emoji} {surname} {event.minute}'{tag}\n\n"
        f"{score}"
    )


async def _post_goal_video_with_file(
    db: AsyncSession,
    event: GameEvent,
    file_path: str,
) -> bool:
    from app.services.telegram_user_client import edit_public_user_message_media

    if event is None:
        return False
    if event.event_type not in GOAL_TYPES:
        return False
    if event.telegram_video_sent_at is not None:
        return False
    if not event.telegram_message_id:
        return False
    game = event.game
    if game is None:
        return False
    ok = await edit_public_user_message_media(
        event.telegram_message_id,
        file_path,
        caption_html=_goal_video_caption_html(event, game),
    )

    if not ok:
        return False

    event.telegram_video_sent_at = utcnow()
    await db.commit()
    return True


async def post_goal_video_from_file(
    db: AsyncSession,
    event_id: int,
    file_path: str | Path,
) -> bool:
    """Attach the goal clip using a local file already present on this host."""
    event = await _load_goal_video_event(db, event_id)
    return await _post_goal_video_with_file(db, event, str(file_path))


async def post_goal_video(db: AsyncSession, event_id: int) -> bool:
    """Attach the goal clip to the existing text goal post via edit_message."""
    import httpx
    import tempfile

    event = await _load_goal_video_event(db, event_id)
    if event is None:
        return False
    video_url = getattr(event, "video_url", None)
    if not video_url:
        return False

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        async with httpx.AsyncClient(timeout=300, follow_redirects=True) as c:
            async with c.stream("GET", video_url) as r:
                r.raise_for_status()
                with tmp_path.open("wb") as f:
                    async for chunk in r.aiter_bytes(chunk_size=1 << 20):
                        f.write(chunk)

        return await _post_goal_video_with_file(db, event, str(tmp_path))
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


# ---------------------------------------------------------------------- #
#  Scenario 4 — Match finish                                              #
# ---------------------------------------------------------------------- #

async def post_match_finish(db: AsyncSession, game_id: int) -> bool:
    q = (
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
        )
    )
    game = (await db.execute(q)).scalar_one_or_none()
    if game is None or game.finish_telegram_sent_at is not None:
        return False

    events_q = (
        select(GameEvent).where(
            GameEvent.game_id == game_id,
            GameEvent.event_type.in_([
                GameEventType.goal, GameEventType.penalty, GameEventType.own_goal,
            ]),
        ).order_by(GameEvent.half, GameEvent.minute)
    )
    events = (await db.execute(events_q)).scalars().all()

    # Tight score line (same style as goal post)
    home_e = _team_emoji(game.home_team)
    away_e = _team_emoji(game.away_team)
    score_line = (
        f"{home_e}«{_esc(_team_name_kz(game.home_team))}» "
        f"{_score(game.home_score, game.away_score)}"
        f"«{_esc(_team_name_kz(game.away_team))}»{away_e}"
    )

    parts: list[str] = ["🏁<b>МАТЧ АЯҚТАЛДЫ</b>", ""]
    # Optional recap (AI-generated preview_kz reused if set)
    recap = (game.preview_kz or "").strip()
    if recap:
        parts.append(f"⚡ <b>{_esc(recap)}</b>")
        parts.append("")
    parts.append(score_line)

    if events:
        parts.append("")
        parts.append("📌Гол авторлары:")
        parts.append("")
        for ev in events:
            surname = _esc(_surname(ev.player_name))
            suffix = ""
            if ev.event_type == GameEventType.penalty:
                suffix = " (пен.)"
            elif ev.event_type == GameEventType.own_goal:
                suffix = " (автогол)"
            parts.append(f"{GOAL_EMOJI_HTML}{surname} {ev.minute}'{suffix}")

    text = "\n".join(parts)

    reply_to = game.lineup_telegram_message_id
    ok = await send_public_telegram_message(text, reply_to=reply_to)
    if not ok:
        return False

    game.finish_telegram_sent_at = utcnow()
    await db.commit()
    return True


# ---------------------------------------------------------------------- #
#  Scenario 5 — Pre-game lineups                                          #
# ---------------------------------------------------------------------- #

PREGAME_WINDOW_MINUTES = 20


async def post_pregame_lineup(db: AsyncSession, game_id: int) -> bool:
    import tempfile
    from pathlib import Path
    from app.services.lineup_renderer import render_lineup_field_png
    from app.services.telegram_user_client import send_public_user_photo
    from app.services.game_lifecycle import _revalidate_match_page

    q = (
        select(Game)
        .where(Game.id == game_id)
        .options(
            selectinload(Game.home_team),
            selectinload(Game.away_team),
            selectinload(Game.stadium_rel),
            selectinload(Game.lineups).selectinload(GameLineup.player),
        )
    )
    game = (await db.execute(q)).scalar_one_or_none()
    if game is None:
        return False
    if game.status != GameStatus.created:
        return False
    if not game.has_lineup or not game.lineups:
        return False
    if not _within_pregame_window(game):
        return False

    starters = [l for l in game.lineups if l.lineup_type == LineupType.starter]
    if not starters:
        return False
    new_hash = _hash_lineup(starters)
    if game.lineup_telegram_hash == new_hash and game.lineup_telegram_sent_at is not None:
        return False

    # Invalidate the public match page cache so the screenshot captures fresh lineup.
    try:
        await _revalidate_match_page(game_id)
    except Exception:
        logger.warning("ISR revalidate failed for game %s (non-fatal)", game_id)

    # Render the field image from the public site
    tmp_dir = Path(tempfile.gettempdir()) / "qfl_lineups"
    tmp_dir.mkdir(exist_ok=True)
    img_path = tmp_dir / f"game_{game_id}.png"
    try:
        rendered = await render_lineup_field_png(game_id, img_path)
    except Exception:
        logger.exception("lineup render failed for game %s", game_id)
        rendered = None

    # Caption template (KZ):
    #   {home_emoji}«Home» 🆚 «Away»{away_emoji}
    #
    #   📌Бастапқы құрам:
    #
    #   {LIVE}Тікелей эфир сілтемесі
    #   📱{KFF League — hyperlink to youtube_live_url}
    home_kz = _team_name_kz(game.home_team)
    away_kz = _team_name_kz(game.away_team)
    caption_lines: list[str] = [
        f"{_team_emoji(game.home_team)}«{_esc(home_kz)}» 🆚 «{_esc(away_kz)}»{_team_emoji(game.away_team)}",
        "",
        "📌Бастапқы құрам:",
    ]
    if game.youtube_live_url:
        caption_lines += [
            "",
            f"{LIVE_EMOJI_HTML}<b>Тікелей эфир сілтемесі</b>",
            "",
            f'{YOUTUBE_EMOJI_HTML}<a href="{_esc(game.youtube_live_url)}">KFF League YouTube channel</a>',
        ]
    caption = "\n".join(caption_lines)

    if rendered:
        msg_id = await send_public_user_photo(str(rendered), caption)
    else:
        # Fallback: text-only lineup block
        home_lineups = [l for l in game.lineups if l.team_id == game.home_team_id]
        away_lineups = [l for l in game.lineups if l.team_id == game.away_team_id]
        players_by_id = {l.player_id: l.player for l in game.lineups if l.player}
        text = (
            caption
            + "\n\n"
            + _format_lineup_block(game.home_team, game.home_formation, home_lineups, players_by_id)
            + "\n\n"
            + _format_lineup_block(game.away_team, game.away_formation, away_lineups, players_by_id)
        )
        msg_id = await send_public_telegram_message(text)

    if not msg_id:
        return False

    now = utcnow()
    game.lineup_telegram_sent_at = now
    game.lineup_telegram_hash = new_hash
    game.lineup_telegram_message_id = msg_id
    await db.commit()
    return True


def _within_pregame_window(game: Game) -> bool:
    """Kickoff (game.date/time) is Asia/Almaty local.

    Compare against Asia/Almaty-local "now", not UTC.
    """
    if game.date is None or game.time is None:
        return False
    try:
        from zoneinfo import ZoneInfo
    except ImportError:  # pragma: no cover
        from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]
    tz = ZoneInfo("Asia/Almaty")
    kickoff_local = datetime.combine(game.date, game.time, tzinfo=tz)
    now_local = datetime.now(tz)
    delta = kickoff_local - now_local
    return timedelta(minutes=-5) <= delta <= timedelta(minutes=PREGAME_WINDOW_MINUTES)
