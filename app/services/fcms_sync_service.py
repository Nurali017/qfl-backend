"""FCMS sync service: pre-match lineup fetch (PDF), post-match protocol PDF download, and event sync."""

import hashlib
import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import Game, GameEvent, GameEventType, GameLineup, GameStatus, LineupType, Player, PlayerTeam, Team
from app.services.fcms_client import FcmsClient
from app.services.file_storage import FileStorageService
from app.services.telegram import send_telegram_document, send_telegram_message
from app.utils.game_event_assists import sync_event_assist
from app.utils.fcms_pdf_parser import parse_pre_match_lineup, extract_attendance_from_match_report
from app.utils.timestamps import utcnow

settings = get_settings()
logger = logging.getLogger(__name__)


def _pdf_text_hash(pdf_bytes: bytes) -> str:
    """SHA-256 of extracted PDF text (ignores metadata/timestamps).

    Strips 'Report Date: ...' lines since FCMS embeds current timestamp
    in the PDF text on every download.
    Falls back to raw byte hash if text extraction fails.
    """
    try:
        import re
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "".join(page.get_text() for page in doc)
        doc.close()
        text = re.sub(r"Report Date:.*", "", text)
        return hashlib.sha256(text.encode()).hexdigest()
    except Exception:
        logger.warning("PDF text extraction failed, falling back to raw byte hash")
        return hashlib.sha256(pdf_bytes).hexdigest()

ALMATY_TZ = ZoneInfo("Asia/Almaty")


class FcmsSyncService:
    """Sync service for FCMS data: lineups (from PDF) and protocol PDFs."""

    def __init__(self, db: AsyncSession, client: FcmsClient):
        self.db = db
        self.client = client

    # ── Query helpers ────────────────────────────────────────────────

    async def get_games_for_fcms_lineup(self) -> list[Game]:
        """Games starting within 90 min that have fcms_match_id but no FCMS lineup yet."""
        from datetime import datetime

        now = datetime.now(ALMATY_TZ)
        today = now.date()
        current_time = now.time()
        latest_time = (now + timedelta(minutes=90)).time()

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.date == today,
                    Game.time.isnot(None),
                    Game.time >= current_time,
                    Game.time <= latest_time,
                    Game.status == GameStatus.created,
                    Game.fcms_match_id.isnot(None),
                    Game.sync_disabled == False,
                    Game.is_schedule_tentative == False,
                    or_(Game.lineup_source != "fcms", Game.lineup_source.is_(None)),
                )
            )
        )
        return list(result.scalars().all())

    async def get_games_for_fcms_protocol(self) -> list[Game]:
        """Finished games 3-24h ago that have fcms_match_id.

        Waits at least 3h after match end (FCMS needs time to generate PDF),
        then keeps checking for updates up to 24h (PDF may be revised).
        """
        now = utcnow()
        cutoff_24h = now - timedelta(hours=24)
        cutoff_3h = now - timedelta(hours=3)

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.status == GameStatus.finished,
                    Game.finished_at >= cutoff_24h,
                    Game.finished_at <= cutoff_3h,
                    Game.fcms_match_id.isnot(None),
                    Game.sync_disabled == False,
                )
            )
        )
        return list(result.scalars().all())

    # ── Pre-match lineup (from PDF) ──────────────────────────────────

    async def sync_fcms_lineup(self, game_id: int) -> dict:
        """Download pre-match report PDF from FCMS, parse lineups, upsert into game_lineups."""
        game = await self.db.get(Game, game_id)
        if not game or not game.fcms_match_id:
            return {"error": f"Game {game_id} not found or no fcms_match_id"}

        # Download pre-match report PDF
        pdf_bytes = await self.client.get_pre_match_report_pdf(game.fcms_match_id)
        if pdf_bytes is None:
            logger.info("FCMS pre-match report not available yet for game %d (fcms=%d)", game_id, game.fcms_match_id)
            return {"status": "pdf_not_available_yet"}

        # Deduplicate: skip if PDF unchanged since last sync
        pdf_hash = _pdf_text_hash(pdf_bytes)
        if game.prematch_pdf_hash == pdf_hash:
            logger.debug("FCMS pre-match PDF unchanged for game %d (hash=%s)", game_id, pdf_hash[:12])
            return {"status": "unchanged"}

        # Parse lineup from PDF
        lineup_data = parse_pre_match_lineup(pdf_bytes)

        total_lineup = 0
        home_count = 0
        away_count = 0

        for side, team_id in [("home", game.home_team_id), ("away", game.away_team_id)]:
            if not team_id:
                continue

            side_data = lineup_data.get(side, {})
            side_count = 0
            matched_player_ids: set[int] = set()

            for lineup_type_str, players in [
                ("starter", side_data.get("starters", [])),
                ("substitute", side_data.get("substitutes", [])),
            ]:
                lineup_type = LineupType.starter if lineup_type_str == "starter" else LineupType.substitute

                for idx, p in enumerate(players):
                    shirt_number = p["shirt_number"]
                    name = p["name"]

                    player_id, amplua = await self._resolve_player_by_number_and_name(
                        shirt_number, name, team_id, game.season_id,
                    )
                    if player_id is None:
                        logger.warning(
                            "FCMS player not matched: #%s %s team=%d game=%d",
                            shirt_number, name, team_id, game_id,
                        )
                        continue

                    # FCMS protocol: first starter is always GK, second is captain
                    if lineup_type_str == "starter" and idx == 0:
                        amplua = "Gk"
                    is_captain = lineup_type_str == "starter" and idx == 1

                    stmt = insert(GameLineup).values(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        lineup_type=lineup_type,
                        shirt_number=shirt_number,
                        is_captain=is_captain,
                        amplua=amplua,
                    )
                    stmt = stmt.on_conflict_do_update(
                        constraint="uq_game_lineup_player",
                        set_={
                            "lineup_type": lineup_type,
                            "shirt_number": shirt_number,
                            "is_captain": is_captain,
                            "amplua": amplua,
                        },
                    )
                    await self.db.execute(stmt)
                    matched_player_ids.add(player_id)
                    side_count += 1
                    total_lineup += 1

            # Remove stale lineup records (e.g. leftover from earlier SOTA sync)
            if matched_player_ids:
                await self.db.execute(
                    GameLineup.__table__.delete().where(
                        GameLineup.game_id == game_id,
                        GameLineup.team_id == team_id,
                        GameLineup.player_id.notin_(matched_player_ids),
                    )
                )

            if side == "home":
                home_count = side_count
            else:
                away_count = side_count

        if total_lineup > 0:
            game.has_lineup = True
            game.lineup_source = "fcms"

        # Only cache PDF hash if both teams matched; partial match → retry next run
        if home_count > 0 and away_count > 0:
            game.prematch_pdf_hash = pdf_hash
        else:
            logger.warning(
                "FCMS lineup incomplete for game %d (home=%d, away=%d) — hash NOT cached, will retry",
                game_id, home_count, away_count,
            )
        await self.db.commit()

        logger.info(
            "FCMS lineup synced for game %d: %d players (home=%d, away=%d)",
            game_id, total_lineup, home_count, away_count,
        )

        # Send PDF to Telegram
        try:
            home_team = await self.db.get(Team, game.home_team_id) if game.home_team_id else None
            away_team = await self.db.get(Team, game.away_team_id) if game.away_team_id else None
            home_name = home_team.name if home_team else "?"
            away_name = away_team.name if away_team else "?"
            caption = f"📋 Предматчевый PDF\n{home_name} vs {away_name}\n{game.date} {game.time or ''}"
            filename = f"prematch_{home_name}_vs_{away_name}_{game.date}.pdf"
            await send_telegram_document(pdf_bytes, filename, caption)
        except Exception:
            logger.exception("Failed to send pre-match PDF to Telegram for game %d", game_id)

        return {
            "game_id": game_id,
            "lineup_count": total_lineup,
            "home_count": home_count,
            "away_count": away_count,
        }

    # PlayerTeam.amplua (int) → GameLineup.amplua (str)
    _AMPLUA_MAP = {1: "Gk", 2: "D", 3: "M", 4: "F"}

    async def _resolve_player_by_number_and_name(
        self, shirt_number: int, full_name: str, team_id: int, season_id: int | None,
    ) -> tuple[int | None, str | None]:
        """Resolve player from PDF data (shirt number + full name) to (player_id, amplua).

        Strategy 1: shirt_number + team + season (most reliable)
        Strategy 2: last_name match + team + season (fallback for number mismatches)
        """
        # Strategy 1: Match by shirt number + team + season
        # Exclude hidden contracts to avoid matching stale players when a new
        # contract is created under the same shirt number (former holder is
        # soft-hidden but still is_active=True).
        if shirt_number and season_id:
            result = await self.db.execute(
                select(Player.id, PlayerTeam.amplua)
                .join(PlayerTeam, PlayerTeam.player_id == Player.id)
                .where(
                    PlayerTeam.team_id == team_id,
                    PlayerTeam.season_id == season_id,
                    PlayerTeam.number == shirt_number,
                    PlayerTeam.is_active == True,
                    PlayerTeam.is_hidden == False,
                )
            )
            row = result.first()
            if row:
                return row[0], self._AMPLUA_MAP.get(row[1])

        # Strategy 2: Match by name (try last word as last_name)
        if full_name and season_id:
            parts = full_name.strip().split()
            if len(parts) >= 2:
                first_name = parts[0]
                last_name = parts[-1]

                result = await self.db.execute(
                    select(Player.id, PlayerTeam.amplua)
                    .join(PlayerTeam, PlayerTeam.player_id == Player.id)
                    .where(
                        PlayerTeam.team_id == team_id,
                        PlayerTeam.season_id == season_id,
                        PlayerTeam.is_active == True,
                        PlayerTeam.is_hidden == False,
                        or_(
                            and_(Player.first_name == first_name, Player.last_name == last_name),
                            and_(Player.last_name == first_name, Player.first_name == last_name),
                            and_(Player.first_name_kz == first_name, Player.last_name_kz == last_name),
                            and_(Player.last_name_kz == first_name, Player.first_name_kz == last_name),
                        ),
                    )
                )
                row = result.first()
                if row:
                    return row[0], self._AMPLUA_MAP.get(row[1])

            # Try single-name match by last_name only (for unique names)
            for name_part in parts:
                result = await self.db.execute(
                    select(Player.id, PlayerTeam.amplua)
                    .join(PlayerTeam, PlayerTeam.player_id == Player.id)
                    .where(
                        PlayerTeam.team_id == team_id,
                        PlayerTeam.season_id == season_id,
                        PlayerTeam.is_active == True,
                        PlayerTeam.is_hidden == False,
                        or_(Player.last_name == name_part, Player.last_name_kz == name_part),
                    )
                )
                rows = result.all()
                if len(rows) == 1:
                    return rows[0][0], self._AMPLUA_MAP.get(rows[0][1])

        return None, None

    # ── Post-match protocol PDF ──────────────────────────────────────

    async def sync_fcms_protocol_pdf(self, game_id: int) -> dict:
        """Download match report PDF from FCMS and upload to MinIO."""
        game = await self.db.get(Game, game_id)
        if not game or not game.fcms_match_id:
            return {"error": f"Game {game_id} not found or no fcms_match_id"}

        # Check match status in FCMS
        match_data = await self.client.get_match(game.fcms_match_id)
        fcms_status = (match_data.get("status") or "").upper()
        if fcms_status not in ("CLOSED", "COMPLETE", "COMPLETED", "FINISHED"):
            logger.debug("FCMS match %d status=%s, not ready for protocol", game.fcms_match_id, fcms_status)
            return {"status": "match_not_completed", "fcms_status": fcms_status}

        # Download match report PDF
        pdf_bytes = await self.client.get_match_report_pdf(game.fcms_match_id)
        if pdf_bytes is None:
            return {"status": "pdf_not_available_yet"}

        # Check if PDF changed since last sync
        pdf_hash = _pdf_text_hash(pdf_bytes)

        # For legacy rows without hash, compute hash from stored MinIO object to compare fairly
        if game.protocol_url and not game.protocol_pdf_hash:
            try:
                from app.utils.file_urls import to_object_name
                result = await FileStorageService.get_file(to_object_name(game.protocol_url))
                if result is None:
                    raise FileNotFoundError("Stored protocol not found in MinIO")
                stored_bytes, _ = result
                game.protocol_pdf_hash = _pdf_text_hash(stored_bytes)
                logger.info("Backfilled protocol hash for game %d from stored object", game_id)
            except Exception:
                logger.warning("Could not read stored protocol for game %d, will re-upload", game_id, exc_info=True)

        if game.protocol_pdf_hash == pdf_hash:
            game.fcms_protocol_synced_at = utcnow()
            await self.db.commit()
            logger.debug("FCMS protocol unchanged for game %d (hash=%s)", game_id, pdf_hash[:12])
            return {"status": "unchanged"}

        is_update = game.protocol_url is not None
        old_object_name = game.protocol_url

        # Upload new PDF first (ensure replacement is durable before deleting old)
        filename = f"protocol_game_{game_id}.pdf"
        upload_result = await FileStorageService.upload_file(
            pdf_bytes,
            filename,
            "application/pdf",
            category="protocol_pdfs",
        )
        object_name = upload_result["object_name"]

        game.protocol_url = object_name
        game.protocol_pdf_hash = pdf_hash
        game.fcms_protocol_synced_at = utcnow()

        # Extract attendance directly from PDF bytes (no need to re-download from MinIO)
        try:
            attendance = extract_attendance_from_match_report(pdf_bytes)
            if attendance is not None:
                game.visitors = attendance
                logger.info("Extracted attendance %d for game %d", attendance, game_id)
        except Exception:
            logger.warning("Failed to extract attendance for game %d", game_id, exc_info=True)

        await self.db.commit()

        # Best-effort cleanup of old object after DB commit
        if old_object_name and old_object_name != object_name:
            try:
                from app.utils.file_urls import to_object_name
                await FileStorageService.delete_file(to_object_name(old_object_name))
            except Exception:
                logger.warning("Failed to delete old protocol for game %d: %s", game_id, old_object_name, exc_info=True)

        # Send Telegram notification
        action = "обновлён" if is_update else "загружен"
        try:
            await send_telegram_message(
                f"📋 Протокол матча {action}\n\n"
                f"🆔 Game #{game_id}\n"
                f"📄 {object_name}\n"
                f"👥 Посещаемость: {game.visitors or 'N/A'}"
            )
        except Exception:
            logger.warning("Failed to send protocol notification for game %d", game_id, exc_info=True)

        logger.info("FCMS protocol PDF %s for game %d: %s", action, game_id, object_name)

        # Sync events from FCMS for games without SOTA (SOTA has priority)
        if not game.sota_id:
            try:
                events_result = await self.sync_fcms_events(game.id)
                logger.info("FCMS events synced for game %d: %s", game_id, events_result)
            except Exception:
                logger.warning("Failed to sync FCMS events for game %d", game_id, exc_info=True)

        return {"status": "updated" if is_update else "uploaded", "object_name": object_name}

    # ── FCMS Event Sync ─────────────────────────────────────────────

    # FCMS eventType+eventSubtype → GameEventType
    _FCMS_EVENT_MAP: dict[tuple[str, str | None], GameEventType] = {
        ("GOAL", "SCORING"): GameEventType.goal,
        ("GOAL", "OWN_GOAL"): GameEventType.own_goal,
        ("GOAL", "PENALTY"): GameEventType.penalty,
        ("GOAL", "MISSED_PENALTY"): GameEventType.missed_penalty,
        ("CARD", "YELLOW"): GameEventType.yellow_card,
        ("CARD", "YELLOW_RED"): GameEventType.second_yellow,
        ("CARD", "RED"): GameEventType.red_card,
        ("SUBSTITUTION", None): GameEventType.substitution,
    }

    async def sync_fcms_events(self, game_id: int) -> dict:
        """Sync events from FCMS matchEvents API for games without sota_id.

        Full reconciliation: adds new events, updates changed ones,
        deletes FCMS events that no longer exist. Manual/SOTA events are protected.
        """
        game = await self.db.get(Game, game_id)
        if not game or not game.fcms_match_id:
            return {"error": f"Game {game_id} not found or no fcms_match_id", "added": 0, "updated": 0, "deleted": 0}

        # Get match data for competitor IDs
        match_data = await self.client.get_match(game.fcms_match_id)
        home_competitor_id = match_data.get("homeCompetitorId")
        away_competitor_id = match_data.get("awayCompetitorId")

        if not home_competitor_id or not away_competitor_id:
            return {"error": "Missing competitor IDs in FCMS match data", "added": 0, "updated": 0, "deleted": 0}

        # Build matchPlayerId → personId mapping from both teams
        mp_to_person: dict[int, int] = {}
        mp_to_name: dict[int, str] = {}
        mp_to_team_id: dict[int, int] = {}
        for comp_id, team_id_for_comp in (
            (home_competitor_id, game.home_team_id),
            (away_competitor_id, game.away_team_id),
        ):
            try:
                match_players = await self.client.get_match_players(game.fcms_match_id, comp_id)
            except Exception:
                logger.warning("Failed to fetch matchPlayers for comp %d game %d", comp_id, game_id, exc_info=True)
                continue
            for mp in match_players:
                mp_id = mp["id"]
                cp = mp.get("competitorPlayer", {})
                player_data = cp.get("player", {})
                person_id = player_data.get("personId")
                if person_id:
                    mp_to_person[mp_id] = person_id
                fn = player_data.get("localFirstName", "")
                ln = player_data.get("localFamilyName", "")
                mp_to_name[mp_id] = f"{fn} {ln}".strip()
                if team_id_for_comp:
                    mp_to_team_id[mp_id] = team_id_for_comp

        # Build personId → our player_id lookup
        all_person_ids = list(mp_to_person.values())
        person_to_player: dict[int, tuple[int, str | None]] = {}
        if all_person_ids:
            result = await self.db.execute(
                select(Player.id, Player.fcms_person_id, Player.first_name, Player.last_name)
                .where(Player.fcms_person_id.in_(all_person_ids))
            )
            for row in result.all():
                person_to_player[row[1]] = (row[0], f"{row[2] or ''} {row[3] or ''}".strip())

        # Fetch events from FCMS
        fcms_events = await self.client.get_match_events(game.fcms_match_id)

        # Load existing FCMS events from DB
        result = await self.db.execute(
            select(GameEvent).where(GameEvent.game_id == game_id, GameEvent.source == "fcms")
        )
        existing_fcms_events = list(result.scalars().all())

        # Index existing by fcms signature (event_type, half, minute, player_id)
        def _sig(et_value: str, half: int, minute: int, player_id: int | None) -> tuple:
            return (et_value, half, minute, player_id)

        existing_by_sig: dict[tuple, list[GameEvent]] = {}
        for e in existing_fcms_events:
            sig = _sig(e.event_type.value, e.half, e.minute, e.player_id)
            existing_by_sig.setdefault(sig, []).append(e)

        matched_db_ids: set[int] = set()
        added = 0
        updated = 0

        for fe in fcms_events:
            event_type_str = fe.get("eventType", "")
            event_subtype = fe.get("eventSubtype")
            game_event_type = self._FCMS_EVENT_MAP.get((event_type_str, event_subtype))
            if game_event_type is None:
                # Try without subtype for SUBSTITUTION
                game_event_type = self._FCMS_EVENT_MAP.get((event_type_str, None))
            if game_event_type is None:
                continue

            half = fe.get("periodNumber", 1)
            minute = fe.get("matchActualTime", 0)
            origin = fe.get("matchPlayerOrigin")

            # Determine team_id
            team_id = None
            if origin == "HOME":
                team_id = game.home_team_id
            elif origin == "AWAY":
                team_id = game.away_team_id

            # Resolve primary player
            mp_id = fe.get("matchPlayerId")

            # Fallback: FCMS иногда не присылает matchPlayerOrigin (чаще всего на
            # карточках) — определяем команду по matchPlayerId / matchPlayerOutId /
            # matchPlayerInId из загруженных составов обеих команд.
            if team_id is None:
                for candidate_mp_id in (
                    mp_id,
                    fe.get("matchPlayerOutId"),
                    fe.get("matchPlayerInId"),
                ):
                    if candidate_mp_id and candidate_mp_id in mp_to_team_id:
                        team_id = mp_to_team_id[candidate_mp_id]
                        break

            player_id = None
            player_name = None
            if mp_id:
                person_id = mp_to_person.get(mp_id)
                if person_id and person_id in person_to_player:
                    player_id, player_name = person_to_player[person_id]
                else:
                    player_name = mp_to_name.get(mp_id)

            # Resolve player2 (substitution: in/out)
            player2_id = None
            player2_name = None
            if game_event_type == GameEventType.substitution:
                # For FCMS: matchPlayerOutId = player going off, matchPlayerInId = player coming on
                out_mp_id = fe.get("matchPlayerOutId")
                in_mp_id = fe.get("matchPlayerInId")
                # In our model: player = going off, player2 = coming on
                if out_mp_id:
                    out_person = mp_to_person.get(out_mp_id)
                    if out_person and out_person in person_to_player:
                        player_id, player_name = person_to_player[out_person]
                    else:
                        player_name = mp_to_name.get(out_mp_id)
                if in_mp_id:
                    in_person = mp_to_person.get(in_mp_id)
                    if in_person and in_person in person_to_player:
                        player2_id, player2_name = person_to_player[in_person]
                    else:
                        player2_name = mp_to_name.get(in_mp_id)

            # Resolve assist
            assist_player_id = None
            assist_player_name = None
            assist_mp_id = fe.get("matchPlayerAssistedId")
            if assist_mp_id:
                assist_person = mp_to_person.get(assist_mp_id)
                if assist_person and assist_person in person_to_player:
                    assist_player_id, assist_player_name = person_to_player[assist_person]
                else:
                    assist_player_name = mp_to_name.get(assist_mp_id)

            # Try to match existing FCMS event
            sig = _sig(game_event_type.value, half, minute, player_id)
            matched = None
            candidates = existing_by_sig.get(sig, [])
            for c in candidates:
                if c.id not in matched_db_ids:
                    matched = c
                    break

            fields = {
                "event_type": game_event_type,
                "half": half,
                "minute": minute,
                "team_id": team_id,
                "player_id": player_id,
                "player_name": player_name,
                "player2_id": player2_id,
                "player2_name": player2_name,
            }
            assist_info = None
            if assist_player_id is not None or assist_player_name:
                assist_info = {
                    "player_id": assist_player_id,
                    "player_name": assist_player_name,
                }

            if matched:
                matched_db_ids.add(matched.id)
                for field, value in fields.items():
                    old_value = getattr(matched, field)
                    if field == "event_type":
                        if old_value != value:
                            setattr(matched, field, value)
                            updated += 1
                    elif old_value != value:
                        setattr(matched, field, value)
                        updated += 1
                before_assist = (
                    matched.assist_player_id,
                    matched.assist_player_name,
                    matched.assist_manual_override,
                )
                sync_event_assist(matched, assist_info)
                after_assist = (
                    matched.assist_player_id,
                    matched.assist_player_name,
                    matched.assist_manual_override,
                )
                if before_assist != after_assist:
                    updated += 1
            else:
                event = GameEvent(
                    game_id=game_id,
                    source="fcms",
                    assist_player_id=assist_player_id,
                    assist_player_name=assist_player_name,
                    **fields,
                )
                self.db.add(event)
                added += 1

        # Delete unmatched FCMS events
        deleted = 0
        for e in existing_fcms_events:
            if e.id not in matched_db_ids:
                await self.db.delete(e)
                deleted += 1

        await self.db.commit()

        logger.info(
            "Game %s: FCMS events added=%d updated=%d deleted=%d",
            game_id, added, updated, deleted,
        )
        return {"added": added, "updated": updated, "deleted": deleted}
