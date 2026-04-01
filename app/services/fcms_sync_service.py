"""FCMS sync service: pre-match lineup fetch (PDF) and post-match protocol PDF download."""

import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import Game, GameLineup, GameStatus, LineupType, Player, PlayerTeam, Team
from app.services.fcms_client import FcmsClient
from app.services.file_storage import FileStorageService
from app.services.telegram import send_telegram_document, send_telegram_message
from app.utils.fcms_pdf_parser import parse_pre_match_lineup, extract_attendance_from_match_report
from app.utils.timestamps import utcnow

settings = get_settings()
logger = logging.getLogger(__name__)

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
        """Finished games within 24h that have fcms_match_id but no protocol yet."""
        cutoff = utcnow() - timedelta(hours=24)

        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.status == GameStatus.finished,
                    Game.finished_at >= cutoff,
                    Game.fcms_match_id.isnot(None),
                    Game.protocol_url.is_(None),
                    Game.fcms_protocol_synced_at.is_(None),
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

            for lineup_type_str, players in [
                ("starter", side_data.get("starters", [])),
                ("substitute", side_data.get("substitutes", [])),
            ]:
                lineup_type = LineupType.starter if lineup_type_str == "starter" else LineupType.substitute

                for p in players:
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

                    stmt = insert(GameLineup).values(
                        game_id=game_id,
                        team_id=team_id,
                        player_id=player_id,
                        lineup_type=lineup_type,
                        shirt_number=shirt_number,
                        is_captain=False,
                        amplua=amplua,
                    )
                    stmt = stmt.on_conflict_do_update(
                        constraint="uq_game_lineup_player",
                        set_={
                            "lineup_type": lineup_type,
                            "shirt_number": shirt_number,
                            "amplua": amplua,
                        },
                    )
                    await self.db.execute(stmt)
                    side_count += 1
                    total_lineup += 1

            if side == "home":
                home_count = side_count
            else:
                away_count = side_count

        if total_lineup > 0:
            game.has_lineup = True
            game.lineup_source = "fcms"

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
        if shirt_number and season_id:
            result = await self.db.execute(
                select(Player.id, PlayerTeam.amplua)
                .join(PlayerTeam, PlayerTeam.player_id == Player.id)
                .where(
                    PlayerTeam.team_id == team_id,
                    PlayerTeam.season_id == season_id,
                    PlayerTeam.number == shirt_number,
                    PlayerTeam.is_active == True,
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

        # Upload to MinIO
        filename = f"protocol_game_{game_id}.pdf"
        upload_result = await FileStorageService.upload_file(
            pdf_bytes,
            filename,
            "application/pdf",
            category="protocol_pdfs",
        )
        object_name = upload_result["object_name"]

        game.protocol_url = object_name
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

        # Send Telegram notification
        try:
            await send_telegram_message(
                f"📋 Протокол матча загружен\n\n"
                f"🆔 Game #{game_id}\n"
                f"📄 {object_name}\n"
                f"👥 Посещаемость: {game.visitors or 'N/A'}"
            )
        except Exception:
            logger.warning("Failed to send protocol notification for game %d", game_id, exc_info=True)

        logger.info("FCMS protocol PDF uploaded for game %d: %s", game_id, object_name)
        return {"status": "uploaded", "object_name": object_name}
