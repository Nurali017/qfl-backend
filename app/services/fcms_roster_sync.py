"""FCMS Roster Sync service.

Syncs player rosters from FCMS for all configured competitions.
FCMS is the source of truth — players missing from FCMS are auto-deactivated.
Coaches (role != 1) are excluded from sync entirely.
"""

import hashlib
import logging
from datetime import date as date_type, datetime, timezone

from sqlalchemy import select, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models.country import Country
from app.models.fcms_roster_sync_log import FcmsRosterSyncLog
from app.models.player import Player
from app.models.player_team import PlayerTeam
from app.models.team import Team
from app.services.fcms_client import FcmsClient
from app.services.telegram import send_telegram_message

logger = logging.getLogger(__name__)

# Competition name lookup
COMP_NAMES = {
    3517: "Премьер-Лига 2026",
    3585: "Первая Лига 2026",
    3596: "Вторая Лига 2026 (ЮЗ)",
    3597: "Вторая Лига 2026 (СВ)",
}


def _name_key(ln: str | None, fn: str | None):
    if ln and fn:
        return (ln.strip().lower(), fn.strip().lower())
    return None


def _make_item_key(person_id: int | None, fn_ru: str, ln_ru: str, dob: str) -> str:
    """Generate stable key for a new_player item."""
    if person_id:
        return f"person_{person_id}"
    raw = f"{fn_ru}{ln_ru}{dob}"
    return f"hash_{hashlib.md5(raw.encode()).hexdigest()}"


class FcmsRosterSyncService:
    def __init__(self, db: AsyncSession, client: FcmsClient):
        self.db = db
        self.client = client

    async def sync_all_competitions(self, triggered_by: str = "celery_beat") -> list[dict]:
        """Sync rosters for all configured competitions."""
        settings = get_settings()
        comp_map = self._parse_competition_map(settings.fcms_competition_season_map)
        all_logs = []

        for comp_id, season_id in comp_map:
            comp_name = COMP_NAMES.get(comp_id, f"Competition {comp_id}")
            log = FcmsRosterSyncLog(
                competition_name=comp_name,
                competition_id=comp_id,
                season_id=season_id,
                status="running",
                triggered_by=triggered_by,
                started_at=datetime.now(timezone.utc),
            )
            self.db.add(log)
            await self.db.flush()

            try:
                results = await self._sync_competition(comp_id, season_id, log)
                all_logs.append({
                    "log_id": log.id,
                    "competition": comp_name,
                    "status": log.status,
                })
            except Exception as e:
                log.status = "failed"
                log.error_message = str(e)
                log.completed_at = datetime.now(timezone.utc)
                await self.db.commit()
                logger.exception("Failed to sync competition %s", comp_name)
                all_logs.append({
                    "log_id": log.id,
                    "competition": comp_name,
                    "status": "failed",
                    "error": str(e),
                })

        return all_logs

    async def _sync_competition(self, comp_id: int, season_id: int, log: FcmsRosterSyncLog) -> list[dict]:
        """Sync all teams for one competition."""
        competitors = await self.client.get_competition_competitors(comp_id)
        fcms_team_ids = [c["teamId"] for c in competitors]

        result = await self.db.execute(
            select(Team).where(Team.fcms_team_id.in_(fcms_team_ids)).order_by(Team.name)
        )
        teams = result.scalars().all()

        if not teams:
            log.status = "completed"
            log.completed_at = datetime.now(timezone.utc)
            log.results = []
            await self.db.commit()
            return []

        # Build fcms_team_id -> competitor mapping for fetching players
        team_fcms_map = {t.fcms_team_id: t for t in teams}

        all_changes = []
        teams_synced = 0
        has_errors = False

        for team in teams:
            try:
                async with self.db.begin_nested():
                    fcms_players = await self.client.get_competitor_players(comp_id, team.fcms_team_id)
                    changes = await self.sync_team_roster(team, fcms_players, season_id)

                    # Coach sync
                    try:
                        fcms_officials = await self.client.get_competitor_officials(comp_id, team.fcms_team_id)
                        coach_changes = await self.sync_team_coaches(team, fcms_officials, season_id)
                        changes["coach_updates"] = coach_changes
                    except Exception:
                        logger.exception("Failed to sync coaches for %s", team.name)

                    all_changes.append(changes)
                    teams_synced += 1
            except Exception as e:
                has_errors = True
                all_changes.append({
                    "team_name": team.name,
                    "team_id": team.id,
                    "fcms_team_id": team.fcms_team_id,
                    "error": str(e),
                })
                logger.exception("Failed to sync team %s", team.name)

        await self.db.commit()

        # Update log
        log.results = all_changes
        log.teams_synced = teams_synced
        log.total_auto_updates = sum(len(c.get("auto_updates", [])) for c in all_changes if "error" not in c)
        log.total_new_players = sum(len(c.get("new_players", [])) for c in all_changes if "error" not in c)
        log.total_auto_deactivated = sum(len(c.get("auto_deactivated", [])) for c in all_changes if "error" not in c)
        log.total_deregistered = sum(len(c.get("deregistered", [])) for c in all_changes if "error" not in c)
        log.status = "completed_with_errors" if has_errors else "completed"
        log.completed_at = datetime.now(timezone.utc)
        await self.db.commit()

        # Telegram report (split into chunks to avoid 4096 char limit)
        try:
            chunks = self._format_telegram_report_chunks(all_changes, log.competition_name)
            for chunk in chunks:
                await send_telegram_message(chunk)
        except Exception:
            logger.exception("Failed to send Telegram report")

        return all_changes

    async def sync_team_roster(self, team: Team, fcms_players: list[dict], season_id: int) -> dict:
        """Sync one team's roster. Returns changes dict with stable keys."""
        # Load local roster (only active players, role=1, not hidden)
        local_result = await self.db.execute(
            select(PlayerTeam, Player)
            .join(Player, PlayerTeam.player_id == Player.id)
            .where(
                PlayerTeam.team_id == team.id,
                PlayerTeam.season_id == season_id,
                PlayerTeam.is_active == True,
                PlayerTeam.is_hidden == False,
                PlayerTeam.role == 1,
            )
        )
        local_roster = local_result.all()

        # Build indexes
        local_by_name: dict[tuple, tuple[PlayerTeam, Player]] = {}
        local_by_fcms: dict[int, tuple[PlayerTeam, Player]] = {}
        local_by_num: dict[int, tuple[PlayerTeam, Player]] = {}
        for pt, p in local_roster:
            for ln, fn in [
                (p.last_name, p.first_name),
                (p.last_name_en, p.first_name_en),
                (p.last_name_kz, p.first_name_kz),
            ]:
                key = _name_key(ln, fn)
                if key:
                    local_by_name[key] = (pt, p)
            if p.fcms_person_id:
                local_by_fcms[p.fcms_person_id] = (pt, p)
            if pt.number:
                local_by_num[pt.number] = (pt, p)

        active_fcms = [fp for fp in fcms_players if fp.get("jerseyNumber")]
        changes = {
            "team_name": team.name,
            "team_id": team.id,
            "fcms_team_id": team.fcms_team_id,
            "fcms_total": len(fcms_players),
            "fcms_active": len(active_fcms),
            "local_count": len(local_roster),
            "auto_updates": [],
            "new_players": [],
            "auto_deactivated": [],
            "deregistered": [],
            "matched": 0,
        }

        matched_player_ids: set[int] = set()

        for fp in fcms_players:
            p_data = fp.get("player", {})
            fn_ru = p_data.get("localFirstName") or ""
            ln_ru = p_data.get("localFamilyName") or ""
            fn_en = p_data.get("firstName") or ""
            ln_en = p_data.get("familyName") or ""
            num_str = fp.get("jerseyNumber", "")
            num = int(num_str) if num_str else None
            dob = p_data.get("dateOfBirth", "")
            person_id = p_data.get("personId")
            fcms_club_id = p_data.get("clubId")
            club_info = p_data.get("club", {})
            club_name = club_info.get("title") or club_info.get("internationalTitle") or ""
            fcms_name = f"{fn_ru} {ln_ru}".strip() or f"{fn_en} {ln_en}".strip()

            # Extract country from nationalCitizenships
            citizenships = p_data.get("nationalCitizenships") or []
            country_iso2 = citizenships[0].get("iso2") if citizenships else None
            country_id = await self._resolve_country(country_iso2) if country_iso2 else None

            # Step 0: no jersey number = deregistered → hide from roster
            if num is None:
                match, method = self._find_in_roster(
                    fn_ru, ln_ru, fn_en, ln_en, person_id, None,
                    local_by_fcms, local_by_name, {},
                )
                if match:
                    pt_m, lp_m = match
                    matched_player_ids.add(lp_m.id)
                    self._remove_from_indexes(
                        lp_m.id, local_by_fcms, local_by_name, local_by_num
                    )
                    if not pt_m.is_hidden:
                        pt_m.is_hidden = True
                        changes["deregistered"].append({
                            "name": fcms_name,
                            "person_id": person_id,
                        })
                continue

            # Step 1: find in current team roster
            match, method = self._find_in_roster(
                fn_ru, ln_ru, fn_en, ln_en, person_id, num,
                local_by_fcms, local_by_name, local_by_num,
            )

            pt = None
            lp = None

            if match:
                pt, lp = match
                # Remove matched player from all indexes to prevent
                # double-matching via stale name/number keys
                self._remove_from_indexes(
                    lp.id, local_by_fcms, local_by_name, local_by_num
                )
            else:
                # Step 2: global search
                lp, method = await self._find_globally(fn_ru, ln_ru, fn_en, ln_en, person_id, dob)
                if lp:
                    # Remove from local indexes — the player may already
                    # have an active PlayerTeam for this team/season, so
                    # they could still be present in local_by_* dicts.
                    self._remove_from_indexes(
                        lp.id, local_by_fcms, local_by_name, local_by_num
                    )
                    # Step 3: ensure PlayerTeam
                    pt_result = await self.db.execute(
                        select(PlayerTeam).where(
                            PlayerTeam.player_id == lp.id,
                            PlayerTeam.team_id == team.id,
                            PlayerTeam.season_id == season_id,
                        )
                    )
                    pt = pt_result.scalars().first()
                    if not pt:
                        pt = PlayerTeam(
                            player_id=lp.id,
                            team_id=team.id,
                            season_id=season_id,
                            number=num,
                            is_active=True,
                            role=1,
                        )
                        self.db.add(pt)
                        await self.db.flush()
                        changes["auto_updates"].append({
                            "name": fcms_name,
                            "num": num_str,
                            "method": method,
                            "details": [f"привязан к {team.name} (id={lp.id}, {method})"],
                        })
                    elif not pt.is_active or pt.is_hidden:
                        details = []
                        if not pt.is_active:
                            pt.is_active = True
                            pt.left_at = None
                            details.append("реактивирован")
                        if pt.is_hidden:
                            details.append("снят is_hidden")
                        pt.is_hidden = False
                        pt.number = num
                        changes["auto_updates"].append({
                            "name": fcms_name,
                            "num": num_str,
                            "method": method,
                            "details": [f"{', '.join(details)} в {team.name} (id={lp.id})"],
                        })

            if lp and pt:
                changes["matched"] += 1
                matched_player_ids.add(lp.id)
                player_updates = []

                # Step 4: auto-update fields
                # Only set/correct fcms_person_id on strong matches.
                # Weak fallbacks (number) must not rewrite the unique identity.
                _strong_methods = {"fcms_id", "name", "name_rev", "global_fcms_id", "global_name", "global_name_rev"}
                if person_id and lp.fcms_person_id != person_id and method in _strong_methods:
                    # Check uniqueness: another player may already own this person_id
                    conflict = await self._fcms_person_id_taken(person_id, lp.id)
                    if conflict:
                        logger.warning(
                            "Cannot set fcms_person_id=%s on player %s (%s %s): "
                            "already owned by player %s",
                            person_id, lp.id, lp.first_name, lp.last_name, conflict,
                        )
                    elif lp.fcms_person_id:
                        logger.warning(
                            "fcms_person_id mismatch for player %s (%s %s): %s → %s (matched by %s)",
                            lp.id, lp.first_name, lp.last_name,
                            lp.fcms_person_id, person_id, method,
                        )
                        player_updates.append(f"fcms_id: {lp.fcms_person_id} → {person_id}")
                        lp.fcms_person_id = person_id
                    else:
                        player_updates.append(f"fcms_id={person_id}")
                        lp.fcms_person_id = person_id
                elif person_id and lp.fcms_person_id != person_id:
                    logger.warning(
                        "Skipping fcms_person_id update for player %s (%s %s): "
                        "matched by weak method '%s', current=%s, fcms=%s",
                        lp.id, lp.first_name, lp.last_name,
                        method, lp.fcms_person_id, person_id,
                    )

                if fn_ru and lp.first_name != fn_ru:
                    player_updates.append(f"имя: {lp.first_name} → {fn_ru}")
                    lp.first_name = fn_ru
                if ln_ru and lp.last_name != ln_ru:
                    player_updates.append(f"фамилия: {lp.last_name} → {ln_ru}")
                    lp.last_name = ln_ru

                if fn_en and lp.first_name_en != fn_en:
                    player_updates.append(f"имя_en: {lp.first_name_en} → {fn_en}")
                    lp.first_name_en = fn_en
                if ln_en and lp.last_name_en != ln_en:
                    player_updates.append(f"фам_en: {lp.last_name_en} → {ln_en}")
                    lp.last_name_en = ln_en

                if dob:
                    try:
                        dob_parsed = date_type.fromisoformat(dob)
                        if lp.birthday != dob_parsed:
                            player_updates.append(f"дата рожд: {lp.birthday} → {dob}")
                            lp.birthday = dob_parsed
                    except ValueError:
                        pass

                if country_id and lp.country_id != country_id:
                    player_updates.append(f"страна: {lp.country_id} → {country_id} ({country_iso2})")
                    lp.country_id = country_id

                if num is not None and pt.number != num:
                    player_updates.append(f"номер: {pt.number} → {num}")
                    pt.number = num

                if player_updates:
                    existing = [u for u in changes["auto_updates"] if u.get("name") == fcms_name]
                    if existing:
                        existing[0]["details"].extend(player_updates)
                    else:
                        changes["auto_updates"].append({
                            "name": fcms_name,
                            "num": num_str,
                            "method": method,
                            "details": player_updates,
                        })
            elif not lp:
                # Step 5: not found anywhere — auto-create
                safe_pid = person_id
                if person_id:
                    existing = await self.db.execute(
                        select(Player.id).where(Player.fcms_person_id == person_id)
                    )
                    if existing.scalar():
                        logger.warning("Player fcms_person_id=%s already taken, creating without it", person_id)
                        safe_pid = None

                dob_parsed = None
                if dob:
                    try:
                        dob_parsed = date_type.fromisoformat(dob)
                    except ValueError:
                        pass

                lp = Player(
                    first_name=fn_ru or fn_en,
                    last_name=ln_ru or ln_en,
                    first_name_en=fn_en or None,
                    last_name_en=ln_en or None,
                    fcms_person_id=safe_pid,
                    country_id=country_id,
                    birthday=dob_parsed,
                )
                self.db.add(lp)
                await self.db.flush()

                pt = PlayerTeam(
                    player_id=lp.id,
                    team_id=team.id,
                    season_id=season_id,
                    number=num,
                    role=1,
                    is_active=True,
                    is_hidden=False,
                )
                self.db.add(pt)
                await self.db.flush()

                matched_player_ids.add(lp.id)
                changes["new_players"].append({
                    "key": _make_item_key(person_id, fn_ru, ln_ru, dob),
                    "name": fcms_name,
                    "num": num_str,
                    "method": "auto_created",
                    "player_id": lp.id,
                    "details": [f"авто-создан (fcms_pid={safe_pid}, страна={country_iso2})"],
                })

        # BL3: auto-deactivate missing players
        today = date_type.today()
        for pt, p in local_roster:
            if p.id not in matched_player_ids:
                pt.is_active = False
                pt.left_at = today
                changes["auto_deactivated"].append({
                    "key": f"pt_{pt.id}",
                    "name": f"{p.first_name} {p.last_name}",
                    "num": pt.number,
                    "id": p.id,
                    "player_team_id": pt.id,
                })

        return changes

    # ── Coach sync ──────────────────────────────────────────────────

    _FCMS_ROLE_MAP: dict[str, tuple[str, str]] = {
        "HDCH": ("Главный тренер", "Бас бапкер"),
        "ASCH": ("Помощник тренера", "Бапкердің көмекшісі"),
    }

    async def sync_team_coaches(
        self, team: Team, fcms_officials: list[dict], season_id: int,
    ) -> list[dict]:
        """Sync head coach and assistants from FCMS. Returns list of change dicts."""
        changes: list[dict] = []

        # Filter: only HDCH and ASCH
        relevant = []
        for o in fcms_officials:
            role_obj = o.get("teamOfficialRole") or {}
            short = role_obj.get("shortName", "")
            if short in self._FCMS_ROLE_MAP:
                relevant.append(o)

        # Load current coaches (role=2) for this team/season
        result = await self.db.execute(
            select(PlayerTeam, Player)
            .join(Player, PlayerTeam.player_id == Player.id)
            .where(
                PlayerTeam.team_id == team.id,
                PlayerTeam.season_id == season_id,
                PlayerTeam.role == 2,
                PlayerTeam.is_active == True,
            )
        )
        local_coaches = result.all()
        local_by_fcms: dict[int, tuple[PlayerTeam, Player]] = {}
        local_by_name: dict[tuple, tuple[PlayerTeam, Player]] = {}
        for pt, p in local_coaches:
            if p.fcms_person_id:
                local_by_fcms[p.fcms_person_id] = (pt, p)
            key = _name_key(p.last_name, p.first_name)
            if key:
                local_by_name[key] = (pt, p)

        matched_ids: set[int] = set()

        for o in relevant:
            to = o.get("teamOfficial", {})
            role_obj = o.get("teamOfficialRole", {})
            short = role_obj.get("shortName", "")
            person_id = to.get("personId")
            fn_ru = to.get("localFirstName") or ""
            ln_ru = to.get("localFamilyName") or ""
            fn_en = to.get("firstName") or ""
            ln_en = to.get("familyName") or ""
            position_ru, position_kz = self._FCMS_ROLE_MAP[short]

            citizenships = to.get("nationalCitizenships") or []
            country_iso2 = citizenships[0].get("iso2") if citizenships else None
            country_id = await self._resolve_country(country_iso2) if country_iso2 else None

            # Match: fcms_person_id → name → global
            pt, lp, method = None, None, None
            if person_id and person_id in local_by_fcms:
                pt, lp = local_by_fcms[person_id]
                method = "fcms_id"
            else:
                for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
                    key = _name_key(ln, fn)
                    if key and key in local_by_name:
                        pt, lp = local_by_name[key]
                        method = "name"
                        break

            if not lp:
                # Global search by fcms_person_id
                if person_id:
                    r = await self.db.execute(
                        select(Player).where(Player.fcms_person_id == person_id)
                    )
                    lp = r.scalars().first()
                    if lp:
                        method = "global_fcms_id"
                # Global search by name
                if not lp and ln_ru and fn_ru:
                    r = await self.db.execute(
                        select(Player).where(
                            Player.last_name == ln_ru,
                            Player.first_name == fn_ru,
                        )
                    )
                    lp = r.scalars().first()
                    if lp:
                        method = "global_name"

            details: list[str] = []

            if not lp:
                # Check uniqueness before creating with fcms_person_id
                safe_pid = person_id
                if person_id:
                    existing = await self.db.execute(
                        select(Player.id).where(Player.fcms_person_id == person_id)
                    )
                    if existing.scalar():
                        logger.warning("Coach fcms_person_id=%s already taken, creating without it", person_id)
                        safe_pid = None
                lp = Player(
                    first_name=fn_ru or fn_en,
                    last_name=ln_ru or ln_en,
                    first_name_en=fn_en or None,
                    last_name_en=ln_en or None,
                    fcms_person_id=safe_pid,
                    country_id=country_id,
                )
                self.db.add(lp)
                await self.db.flush()
                method = "new"
                details.append("создан")
            else:
                # Update existing
                if person_id and not lp.fcms_person_id:
                    conflict = await self._fcms_person_id_taken(person_id, lp.id)
                    if not conflict:
                        lp.fcms_person_id = person_id
                if fn_ru and lp.first_name != fn_ru:
                    details.append(f"имя: {lp.first_name} → {fn_ru}")
                    lp.first_name = fn_ru
                if ln_ru and lp.last_name != ln_ru:
                    details.append(f"фамилия: {lp.last_name} → {ln_ru}")
                    lp.last_name = ln_ru
                if fn_en and lp.first_name_en != fn_en:
                    lp.first_name_en = fn_en
                if ln_en and lp.last_name_en != ln_en:
                    lp.last_name_en = ln_en
                if country_id and lp.country_id != country_id:
                    details.append(f"страна: {lp.country_id} → {country_id}")
                    lp.country_id = country_id

            matched_ids.add(lp.id)

            # Ensure PlayerTeam(role=2)
            if not pt:
                pt_result = await self.db.execute(
                    select(PlayerTeam).where(
                        PlayerTeam.player_id == lp.id,
                        PlayerTeam.team_id == team.id,
                        PlayerTeam.season_id == season_id,
                        PlayerTeam.role == 2,
                    )
                )
                pt = pt_result.scalars().first()

            if not pt:
                pt = PlayerTeam(
                    player_id=lp.id,
                    team_id=team.id,
                    season_id=season_id,
                    role=2,
                    position_ru=position_ru,
                    position_kz=position_kz,
                    is_active=True,
                    is_hidden=False,
                )
                self.db.add(pt)
                await self.db.flush()
                details.append(f"назначен {position_ru}")
            else:
                if not pt.is_active:
                    pt.is_active = True
                    details.append("реактивирован")
                if pt.is_hidden:
                    pt.is_hidden = False
                if pt.position_ru != position_ru:
                    details.append(f"роль: {pt.position_ru} → {position_ru}")
                    pt.position_ru = position_ru
                    pt.position_kz = position_kz

            if details:
                name = f"{fn_ru} {ln_ru}".strip() or f"{fn_en} {ln_en}".strip()
                changes.append({
                    "name": name,
                    "position": position_ru,
                    "method": method,
                    "details": details,
                })

        # Deactivate coaches not in FCMS
        for pt, p in local_coaches:
            if p.id not in matched_ids:
                pt.is_active = False
                changes.append({
                    "name": f"{p.first_name} {p.last_name}",
                    "position": pt.position_ru or "тренер",
                    "method": "deactivated",
                    "details": ["деактивирован (нет в FCMS)"],
                })

        return changes

    async def _fcms_person_id_taken(self, person_id: int, exclude_player_id: int) -> int | None:
        """Check if fcms_person_id is already used by another player. Returns conflicting player_id or None."""
        r = await self.db.execute(
            select(Player.id).where(
                Player.fcms_person_id == person_id,
                Player.id != exclude_player_id,
            )
        )
        return r.scalar()

    @staticmethod
    def _remove_from_indexes(
        player_id: int,
        local_by_fcms: dict,
        local_by_name: dict,
        local_by_num: dict,
    ) -> None:
        """Remove a matched player from all lookup indexes to prevent double-matching."""
        for idx in (local_by_fcms, local_by_name, local_by_num):
            keys_to_remove = [k for k, v in idx.items() if v[1].id == player_id]
            for k in keys_to_remove:
                del idx[k]

    def _find_in_roster(self, fn_ru, ln_ru, fn_en, ln_en, person_id, num,
                        local_by_fcms, local_by_name, local_by_num):
        """Find player in current team roster."""
        if person_id and person_id in local_by_fcms:
            return local_by_fcms[person_id], "fcms_id"
        for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
            key = _name_key(ln, fn)
            if key and key in local_by_name:
                return local_by_name[key], "name"
        for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
            key = _name_key(fn, ln)
            if key and key in local_by_name:
                return local_by_name[key], "name_rev"
        if num and num in local_by_num:
            return local_by_num[num], "number"
        return None, None

    async def _find_globally(self, fn_ru, ln_ru, fn_en, ln_en, person_id, dob):
        """Find player across entire DB."""
        if person_id:
            r = await self.db.execute(select(Player).where(Player.fcms_person_id == person_id))
            p = r.scalars().first()
            if p:
                return p, "global_fcms_id"

        for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
            if ln and fn:
                r = await self.db.execute(select(Player).where(
                    or_(
                        and_(Player.last_name == ln, Player.first_name == fn),
                        and_(Player.last_name_en == ln, Player.first_name_en == fn),
                        and_(Player.last_name_kz == ln, Player.first_name_kz == fn),
                    )
                ))
                p = r.scalars().first()
                if p:
                    return p, "global_name"

        for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
            if ln and fn:
                r = await self.db.execute(select(Player).where(
                    or_(
                        and_(Player.last_name == fn, Player.first_name == ln),
                        and_(Player.last_name_en == fn, Player.first_name_en == ln),
                    )
                ))
                p = r.scalars().first()
                if p:
                    return p, "global_name_rev"

        if dob and ln_ru:
            try:
                dob_parsed = date_type.fromisoformat(dob)
                r = await self.db.execute(select(Player).where(
                    Player.birthday == dob_parsed,
                    or_(
                        Player.last_name.ilike(f"%{ln_ru}%"),
                        Player.last_name_en.ilike(f"%{ln_en}%") if ln_en else False,
                    )
                ))
                p = r.scalars().first()
                if p:
                    return p, "global_dob+name"
            except ValueError:
                pass

        return None, None

    _country_cache: dict[str, int | None] = {}

    async def _resolve_country(self, iso2: str) -> int | None:
        """Resolve ISO2 country code to country_id, with caching."""
        iso2 = iso2.upper()
        if iso2 in self._country_cache:
            return self._country_cache[iso2]
        result = await self.db.execute(
            select(Country.id).where(Country.code == iso2)
        )
        country_id = result.scalar()
        self._country_cache[iso2] = country_id
        return country_id

    def _format_telegram_report_chunks(self, all_changes: list[dict], comp_name: str) -> list[str]:
        """Format all changes into Telegram messages, split by team to stay under 4096 chars."""
        MAX_LEN = 4000
        header = f"<b>📋 FCMS Roster Sync — {comp_name}</b>\n"
        team_blocks: list[str] = []

        has_any = False
        for ch in all_changes:
            if "error" in ch:
                has_any = True
                team_blocks.append(f"<b>{ch['team_name']}</b> ⚠️ ОШИБКА: {ch['error']}\n")
                continue

            team_lines = []

            if ch.get("auto_updates"):
                has_any = True
                for p in ch["auto_updates"]:
                    details = ", ".join(p["details"])
                    team_lines.append(f"  ✏️ #{p['num']} {p['name']}: {details}")

            if ch.get("new_players"):
                has_any = True
                for p in ch["new_players"]:
                    details = ", ".join(p.get("details", []))
                    team_lines.append(f"  🆕 #{p['num']} {p['name']}: {details}")

            if ch.get("auto_deactivated"):
                has_any = True
                for p in ch["auto_deactivated"]:
                    team_lines.append(f"  🚫 #{p['num']} {p['name']} (id={p['id']}) — ДЕАКТИВИРОВАН (нет в FCMS)")

            if ch.get("deregistered"):
                for p in ch["deregistered"]:
                    team_lines.append(f"  ⏸ {p['name']} — отзаявлен")

            if ch.get("coach_updates"):
                has_any = True
                for c in ch["coach_updates"]:
                    details = ", ".join(c["details"])
                    team_lines.append(f"  👔 {c['name']} — {c['position']}: {details}")

            if team_lines:
                block = f"<b>{ch['team_name']}</b> ({ch['matched']}/{ch['fcms_active']} заявленных)\n"
                block += "\n".join(team_lines) + "\n"
                team_blocks.append(block)

        if not has_any:
            return []

        # Split into chunks
        chunks: list[str] = []
        current = header
        for block in team_blocks:
            if len(current) + len(block) > MAX_LEN:
                chunks.append(current.rstrip())
                current = f"<b>📋 {comp_name} (продолжение)</b>\n\n"
            current += "\n" + block
        if current.strip():
            chunks.append(current.rstrip())

        return chunks

    @staticmethod
    def _parse_competition_map(raw: str) -> list[tuple[int, int]]:
        """Parse 'comp_id:season_id,...' string into list of tuples."""
        result = []
        for pair in raw.split(","):
            pair = pair.strip()
            if not pair:
                continue
            comp_id_str, season_id_str = pair.split(":")
            result.append((int(comp_id_str), int(season_id_str)))
        return result
