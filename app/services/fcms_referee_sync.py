"""FCMS referee sync: pulls /v1/matches/{id}/matchOfficialAllocations
and upserts into game_referees.

Source of truth: FCMS. Referees missing from our DB are auto-created
by fcms_person_id with full ФИО from FCMS.
"""

from __future__ import annotations

import logging
from datetime import date as date_type, timedelta
from typing import Iterable

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Country,
    Game,
    GameReferee,
    GameStatus,
    Referee,
    RefereeRole,
)
from app.services.fcms_client import FcmsClient

logger = logging.getLogger(__name__)


# FCMS matchOfficialRole.roleType → internal RefereeRole.
# Discovered via /v1/matches/{id}/matchOfficialAllocations on PL-2026 matches
# (see backend/agent/FCMS_API.md §4a).
_FCMS_REF_ROLE_MAP: dict[str, RefereeRole] = {
    "REFEREE": RefereeRole.main,
    "ASSISTANT_REFEREE_1ST": RefereeRole.first_assistant,
    "ASSISTANT_REFEREE_2ND": RefereeRole.second_assistant,
    "FOURTH_OFFICIAL": RefereeRole.fourth_referee,
    "VIDEO_ASSISTANT_REFEREE": RefereeRole.var_main,
    "ASSISTANT_VIDEO_ASSISTANT_REFEREE_1ST": RefereeRole.var_assistant,
    "VAR_OPERATOR": RefereeRole.var_operator,
    "MATCH_COMMISSIONER": RefereeRole.match_commissioner,
    "MATCH_INSPECTOR": RefereeRole.match_inspector,
}


def _name_key(ln: str | None, fn: str | None) -> tuple[str, str] | None:
    if ln and fn:
        return (ln.strip().lower(), fn.strip().lower())
    return None


class FcmsRefereeSyncService:
    """Sync referees for individual games from FCMS."""

    def __init__(self, db: AsyncSession, client: FcmsClient):
        self.db = db
        self.client = client
        self._country_cache: dict[str, int | None] = {}

    # ── Query helpers ────────────────────────────────────────────────

    async def get_games_for_referee_sync(
        self, *, horizon_days: int = 7, today: date_type | None = None,
    ) -> list[Game]:
        """Upcoming games (next `horizon_days`) with fcms_match_id."""
        today = today or date_type.today()
        end = today + timedelta(days=horizon_days)
        result = await self.db.execute(
            select(Game).where(
                and_(
                    Game.date >= today,
                    Game.date <= end,
                    Game.status == GameStatus.created,
                    Game.fcms_match_id.isnot(None),
                    Game.sync_disabled == False,  # noqa: E712
                )
            ).order_by(Game.date, Game.time)
        )
        return list(result.scalars().all())

    # ── Main entry: per-match sync ───────────────────────────────────

    async def sync_match_referees(self, game_id: int) -> dict:
        """Pull officials from FCMS for one match and reconcile game_referees.

        Returns counters: added / updated / removed / created_referees / skipped.
        """
        result: dict = {
            "added": 0, "updated": 0, "removed": 0,
            "created_referees": 0, "skipped": 0,
        }
        game = await self.db.get(Game, game_id)
        if not game or not game.fcms_match_id:
            return {"error": f"Game {game_id} not found or no fcms_match_id", **result}

        try:
            allocations = await self.client.get_match_official_allocations(game.fcms_match_id)
        except Exception as e:
            logger.exception("FCMS matchOfficialAllocations failed game=%d fcms=%d", game_id, game.fcms_match_id)
            return {"error": f"FCMS request failed: {e}", **result}

        # Build target snapshot: role → (referee_id, fcms_payload) deduplicated by role.
        # FCMS may return multiple officials per role only for substitutions; we keep
        # the latest CONFIRMED one (last-wins is fine — single appointment per match).
        snapshot: dict[RefereeRole, int] = {}
        for alloc in allocations:
            role_obj = alloc.get("matchOfficialRole") or {}
            mo = alloc.get("matchOfficial") or {}
            role_type = role_obj.get("roleType")
            internal_role = _FCMS_REF_ROLE_MAP.get(role_type)
            if internal_role is None:
                logger.warning(
                    "FCMS unknown roleType=%s for match=%d (game=%d)",
                    role_type, game.fcms_match_id, game_id,
                )
                result["skipped"] += 1
                continue

            try:
                referee, was_created = await self._resolve_or_create_referee(mo)
            except Exception:
                logger.exception(
                    "Referee resolve failed for match=%d role=%s personId=%s",
                    game.fcms_match_id, role_type, mo.get("personId"),
                )
                result["skipped"] += 1
                continue

            if referee is None:
                result["skipped"] += 1
                continue
            if was_created:
                result["created_referees"] += 1
            snapshot[internal_role] = referee.id

        # Existing assignments for this game.
        existing_q = await self.db.execute(
            select(GameReferee).where(GameReferee.game_id == game_id)
        )
        existing: list[GameReferee] = list(existing_q.scalars().all())
        existing_by_role: dict[RefereeRole, GameReferee] = {gr.role: gr for gr in existing}

        # Reconcile.
        target_roles = set(snapshot.keys())
        existing_roles = set(existing_by_role.keys() & set(_FCMS_REF_ROLE_MAP.values()))

        # Remove FCMS-managed roles no longer present (do not touch roles outside FCMS scope).
        for role in existing_roles - target_roles:
            await self.db.delete(existing_by_role[role])
            result["removed"] += 1

        # Add/update.
        for role, ref_id in snapshot.items():
            current = existing_by_role.get(role)
            if current is None:
                self.db.add(GameReferee(game_id=game_id, referee_id=ref_id, role=role))
                result["added"] += 1
            elif current.referee_id != ref_id:
                current.referee_id = ref_id
                result["updated"] += 1

        await self.db.flush()
        return result

    # ── Helpers ──────────────────────────────────────────────────────

    async def _resolve_or_create_referee(self, mo: dict) -> tuple[Referee | None, bool]:
        """Find Referee by fcms_person_id → name → create. Returns (ref, was_created)."""
        person_id = mo.get("personId")
        fn_ru = (mo.get("localFirstName") or "").strip() or None
        ln_ru = (mo.get("localFamilyName") or "").strip() or None
        fn_en = (mo.get("firstName") or "").strip() or None
        ln_en = (mo.get("familyName") or "").strip() or None

        # 1. Match by fcms_person_id
        if person_id:
            r = await self.db.execute(
                select(Referee).where(Referee.fcms_person_id == person_id)
            )
            ref = r.scalars().first()
            if ref:
                self._update_referee_fields(ref, fn_ru, ln_ru, fn_en, ln_en, mo)
                return ref, False

        # 2. Match by name (RU then EN)
        for ln, fn in [(ln_ru, fn_ru), (ln_en, fn_en)]:
            key = _name_key(ln, fn)
            if not key:
                continue
            r = await self.db.execute(
                select(Referee).where(
                    Referee.last_name == ln, Referee.first_name == fn
                )
            )
            ref = r.scalars().first()
            if ref:
                if person_id and not ref.fcms_person_id:
                    if not await self._fcms_person_id_taken(person_id):
                        ref.fcms_person_id = person_id
                self._update_referee_fields(ref, fn_ru, ln_ru, fn_en, ln_en, mo)
                return ref, False

        # 3. Create
        if not (ln_ru or ln_en) or not (fn_ru or fn_en):
            logger.warning("FCMS official without usable name: personId=%s", person_id)
            return None, False

        country_id = await self._resolve_country_from_payload(mo)
        safe_pid = person_id
        if person_id and await self._fcms_person_id_taken(person_id):
            logger.warning(
                "Referee fcms_person_id=%s already taken, creating without it",
                person_id,
            )
            safe_pid = None

        ref = Referee(
            first_name=fn_ru or fn_en or "",
            last_name=ln_ru or ln_en or "",
            first_name_ru=fn_ru,
            last_name_ru=ln_ru,
            first_name_en=fn_en,
            last_name_en=ln_en,
            fcms_person_id=safe_pid,
            country_id=country_id,
        )
        self.db.add(ref)
        await self.db.flush()
        return ref, True

    def _update_referee_fields(
        self,
        ref: Referee,
        fn_ru: str | None, ln_ru: str | None,
        fn_en: str | None, ln_en: str | None,
        mo: dict,
    ) -> None:
        """Backfill missing fields on existing referee from FCMS payload."""
        if fn_ru and not ref.first_name_ru:
            ref.first_name_ru = fn_ru
        if ln_ru and not ref.last_name_ru:
            ref.last_name_ru = ln_ru
        if fn_en and not ref.first_name_en:
            ref.first_name_en = fn_en
        if ln_en and not ref.last_name_en:
            ref.last_name_en = ln_en

    async def _resolve_country_from_payload(self, mo: dict) -> int | None:
        cits = mo.get("nationalCitizenships") or []
        if not cits:
            return None
        iso2 = (cits[0].get("iso2") or "").upper()
        if not iso2:
            return None
        if iso2 in self._country_cache:
            return self._country_cache[iso2]
        r = await self.db.execute(select(Country.id).where(Country.code == iso2))
        cid = r.scalar()
        self._country_cache[iso2] = cid
        return cid

    async def _fcms_person_id_taken(self, person_id: int) -> bool:
        r = await self.db.execute(
            select(Referee.id).where(Referee.fcms_person_id == person_id)
        )
        return r.scalar() is not None

    # ── Bulk ─────────────────────────────────────────────────────────

    async def sync_many(self, game_ids: Iterable[int]) -> dict:
        """Sync a batch of games, isolating per-game errors via SAVEPOINT."""
        totals = {
            "games": 0, "added": 0, "updated": 0, "removed": 0,
            "created_referees": 0, "skipped": 0, "errors": 0,
        }
        for gid in game_ids:
            totals["games"] += 1
            try:
                async with self.db.begin_nested():
                    res = await self.sync_match_referees(gid)
                if "error" in res:
                    totals["errors"] += 1
                    continue
                for k in ("added", "updated", "removed", "created_referees", "skipped"):
                    totals[k] += res.get(k, 0)
            except Exception:
                logger.exception("Referee sync failed for game=%d", gid)
                totals["errors"] += 1
        return totals
