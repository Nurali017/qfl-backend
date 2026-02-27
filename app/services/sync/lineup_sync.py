"""
Pre-game lineup sync service.

Syncs from SOTA /public/v1/games/{game_id}/pre_game_lineup/ endpoint:
- Referees + their roles
- Coaches + team assignments for the season
- Player lineups (starters/substitutes)
- Live positions from /em/{game_id}-team-{home,away}.json (amplua/position)
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.dialects.postgresql import insert

from app.models import (
    Game,
    GameLineup,
    GameStatus,
    LineupType,
    Player,
)
from app.services.sync.base import BaseSyncService
from app.utils.lineup_feed_parser import (
    FORMATION_MARKER,
    STARTING_MARKERS,
    SUBS_MARKERS,
    normalize_lineup_entry,
)
from app.utils.lineup_positions import derive_field_positions, infer_formation

logger = logging.getLogger(__name__)

LiveSyncMode = Literal["live_read", "finished_repair"]

VALID_AMPLUA = {
    "GK": "Gk",
    "D": "D",
    "DM": "DM",
    "M": "M",
    "AM": "AM",
    "F": "F",
}
VALID_FIELD_POSITIONS = {"L", "LC", "C", "RC", "R"}
HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


class LineupSyncService(BaseSyncService):
    async def _ensure_player_exists(self, player_data: dict) -> int | None:
        pid_str = player_data.get("id")
        if not pid_str:
            return None
        try:
            sota_id = UUID(pid_str)
        except (ValueError, TypeError):
            return None

        exists = await self.db.execute(select(Player).where(Player.sota_id == sota_id))
        existing_player = exists.scalar_one_or_none()
        if existing_player is not None:
            return existing_player.id

        first_name = player_data.get("first_name", "") or ""
        last_name_raw = player_data.get("last_name", [])
        if isinstance(last_name_raw, list):
            last_name = last_name_raw[0] if last_name_raw else ""
        else:
            last_name = str(last_name_raw) if last_name_raw else ""

        new_player = Player(
            sota_id=sota_id,
            first_name=first_name,
            last_name=last_name,
            updated_at=datetime.utcnow(),
        )
        self.db.add(new_player)
        await self.db.flush()
        return new_player.id

    @staticmethod
    def _normalize_shirt_number(number: object) -> int | None:
        if isinstance(number, int):
            return number
        if isinstance(number, str):
            if number in {
                "TEAM",
                "FORMATION",
                "COACH",
                "MAIN",
                "ОСНОВНЫЕ",
                "ЗАПАСНЫЕ",
            }:
                return None
            try:
                return int(number)
            except ValueError:
                return None
        return None

    @staticmethod
    def _normalize_amplua(amplua: object, *, gk: bool = False) -> str | None:
        if gk:
            return "Gk"
        if not isinstance(amplua, str):
            return None
        value = amplua.strip().upper()
        return VALID_AMPLUA.get(value)

    @staticmethod
    def _normalize_field_position(position: object) -> str | None:
        if not isinstance(position, str):
            return None
        value = position.strip().upper()
        return value if value in VALID_FIELD_POSITIONS else None

    @staticmethod
    def _normalize_kit_color(color: object) -> str | None:
        if not isinstance(color, str):
            return None
        value = color.strip()
        if HEX_COLOR_RE.match(value):
            return value.upper()
        return None

    @staticmethod
    def _extract_formation_and_kit(live_data: list[dict]) -> tuple[str | None, str | None]:
        for item in live_data:
            entry = normalize_lineup_entry(item)
            if not entry or entry["number_upper"] != FORMATION_MARKER:
                continue

            formation_raw = entry["first_name"]
            formation = formation_raw.strip() if isinstance(formation_raw, str) and formation_raw.strip() else None

            # SOTA contract: FORMATION.full_name contains HEX color.
            kit_color = LineupSyncService._normalize_kit_color(entry["full_name"])
            return formation, kit_color
        return None, None

    @staticmethod
    def _resolve_lineup_type_hint(
        current_section: LineupType | None,
        amplua: str | None,
    ) -> LineupType | None:
        if current_section == LineupType.starter:
            return LineupType.starter
        if current_section == LineupType.substitute:
            # Do not downgrade players to substitute when live feed has no amplua.
            return LineupType.substitute if amplua else None
        if amplua:
            return LineupType.starter
        return None

    async def _resolve_player_id_by_sota(
        self,
        player_data: dict,
        *,
        create_if_missing: bool,
    ) -> int | None:
        pid_str = player_data.get("id")
        if not pid_str:
            return None
        try:
            sota_id = UUID(pid_str)
        except (ValueError, TypeError):
            return None

        existing = await self.db.execute(select(Player.id).where(Player.sota_id == sota_id))
        player_id = existing.scalar_one_or_none()
        if player_id is not None:
            return player_id

        if create_if_missing:
            return await self._ensure_player_exists(player_data)
        return None

    async def _update_lineup_by_shirt_number(
        self,
        *,
        game_uuid: UUID,
        team_id: int,
        shirt_number: int,
        values: dict,
    ) -> int:
        result = await self.db.execute(
            GameLineup.__table__.update()
            .where(
                GameLineup.game_id == game_uuid,
                GameLineup.team_id == team_id,
                GameLineup.shirt_number == shirt_number,
            )
            .values(**values)
        )
        return int(result.rowcount or 0)

    async def _update_lineup_by_player_id(
        self,
        *,
        game_uuid: UUID,
        team_id: int,
        player_id: int,
        values: dict,
    ) -> int:
        result = await self.db.execute(
            GameLineup.__table__.update()
            .where(
                GameLineup.game_id == game_uuid,
                GameLineup.team_id == team_id,
                GameLineup.player_id == player_id,
            )
            .values(**values)
        )
        return int(result.rowcount or 0)

    async def _demote_stale_starters(
        self,
        *,
        game_uuid: UUID,
        team_id: int,
        keep_starter_numbers: set[int],
    ) -> int:
        """
        In strict repair mode, keep only SOTA "ОСНОВНЫЕ" shirt numbers as starters.
        """
        if not keep_starter_numbers:
            return 0

        result = await self.db.execute(
            GameLineup.__table__.update()
            .where(
                GameLineup.game_id == game_uuid,
                GameLineup.team_id == team_id,
                GameLineup.lineup_type == LineupType.starter,
                or_(
                    GameLineup.shirt_number.is_(None),
                    ~GameLineup.shirt_number.in_(sorted(keep_starter_numbers)),
                ),
            )
            .values(lineup_type=LineupType.substitute)
        )
        return int(result.rowcount or 0)

    # VSporte uses "host"/"guest" instead of SOTA's "home"/"away"
    SOTA_SIDE_TO_VSPORTE = {"home": "host", "away": "guest"}

    async def _fetch_live_team_data(
        self,
        *,
        game_id: str,
        side: str,
        timeout_seconds: float | None,
        vsporte_id: str | None = None,
    ) -> list[dict] | None:
        # Try SOTA first (if game_id is available)
        if game_id:
            live_data = await self._fetch_from_sota(
                game_id=game_id, side=side, timeout_seconds=timeout_seconds,
            )
            if live_data is not None:
                return live_data

        # Fallback to VSporte if available
        if vsporte_id:
            live_data = await self._fetch_from_vsporte(
                vsporte_id=vsporte_id, side=side, timeout_seconds=timeout_seconds,
            )
            if live_data is not None:
                return live_data

        return None

    async def _fetch_from_sota(
        self,
        *,
        game_id: str,
        side: str,
        timeout_seconds: float | None,
    ) -> list[dict] | None:
        try:
            if timeout_seconds and timeout_seconds > 0:
                live_data = await asyncio.wait_for(
                    self.client.get_live_team_lineup(game_id, side),
                    timeout=timeout_seconds,
                )
            else:
                live_data = await self.client.get_live_team_lineup(game_id, side)
        except asyncio.TimeoutError:
            logger.warning("Timed out fetching SOTA lineup for game %s (%s)", game_id, side)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch SOTA lineup for game %s (%s): %s", game_id, side, exc)
            return None

        if isinstance(live_data, list):
            return live_data
        logger.warning("Unexpected SOTA lineup payload type for game %s (%s)", game_id, side)
        return None

    async def _fetch_from_vsporte(
        self,
        *,
        vsporte_id: str,
        side: str,
        timeout_seconds: float | None,
    ) -> list[dict] | None:
        vsporte_side = self.SOTA_SIDE_TO_VSPORTE.get(side, side)
        try:
            if timeout_seconds and timeout_seconds > 0:
                live_data = await asyncio.wait_for(
                    self.client.get_vsporte_team_lineup(vsporte_id, vsporte_side),
                    timeout=timeout_seconds,
                )
            else:
                live_data = await self.client.get_vsporte_team_lineup(vsporte_id, vsporte_side)
        except asyncio.TimeoutError:
            logger.warning("Timed out fetching VSporte lineup for %s (%s)", vsporte_id, vsporte_side)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch VSporte lineup for %s (%s): %s", vsporte_id, vsporte_side, exc)
            return None

        if isinstance(live_data, list):
            return live_data
        logger.warning("Unexpected VSporte lineup payload type for %s (%s)", vsporte_id, vsporte_side)
        return None

    async def _apply_live_team_updates(
        self,
        *,
        game_uuid: UUID,
        team_id: int,
        live_data: list[dict],
        allow_insert: bool,
        update_lineup_type: bool,
        update_captain: bool,
        create_missing_players: bool,
        strict_lineup_sections: bool,
    ) -> dict[str, int]:
        result = {"positions_updated": 0, "players_added": 0, "lineup_types_updated": 0}
        current_section: LineupType | None = None
        strict_starter_numbers: set[int] = set()
        saw_starter_section = False

        for item in live_data:
            entry = normalize_lineup_entry(item)
            if not entry:
                continue

            number_upper = entry["number_upper"]
            if number_upper in STARTING_MARKERS:
                current_section = LineupType.starter
                saw_starter_section = True
                continue
            if number_upper in SUBS_MARKERS:
                current_section = LineupType.substitute
                continue

            shirt_number = entry["number_int"]
            if shirt_number is None:
                continue
            if strict_lineup_sections and current_section == LineupType.starter:
                strict_starter_numbers.add(shirt_number)

            gk_flag = bool(entry["gk"])
            amplua = self._normalize_amplua(entry.get("amplua"), gk=gk_flag)
            field_position = self._normalize_field_position(entry.get("position"))
            if update_lineup_type:
                if strict_lineup_sections and current_section in {LineupType.starter, LineupType.substitute}:
                    lineup_type_hint = current_section
                else:
                    lineup_type_hint = self._resolve_lineup_type_hint(current_section, amplua)
            else:
                lineup_type_hint = None

            values: dict = {}
            if update_captain:
                is_captain = bool(entry["capitan"])
                values["is_captain"] = is_captain
            if amplua:
                values["amplua"] = amplua
            if field_position:
                values["field_position"] = field_position
            if lineup_type_hint is not None:
                values["lineup_type"] = lineup_type_hint

            if not values:
                continue

            updated = await self._update_lineup_by_shirt_number(
                game_uuid=game_uuid,
                team_id=team_id,
                shirt_number=shirt_number,
                values=values,
            )

            player_internal_id: int | None = None
            if updated == 0:
                player_internal_id = await self._resolve_player_id_by_sota(
                    entry,
                    create_if_missing=create_missing_players,
                )
                if player_internal_id is not None:
                    updated = await self._update_lineup_by_player_id(
                        game_uuid=game_uuid,
                        team_id=team_id,
                        player_id=player_internal_id,
                        values=values,
                    )

            if updated > 0:
                if amplua or field_position:
                    result["positions_updated"] += updated
                continue

            if not allow_insert or lineup_type_hint is None:
                continue
            if player_internal_id is None:
                player_internal_id = await self._resolve_player_id_by_sota(
                    entry,
                    create_if_missing=create_missing_players,
                )
                if player_internal_id is None:
                    continue

            is_captain = bool(entry["capitan"])

            insert_result = await self.db.execute(
                insert(GameLineup)
                .values(
                    game_id=game_uuid,
                    team_id=team_id,
                    player_id=player_internal_id,
                    lineup_type=lineup_type_hint,
                    shirt_number=shirt_number,
                    is_captain=is_captain,
                    amplua=amplua,
                    field_position=field_position,
                )
                .on_conflict_do_nothing(index_elements=["game_id", "player_id"])
            )
            inserted = int(insert_result.rowcount or 0)
            if inserted > 0:
                result["players_added"] += inserted
                if amplua or field_position:
                    result["positions_updated"] += inserted

        if strict_lineup_sections and update_lineup_type and saw_starter_section:
            demoted = await self._demote_stale_starters(
                game_uuid=game_uuid,
                team_id=team_id,
                keep_starter_numbers=strict_starter_numbers,
            )
            result["lineup_types_updated"] += demoted

        return result

    async def _recalculate_field_positions(
        self,
        game_id: int,
        team_id: int,
        formation: str,
    ) -> int:
        """
        Recalculate field_position for starters using formation slots + top_role hints.

        Only runs when we have a full set of starters (11 = 1 GK + 10 outfield).
        Returns count of updated rows.
        """
        rows = (
            await self.db.execute(
                select(GameLineup.player_id, GameLineup.amplua, Player.top_role)
                .join(Player, Player.id == GameLineup.player_id)
                .where(
                    GameLineup.game_id == game_id,
                    GameLineup.team_id == team_id,
                    GameLineup.lineup_type == LineupType.starter,
                    GameLineup.amplua.isnot(None),
                )
            )
        ).all()

        # Need at least 10 outfield starters for formation-based recalculation
        outfield = [r for r in rows if r[1] != "Gk"]
        if len(outfield) < 10:
            return 0

        starters = [
            {"player_id": r[0], "amplua": r[1], "top_role": r[2]}
            for r in rows
        ]

        assignments = derive_field_positions(formation, starters)
        if not assignments:
            return 0

        updated = 0
        for assignment in assignments:
            res = await self.db.execute(
                GameLineup.__table__.update()
                .where(
                    GameLineup.game_id == game_id,
                    GameLineup.team_id == team_id,
                    GameLineup.player_id == assignment["player_id"],
                )
                .values(field_position=assignment["field_position"])
            )
            updated += int(res.rowcount or 0)

        return updated

    async def _infer_formation_from_starters(
        self,
        game_id: int,
        team_id: int,
    ) -> str | None:
        """Infer formation from amplua of starters when SOTA provides no formation."""
        rows = (
            await self.db.execute(
                select(GameLineup.amplua)
                .where(
                    GameLineup.game_id == game_id,
                    GameLineup.team_id == team_id,
                    GameLineup.lineup_type == LineupType.starter,
                    GameLineup.amplua.isnot(None),
                )
            )
        ).all()

        if not rows:
            return None

        starters = [{"amplua": r[0]} for r in rows]
        return infer_formation(starters)

    async def sync_live_positions_and_kits(
        self,
        game_id: int,
        mode: LiveSyncMode = "live_read",
        *,
        timeout_seconds: float | None = None,
        auto_commit: bool = True,
        touch_live_sync_timestamp: bool = True,
    ) -> dict:
        """
        Enrich lineup records with live SOTA positioning and kit colors.

        Modes:
        - live_read: best-effort refresh for read-path (can proceed with one side missing)
        - finished_repair: strict repair for historical matches (requires both sides)
        """
        result: dict = {
            "game_id": game_id,
            "mode": mode,
            "status": "noop",
            "positions_updated": 0,
            "lineup_types_updated": 0,
            "players_added": 0,
            "formations_updated": 0,
            "kit_colors_updated": 0,
            "failed_sides": [],
        }

        game = await self.db.get(Game, game_id)
        if not game:
            result["status"] = "failed"
            result["error"] = f"Game {game_id} not found"
            return result
        if not game.sota_id and not game.vsporte_id:
            result["status"] = "failed"
            result["error"] = f"Game {game_id} has no sota_id or vsporte_id"
            return result
        sota_uuid = str(game.sota_id) if game.sota_id else ""
        vsporte_id = game.vsporte_id

        sides_payload: dict[str, list[dict]] = {}
        side_errors: list[str] = []
        for side in ("home", "away"):
            payload = await self._fetch_live_team_data(
                game_id=sota_uuid,
                side=side,
                timeout_seconds=timeout_seconds,
                vsporte_id=vsporte_id,
            ) if sota_uuid else await self._fetch_from_vsporte(
                vsporte_id=vsporte_id,
                side=side,
                timeout_seconds=timeout_seconds,
            ) if vsporte_id else None
            if payload is None:
                side_errors.append(side)
            else:
                sides_payload[side] = payload

        result["failed_sides"] = side_errors
        if mode == "finished_repair" and side_errors:
            result["status"] = "skipped_missing_side"
            if auto_commit:
                await self.db.rollback()
            return result

        for side, team_id, formation_field, kit_field in (
            ("home", game.home_team_id, "home_formation", "home_kit_color"),
            ("away", game.away_team_id, "away_formation", "away_kit_color"),
        ):
            if not team_id or side not in sides_payload:
                continue

            live_data = sides_payload[side]

            formation, kit_color = self._extract_formation_and_kit(live_data)
            if formation and getattr(game, formation_field) != formation:
                setattr(game, formation_field, formation)
                result["formations_updated"] += 1
            if kit_color and getattr(game, kit_field) != kit_color:
                setattr(game, kit_field, kit_color)
                result["kit_colors_updated"] += 1

            team_result = await self._apply_live_team_updates(
                game_uuid=game_id,
                team_id=team_id,
                live_data=live_data,
                allow_insert=(mode == "finished_repair"),
                update_lineup_type=(mode == "finished_repair"),
                update_captain=(mode == "finished_repair"),
                create_missing_players=(mode == "finished_repair"),
                strict_lineup_sections=(mode == "finished_repair"),
            )
            result["positions_updated"] += team_result["positions_updated"]
            result["players_added"] += team_result["players_added"]
            result["lineup_types_updated"] += team_result["lineup_types_updated"]

            # Recalculate field_position from formation + top_role
            effective_formation = formation or getattr(game, formation_field)

            # Fallback: infer formation from starters' amplua
            if not effective_formation:
                effective_formation = await self._infer_formation_from_starters(game_id, team_id)
                if effective_formation:
                    setattr(game, formation_field, effective_formation)
                    result["formations_updated"] += 1
                    logger.info(
                        "Inferred formation %s for game %s %s",
                        effective_formation, game_id, formation_field,
                    )

            if effective_formation:
                recalc = await self._recalculate_field_positions(
                    game_id, team_id, effective_formation,
                )
                result["positions_updated"] += recalc

        if mode == "live_read" and touch_live_sync_timestamp:
            game.lineup_live_synced_at = datetime.utcnow()

        total_updates = (
            result["positions_updated"]
            + result["lineup_types_updated"]
            + result["players_added"]
            + result["formations_updated"]
            + result["kit_colors_updated"]
        )
        if not sides_payload:
            result["status"] = "failed"
        elif total_updates > 0:
            result["status"] = "updated"
        else:
            result["status"] = "noop"

        if auto_commit:
            await self.db.commit()
        return result

    async def backfill_finished_games_positions_and_kits(
        self,
        *,
        season_id: int | None = None,
        batch_size: int = 100,
        limit: int | None = None,
        game_ids: list[str] | None = None,
        timeout_seconds: float | None = None,
    ) -> dict:
        """
        Repair positioning and kit colors for finished matches.
        """
        today = date.today()
        result: dict = {
            "processed": 0,
            "updated_games": 0,
            "positions_updated": 0,
            "lineup_types_updated": 0,
            "formations_updated": 0,
            "kit_colors_updated": 0,
            "failed_games": [],
        }

        selected_game_ids: list[int] = []
        if game_ids:
            for raw_game_id in game_ids:
                try:
                    selected_game_ids.append(int(raw_game_id))
                except (ValueError, TypeError):
                    result["failed_games"].append(
                        {"game_id": raw_game_id, "reason": "invalid_id"}
                    )
        else:
            query = (
                select(Game.id)
                .where(Game.status != GameStatus.live)
                .where(
                    or_(
                        and_(Game.home_score.isnot(None), Game.away_score.isnot(None)),
                        Game.date < today,
                    )
                )
                .order_by(Game.date.desc(), Game.id.desc())
            )
            if season_id is not None:
                query = query.where(Game.season_id == season_id)
            if limit is not None:
                query = query.limit(limit)

            games_result = await self.db.execute(query)
            selected_game_ids = list(games_result.scalars().all())

        if limit is not None:
            selected_game_ids = selected_game_ids[:limit]

        if not selected_game_ids:
            return result

        safe_batch_size = max(1, batch_size)
        for start in range(0, len(selected_game_ids), safe_batch_size):
            batch = selected_game_ids[start:start + safe_batch_size]
            for game_id in batch:
                sync_result = await self.sync_live_positions_and_kits(
                    game_id,
                    mode="finished_repair",
                    timeout_seconds=timeout_seconds,
                    auto_commit=True,
                    touch_live_sync_timestamp=False,
                )

                result["processed"] += 1
                result["positions_updated"] += int(sync_result.get("positions_updated", 0))
                result["lineup_types_updated"] += int(sync_result.get("lineup_types_updated", 0))
                result["formations_updated"] += int(sync_result.get("formations_updated", 0))
                result["kit_colors_updated"] += int(sync_result.get("kit_colors_updated", 0))

                if sync_result.get("status") == "updated":
                    result["updated_games"] += 1

                if sync_result.get("status") in {"failed", "skipped_missing_side"}:
                    result["failed_games"].append(
                        {
                            "game_id": game_id,
                            "reason": sync_result.get("status"),
                            "failed_sides": sync_result.get("failed_sides", []),
                            "error": sync_result.get("error"),
                        }
                    )

        return result

    async def sync_pre_game_lineup(self, game_id: int) -> dict[str, int]:
        """
        Sync pre-game lineup data for a specific game.

        Returns dict with counts:
        - referees
        - coaches
        - lineups
        """
        result: dict[str, int] = {
            "referees": 0,
            "coaches": 0,
            "lineups": 0,
            "positions_updated": 0,
            "players_added": 0,
            "formations_updated": 0,
            "kit_colors_updated": 0,
        }

        game = await self.db.get(Game, game_id)
        if not game or not game.sota_id:
            return result
        sota_uuid = str(game.sota_id)

        try:
            lineup_data = await self.client.get_pre_game_lineup(sota_uuid)
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to fetch pre-game lineup for game %s: %s", game_id, e)
            return result

        if not isinstance(lineup_data, dict):
            return result

        for team_key, team_id in (("home_team", game.home_team_id), ("away_team", game.away_team_id)):
            if not team_id:
                continue

            team_data = lineup_data.get(team_key, {})
            if not isinstance(team_data, dict):
                continue

            all_players = team_data.get("lineup", []) or []
            explicit_substitutes = team_data.get("substitutes", []) or []

            if explicit_substitutes:
                starters = all_players
                substitutes = explicit_substitutes
            else:
                field_players = [p for p in all_players if not p.get("is_gk")]
                goalkeepers = [p for p in all_players if p.get("is_gk")]

                starter_field = field_players[:10]
                starter_gk = goalkeepers[:1] if goalkeepers else []
                starters = starter_gk + starter_field

                substitutes = goalkeepers[1:] + field_players[10:]

            for player_data, lineup_type in (
                *((p, LineupType.starter) for p in starters),
                *((p, LineupType.substitute) for p in substitutes),
            ):
                player_internal_id = await self._ensure_player_exists(player_data)
                if player_internal_id is None:
                    continue

                gl_stmt = insert(GameLineup).values(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=player_internal_id,
                    lineup_type=lineup_type,
                    shirt_number=player_data.get("number"),
                    is_captain=player_data.get("is_captain", False),
                )
                gl_stmt = gl_stmt.on_conflict_do_update(
                    index_elements=["game_id", "player_id"],
                    set_={
                        "team_id": gl_stmt.excluded.team_id,
                        "lineup_type": gl_stmt.excluded.lineup_type,
                        "shirt_number": gl_stmt.excluded.shirt_number,
                        "is_captain": gl_stmt.excluded.is_captain,
                    },
                )
                await self.db.execute(gl_stmt)
                result["lineups"] += 1

        if result["lineups"] > 0:
            await self.db.execute(
                Game.__table__
                .update()
                .where(Game.id == game_id)
                .values(
                    has_lineup=True,
                    lineup_source="sota_api",
                    updated_at=datetime.utcnow(),
                )
            )

        live_result = await self.sync_live_positions_and_kits(
            game_id,
            mode="live_read",
            auto_commit=False,
            touch_live_sync_timestamp=False,
        )
        result["positions_updated"] += int(live_result.get("positions_updated", 0))
        result["players_added"] += int(live_result.get("players_added", 0))
        result["formations_updated"] += int(live_result.get("formations_updated", 0))
        result["kit_colors_updated"] += int(live_result.get("kit_colors_updated", 0))

        await self.db.commit()
        return result
