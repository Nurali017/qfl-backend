"""
Pre-game lineup sync service.

Syncs from SOTA /public/v1/games/{game_id}/pre_game_lineup/ endpoint:
- Referees + their roles
- Coaches + team assignments for the season
- Player lineups (starters/substitutes)

This data is used by the Match Center game detail endpoint.
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import (
    Coach,
    Game,
    GameLineup,
    GameReferee,
    LineupType,
    Player,
    PlayerTeam,
    Referee,
    RefereeRole,
    TeamCoach,
)
from app.models.coach import CoachRole
from app.services.sync.base import BaseSyncService

logger = logging.getLogger(__name__)


class LineupSyncService(BaseSyncService):
    async def sync_pre_game_lineup(self, game_id: str) -> dict[str, int]:
        """
        Sync pre-game lineup data for a specific game.

        Returns dict with counts:
        - referees
        - coaches
        - lineups
        """
        result: dict[str, int] = {"referees": 0, "coaches": 0, "lineups": 0}

        try:
            lineup_data = await self.client.get_pre_game_lineup(game_id)
        except Exception as e:
            logger.warning("Failed to fetch pre-game lineup for game %s: %s", game_id, e)
            return result

        if not isinstance(lineup_data, dict):
            return result

        game_uuid = UUID(game_id)

        game_result = await self.db.execute(select(Game).where(Game.id == game_uuid))
        game = game_result.scalar_one_or_none()
        if not game:
            return result

        # -------------------- Referees --------------------
        referee_role_map: dict[str, RefereeRole] = {
            "main": RefereeRole.main,
            "1st_assistant": RefereeRole.first_assistant,
            "2nd_assistant": RefereeRole.second_assistant,
            "4th_referee": RefereeRole.fourth_referee,
            "video_assistant_main": RefereeRole.var_main,
            "video_assistant_1": RefereeRole.var_assistant,
            "match_inspector": RefereeRole.match_inspector,
        }

        referees_data = lineup_data.get("referees", {})
        if isinstance(referees_data, dict):
            existing = await self.db.execute(select(Referee))
            all_refs = list(existing.scalars().all())

            def normalize(value: str) -> str:
                return (
                    value.lower()
                    .strip()
                    .replace("ё", "е")
                    .replace("ә", "а")
                    .replace("ұ", "у")
                    .replace("і", "и")
                    .replace("ғ", "г")
                    .replace("қ", "к")
                    .replace("ң", "н")
                    .replace("ө", "о")
                    .replace("ү", "у")
                    .replace("ы", "и")
                    .replace("һ", "х")
                    .replace("й", "и")
                )

            def is_similar(a: str, b: str, max_diff: int = 2) -> bool:
                na, nb = normalize(a), normalize(b)
                if na == nb:
                    return True
                if abs(len(na) - len(nb)) > max_diff:
                    return False
                diff = sum(1 for x, y in zip(na, nb) if x != y) + abs(len(na) - len(nb))
                return diff <= max_diff

            for role_key, name in referees_data.items():
                if not name:
                    continue

                role = referee_role_map.get(role_key)
                if not role:
                    continue

                parts = str(name).split()
                first_name = parts[0] if len(parts) > 1 else ""
                last_name = parts[-1] if parts else str(name)

                existing_ref = None
                for ref in all_refs:
                    ref_fn = ref.first_name or ""
                    ref_ln = ref.last_name or ""
                    if is_similar(ref_fn, first_name) and is_similar(ref_ln, last_name):
                        existing_ref = ref
                        break
                    if is_similar(ref_fn, last_name) and is_similar(ref_ln, first_name):
                        existing_ref = ref
                        break

                if existing_ref:
                    ref_id = existing_ref.id
                else:
                    ref_stmt = insert(Referee).values(first_name=first_name, last_name=last_name)
                    ref_result = await self.db.execute(ref_stmt)
                    ref_id = ref_result.inserted_primary_key[0]
                    all_refs.append(Referee(id=ref_id, first_name=first_name, last_name=last_name))

                gr_stmt = insert(GameReferee).values(game_id=game_uuid, referee_id=ref_id, role=role)
                gr_stmt = gr_stmt.on_conflict_do_nothing()
                await self.db.execute(gr_stmt)
                result["referees"] += 1

        # -------------------- Coaches --------------------
        coach_role_map: dict[str, CoachRole] = {
            "coach": CoachRole.head_coach,
            "first_assistant": CoachRole.assistant,
            "second_assistant": CoachRole.assistant,
        }

        for team_key, team_id in (("home_team", game.home_team_id), ("away_team", game.away_team_id)):
            if not team_id:
                continue

            team_data = lineup_data.get(team_key, {})
            if not isinstance(team_data, dict):
                continue

            for role_key, role_enum in coach_role_map.items():
                coach_data = team_data.get(role_key)
                if not coach_data or not isinstance(coach_data, dict):
                    continue

                first_name = coach_data.get("first_name") or ""
                last_name_raw = coach_data.get("last_name", [])
                if isinstance(last_name_raw, list):
                    last_name = last_name_raw[0] if last_name_raw else ""
                else:
                    last_name = str(last_name_raw) if last_name_raw else ""

                if not first_name or not last_name:
                    continue

                existing = await self.db.execute(
                    select(Coach).where(Coach.first_name == first_name, Coach.last_name == last_name)
                )
                coach = existing.scalar_one_or_none()

                if coach:
                    coach_id = coach.id
                else:
                    coach_stmt = insert(Coach).values(first_name=first_name, last_name=last_name)
                    coach_result = await self.db.execute(coach_stmt)
                    coach_id = coach_result.inserted_primary_key[0]

                tc_stmt = insert(TeamCoach).values(
                    team_id=team_id,
                    coach_id=coach_id,
                    season_id=game.season_id,
                    role=role_enum,
                    is_active=True,
                    start_date=game.date,
                )
                tc_stmt = tc_stmt.on_conflict_do_update(
                    index_elements=["team_id", "coach_id", "season_id", "role"],
                    set_={"start_date": tc_stmt.excluded.start_date, "is_active": True},
                )
                await self.db.execute(tc_stmt)
                result["coaches"] += 1

        # -------------------- Lineups --------------------
        async def ensure_player_exists(player_data: dict, team_id: int) -> None:
            pid_str = player_data.get("id")
            if not pid_str:
                return
            try:
                pid = UUID(pid_str)
            except (ValueError, TypeError):
                return

            exists = await self.db.execute(select(Player.id).where(Player.id == pid))
            if exists.scalar_one_or_none() is not None:
                return

            first_name = player_data.get("first_name", "") or ""
            last_name_raw = player_data.get("last_name", [])
            if isinstance(last_name_raw, list):
                last_name = last_name_raw[0] if last_name_raw else ""
            else:
                last_name = str(last_name_raw) if last_name_raw else ""

            stmt = insert(Player).values(id=pid, first_name=first_name, last_name=last_name).on_conflict_do_nothing()
            await self.db.execute(stmt)

            pt_stmt = (
                insert(PlayerTeam)
                .values(player_id=pid, team_id=team_id, season_id=game.season_id)
                .on_conflict_do_nothing()
            )
            await self.db.execute(pt_stmt)

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
                pid_str = player_data.get("id")
                if not pid_str:
                    continue
                try:
                    player_uuid = UUID(pid_str)
                except (ValueError, TypeError):
                    continue

                await ensure_player_exists(player_data, team_id)

                gl_stmt = insert(GameLineup).values(
                    game_id=game_uuid,
                    team_id=team_id,
                    player_id=player_uuid,
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
                .where(Game.id == game_uuid)
                .values(has_lineup=True, updated_at=datetime.utcnow())
            )

        await self.db.commit()
        return result

