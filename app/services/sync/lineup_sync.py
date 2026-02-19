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
    Game,
    GameLineup,
    LineupType,
    Player,
)
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

        # Referees and coaches are managed locally — SOTA must not create or assign them

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
            # player_teams is managed locally — not created from SOTA lineup data

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

