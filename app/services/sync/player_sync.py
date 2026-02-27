"""
Player sync service.

Handles synchronization of players and player season statistics from SOTA API.
"""
import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models import Player, PlayerTeam, PlayerSeasonStats
from app.services.sync.base import BaseSyncService, parse_date, PLAYER_SEASON_STATS_FIELDS

logger = logging.getLogger(__name__)


class PlayerSyncService(BaseSyncService):
    """
    Service for syncing player data and statistics.

    Handles:
    - Player profiles (with multilingual support)
    - Player-team associations
    - Player season statistics (50+ metrics from v2 API)
    """

    async def sync_players(self, season_id: int) -> int:
        """
        Sync players for a specific season with all 3 languages.

        Args:
            season_id: Season ID to sync players for

        Returns:
            Number of players synced
        """
        # Fetch data in all 3 languages
        players_ru = await self.client.get_players(season_id, language="ru")
        players_kz = await self.client.get_players(season_id, language="kk")
        players_en = await self.client.get_players(season_id, language="en")

        # Build lookup dicts by player id
        kz_by_id = {p["id"]: p for p in players_kz}
        en_by_id = {p["id"]: p for p in players_en}

        count = 0
        for p in players_ru:
            try:
                sota_id = UUID(p["id"])
            except (ValueError, TypeError):
                logger.warning("Skipping player with invalid SOTA id: %s", p.get("id"))
                continue
            p_kz = kz_by_id.get(p["id"], {})
            p_en = en_by_id.get(p["id"], {})

            # Find country_id by country_name (try Russian first, then English)
            country_id = await self._find_country_id(p.get("country_name"))
            if not country_id:
                country_id = await self._find_country_id(p_en.get("country_name"))

            stmt = insert(Player).values(
                sota_id=sota_id,
                first_name=p.get("first_name"),  # Russian as default
                first_name_kz=p_kz.get("first_name"),
                first_name_en=p_en.get("first_name"),
                last_name=p.get("last_name"),  # Russian as default
                last_name_kz=p_kz.get("last_name"),
                last_name_en=p_en.get("last_name"),
                birthday=parse_date(p.get("birthday")),
                player_type=p.get("type"),
                country_id=country_id,
                top_role=p.get("top_role"),  # Russian as default
                # top_role_kz not synced - SOTA returns English for kk
                top_role_en=p_en.get("top_role"),
                updated_at=datetime.utcnow(),
            )

            update_dict = {
                "first_name": stmt.excluded.first_name,
                "first_name_kz": stmt.excluded.first_name_kz,
                "first_name_en": stmt.excluded.first_name_en,
                "last_name": stmt.excluded.last_name,
                "last_name_kz": stmt.excluded.last_name_kz,
                "last_name_en": stmt.excluded.last_name_en,
                "birthday": stmt.excluded.birthday,
                "player_type": stmt.excluded.player_type,
                "country_id": stmt.excluded.country_id,
                "top_role": stmt.excluded.top_role,
                "top_role_en": stmt.excluded.top_role_en,
                "updated_at": stmt.excluded.updated_at,
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=["sota_id"],
                set_=update_dict,
            )
            await self.db.execute(stmt)
            # player_teams (squad composition) is managed locally — not synced from SOTA
            count += 1

        await self.db.commit()
        logger.info(f"Synced {count} players for season {season_id}")
        return count

    async def sync_player_season_stats(self, season_id: int) -> int:
        """
        Sync season stats for ALL players in a season from SOTA API v2.

        Uses v2 endpoint which provides 50+ metrics including:
        - xG, xG per 90
        - Duels, aerial/ground duels
        - Dribbles, tackles, interceptions
        - Key passes, progressive passes

        Args:
            season_id: Season ID to sync stats for

        Returns:
            Number of player stats synced
        """
        # Get all players in this season with their team
        player_teams_result = await self.db.execute(
            select(PlayerTeam.player_id, PlayerTeam.team_id, Player.sota_id)
            .join(Player, Player.id == PlayerTeam.player_id)
            .where(
                PlayerTeam.season_id == season_id,
                Player.sota_id.is_not(None),
            )
        )
        player_teams = list(player_teams_result.fetchall())

        if not player_teams:
            return 0

        count = 0
        for player_id, team_id, sota_id in player_teams:
            try:
                # Get all metrics from SOTA v2 API
                stats = await self.client.get_player_season_stats(str(sota_id), season_id)

                # Extract extra stats (fields not in our known list)
                extra_stats = {k: v for k, v in stats.items() if k not in PLAYER_SEASON_STATS_FIELDS}

                stmt = insert(PlayerSeasonStats).values(
                    player_id=player_id,
                    season_id=season_id,
                    team_id=team_id,
                    # Basic stats
                    games_played=stats.get("games_played"),
                    games_starting=stats.get("games_starting"),
                    games_as_subst=stats.get("games_as_subst"),
                    games_be_subst=stats.get("games_be_subst"),
                    games_unused=stats.get("games_unused"),
                    minutes_played=stats.get("time_on_field_total"),
                    # Goals & Assists
                    goals=stats.get("goal"),
                    assists=stats.get("goal_pass"),
                    goal_and_assist=stats.get("goal_and_assist"),
                    goal_out_box=stats.get("goal_out_box"),
                    owngoal=stats.get("owngoal"),
                    penalty_success=stats.get("penalty_success"),
                    xg=stats.get("xg"),
                    xg_per_90=stats.get("xg_per_90"),
                    # Shots
                    shots=stats.get("shot"),
                    shots_on_goal=stats.get("shots_on_goal"),
                    shots_blocked_opponent=stats.get("shots_blocked_opponent"),
                    # Passes
                    passes=stats.get("pass"),
                    pass_accuracy=stats.get("pass_ratio"),
                    pass_acc=stats.get("pass_acc"),
                    key_passes=stats.get("key_pass"),
                    pass_forward=stats.get("pass_forward"),
                    pass_forward_ratio=stats.get("pass_forward_ratio"),
                    pass_progressive=stats.get("pass_progressive"),
                    pass_cross=stats.get("pass_cross"),
                    pass_cross_acc=stats.get("pass_cross_acc"),
                    pass_cross_ratio=stats.get("pass_cross_ratio"),
                    pass_cross_per_90=stats.get("pass_cross_per_90"),
                    pass_to_box=stats.get("pass_to_box"),
                    pass_to_box_ratio=stats.get("pass_to_box_ratio"),
                    pass_to_3rd=stats.get("pass_to_3rd"),
                    pass_to_3rd_ratio=stats.get("pass_to_3rd_ratio"),
                    # Duels
                    duels=stats.get("duel"),
                    duels_won=stats.get("duel_success"),
                    aerial_duel=stats.get("aerial_duel"),
                    aerial_duel_success=stats.get("aerial_duel_success"),
                    ground_duel=stats.get("ground_duel"),
                    ground_duel_success=stats.get("ground_duel_success"),
                    # Defense
                    tackle=stats.get("tackle"),
                    tackle_per_90=stats.get("tackle_per_90"),
                    interception=stats.get("interception"),
                    recovery=stats.get("recovery"),
                    # Dribbles
                    dribble=stats.get("dribble"),
                    dribble_success=stats.get("dribble_success"),
                    dribble_per_90=stats.get("dribble_per_90"),
                    # Other
                    corner=stats.get("corner"),
                    offside=stats.get("offside"),
                    foul=stats.get("foul"),
                    foul_taken=stats.get("foul_taken"),
                    # Discipline
                    yellow_cards=stats.get("yellow_cards"),
                    second_yellow_cards=stats.get("second_yellow_cards"),
                    red_cards=stats.get("red_cards"),
                    # Goalkeeper
                    goals_conceded=stats.get("goals_conceded"),
                    goals_conceded_penalty=stats.get("goals_conceded_penalty"),
                    goals_conceeded_per_90=stats.get("goals_conceeded_per_90"),
                    save_shot=stats.get("save_shot"),
                    save_shot_ratio=stats.get("save_shot_ratio"),
                    saved_shot_per_90=stats.get("saved_shot_per_90"),
                    save_shot_penalty=stats.get("save_shot_penalty"),
                    save_shot_penalty_success=stats.get("save_shot_penalty_success"),
                    dry_match=stats.get("dry_match"),
                    exit=stats.get("exit"),
                    exit_success=stats.get("exit_success"),
                    # Extra stats for unknown fields
                    extra_stats=extra_stats if extra_stats else None,
                    updated_at=datetime.utcnow(),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id", "season_id"],
                    set_={
                        "team_id": stmt.excluded.team_id,
                        "games_played": stmt.excluded.games_played,
                        "games_starting": stmt.excluded.games_starting,
                        "games_as_subst": stmt.excluded.games_as_subst,
                        "games_be_subst": stmt.excluded.games_be_subst,
                        "games_unused": stmt.excluded.games_unused,
                        "minutes_played": stmt.excluded.minutes_played,
                        "goals": stmt.excluded.goals,
                        "assists": stmt.excluded.assists,
                        "goal_and_assist": stmt.excluded.goal_and_assist,
                        "goal_out_box": stmt.excluded.goal_out_box,
                        "owngoal": stmt.excluded.owngoal,
                        "penalty_success": stmt.excluded.penalty_success,
                        "xg": stmt.excluded.xg,
                        "xg_per_90": stmt.excluded.xg_per_90,
                        "shots": stmt.excluded.shots,
                        "shots_on_goal": stmt.excluded.shots_on_goal,
                        "shots_blocked_opponent": stmt.excluded.shots_blocked_opponent,
                        "passes": stmt.excluded.passes,
                        "pass_accuracy": stmt.excluded.pass_accuracy,
                        "pass_acc": stmt.excluded.pass_acc,
                        "key_passes": stmt.excluded.key_passes,
                        "pass_forward": stmt.excluded.pass_forward,
                        "pass_forward_ratio": stmt.excluded.pass_forward_ratio,
                        "pass_progressive": stmt.excluded.pass_progressive,
                        "pass_cross": stmt.excluded.pass_cross,
                        "pass_cross_acc": stmt.excluded.pass_cross_acc,
                        "pass_cross_ratio": stmt.excluded.pass_cross_ratio,
                        "pass_cross_per_90": stmt.excluded.pass_cross_per_90,
                        "pass_to_box": stmt.excluded.pass_to_box,
                        "pass_to_box_ratio": stmt.excluded.pass_to_box_ratio,
                        "pass_to_3rd": stmt.excluded.pass_to_3rd,
                        "pass_to_3rd_ratio": stmt.excluded.pass_to_3rd_ratio,
                        "duels": stmt.excluded.duels,
                        "duels_won": stmt.excluded.duels_won,
                        "aerial_duel": stmt.excluded.aerial_duel,
                        "aerial_duel_success": stmt.excluded.aerial_duel_success,
                        "ground_duel": stmt.excluded.ground_duel,
                        "ground_duel_success": stmt.excluded.ground_duel_success,
                        "tackle": stmt.excluded.tackle,
                        "tackle_per_90": stmt.excluded.tackle_per_90,
                        "interception": stmt.excluded.interception,
                        "recovery": stmt.excluded.recovery,
                        "dribble": stmt.excluded.dribble,
                        "dribble_success": stmt.excluded.dribble_success,
                        "dribble_per_90": stmt.excluded.dribble_per_90,
                        "corner": stmt.excluded.corner,
                        "offside": stmt.excluded.offside,
                        "foul": stmt.excluded.foul,
                        "foul_taken": stmt.excluded.foul_taken,
                        "yellow_cards": stmt.excluded.yellow_cards,
                        "second_yellow_cards": stmt.excluded.second_yellow_cards,
                        "red_cards": stmt.excluded.red_cards,
                        "goals_conceded": stmt.excluded.goals_conceded,
                        "goals_conceded_penalty": stmt.excluded.goals_conceded_penalty,
                        "goals_conceeded_per_90": stmt.excluded.goals_conceeded_per_90,
                        "save_shot": stmt.excluded.save_shot,
                        "save_shot_ratio": stmt.excluded.save_shot_ratio,
                        "saved_shot_per_90": stmt.excluded.saved_shot_per_90,
                        "save_shot_penalty": stmt.excluded.save_shot_penalty,
                        "save_shot_penalty_success": stmt.excluded.save_shot_penalty_success,
                        "dry_match": stmt.excluded.dry_match,
                        "exit": stmt.excluded.exit,
                        "exit_success": stmt.excluded.exit_success,
                        "extra_stats": stmt.excluded.extra_stats,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                await self.db.execute(stmt)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to sync player season stats for player {player_id}: {e}")
                continue  # Skip players without v2 stats

        await self.db.commit()
        logger.info(f"Synced {count} player season stats for season {season_id}")
        return count
