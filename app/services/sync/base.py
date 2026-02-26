"""
Base class and utilities for sync services.

Contains shared logic, constants, and helper functions used across
all sync service implementations.
"""
import logging
import re
from datetime import datetime, date, time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Country, Stadium, Team
from app.services.sota_client import SotaClient, get_sota_client
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# ==================== Date/Time Parsing ====================

def parse_date(value: Any) -> date | None:
    """Parse date from string or return None."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def parse_time(value: Any) -> time | None:
    """Parse time from string or return None."""
    if value is None:
        return None
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%H:%M:%S").time()
        except ValueError:
            try:
                return datetime.strptime(value, "%H:%M").time()
            except ValueError:
                return None
    return None


# ==================== Stats Field Definitions ====================

PLAYER_SEASON_STATS_FIELDS = {
    # Basic stats
    "games_played", "games_starting", "games_as_subst", "games_be_subst", "games_unused",
    "time_on_field_total",  # maps to minutes_played
    # Goals & Assists
    "goal", "goal_pass", "goal_and_assist", "goal_out_box", "owngoal", "penalty_success",
    "xg", "xg_per_90",
    # Shots
    "shot", "shots_on_goal", "shots_blocked_opponent",
    # Passes
    "pass", "pass_ratio", "pass_acc", "key_pass",
    "pass_forward", "pass_forward_ratio", "pass_progressive",
    "pass_cross", "pass_cross_acc", "pass_cross_ratio", "pass_cross_per_90",
    "pass_to_box", "pass_to_box_ratio", "pass_to_3rd", "pass_to_3rd_ratio",
    # Duels
    "duel", "duel_success", "aerial_duel", "aerial_duel_success",
    "ground_duel", "ground_duel_success",
    # Defense
    "tackle", "tackle_per_90", "interception", "recovery",
    # Dribbles
    "dribble", "dribble_success", "dribble_per_90",
    # Other
    "corner", "offside", "foul", "foul_taken",
    # Discipline
    "yellow_cards", "second_yellow_cards", "red_cards",
    # Goalkeeper
    "goals_conceded", "goals_conceded_penalty", "goals_conceeded_per_90",
    "save_shot", "save_shot_ratio", "saved_shot_per_90",
    "save_shot_penalty", "save_shot_penalty_success", "dry_match", "exit", "exit_success",
    # Meta fields to skip
    "first_name", "last_name",
}

TEAM_SEASON_STATS_FIELDS = {
    # Basic stats
    "games_played", "games_total", "win", "draw", "match_loss",
    "goal", "goals_conceded", "goals_difference", "points",
    # xG
    "xg", "xg_per_match", "opponent_xg",
    # Shots
    "shot", "shots_on_goal", "shots_on_goal_per_match", "shots_off_goal",
    "shot_per_90", "shot_per_match", "shot_to_goal", "goal_to_shot_ratio", "shots_against_penalty",
    # Possession
    "possession_percent_average",
    # Passes
    "pass", "pass_per_match", "pass_ratio",
    "pass_forward", "pass_forward_per_match", "pass_forward_ratio",
    "pass_long", "pass_long_per_match", "pass_long_ratio",
    "pass_progressive", "pass_progressive_per_match",
    "pass_cross", "pass_cross_per_match", "pass_cross_ratio",
    "pass_to_box", "pass_to_box_per_match", "pass_to_box_ratio",
    "pass_to_3rd", "pass_to_3rd_acc", "pass_to_3rd_per_match", "pass_to_3rd_ratio",
    "key_pass", "key_pass_per_match", "key_pass_ratio", "goal_pass",
    # Free kicks
    "freekick_pass", "freekick_pass_per_match", "freekick_shot", "freekick_shot_per_match",
    # Duels
    "duel", "duel_per_match", "duel_ratio",
    "aerial_duel_offence", "aerial_duel_offence_per_match", "aerial_duel_offence_ratio",
    "aerial_duel_defence", "aerial_duel_defence_per_match", "aerial_duel_defence_ratio",
    "ground_duel_offence", "ground_duel_offence_per_match", "ground_duel_offence_ratio",
    "ground_duel_defence", "ground_duel_defence_per_match", "ground_duel_defence_ratio",
    # Defense
    "tackle", "tackle_per_match", "tackle1-1", "tackle1-1_per_match", "tackle1-1_ratio",
    "interception", "interception_per_match", "recovery", "recovery_per_match",
    # Dribbles
    "dribble", "dribble_per_match", "dribble_ratio",
    # Penalties
    "penalty", "penalty_ratio", "save_penalty_ratio",
    # Discipline
    "foul", "foul_taken", "yellow_cards", "second_yellow_cards", "red_cards",
    # Set pieces
    "corner", "corner_per_match", "offside", "out",
    # Visitors
    "visitor_total", "average_visitors",
}

GAME_PLAYER_STATS_FIELDS = {
    "goals", "assists", "shot", "shots_on_goal", "shots_off_goal",
    "pass", "pass_accuracy", "duel", "tackle", "corner", "offside", "foul",
    "yellow_cards", "red_cards",
}

GAME_TEAM_STATS_FIELDS = {
    "possession", "possession_percent", "shot", "shots_on_goal", "shots_off_goal",
    "pass", "pass_accuracy", "foul", "yellow_cards", "red_cards", "corner", "offside",
}


# ==================== Base Sync Service ====================

class BaseSyncService:
    """
    Base class for all sync services.

    Provides common functionality:
    - Database session management
    - SOTA API client access
    - Country cache for lookups
    - Stadium management
    - Team stadium cache
    """

    def __init__(self, db: AsyncSession, client: SotaClient | None = None):
        """
        Initialize the sync service.

        Args:
            db: SQLAlchemy async session
            client: Optional SOTA client (uses singleton if not provided)
        """
        self.db = db
        self.client = client or get_sota_client()
        self._country_cache: dict[str, int] | None = None
        self._team_stadium_cache: dict[int, int] | None = None

    async def _get_country_cache(self) -> dict[str, int]:
        """
        Build a cache mapping country names (lowercase) to country IDs.

        Maps all language variants (RU, KZ, EN) to the same country_id.
        """
        if self._country_cache is not None:
            return self._country_cache

        result = await self.db.execute(select(Country))
        countries = result.scalars().all()

        cache = {}
        for c in countries:
            # Map all name variants to the same country_id
            if c.name:
                cache[c.name.lower().strip()] = c.id
            if c.name_kz:
                cache[c.name_kz.lower().strip()] = c.id
            if c.name_en:
                cache[c.name_en.lower().strip()] = c.id

        self._country_cache = cache
        return cache

    async def _find_country_id(self, country_name: str | None) -> int | None:
        """Find country_id by country name (supports any language)."""
        if not country_name:
            return None

        cache = await self._get_country_cache()
        return cache.get(country_name.lower().strip())

    async def _get_or_create_stadium(self, stadium_name: str | None) -> int | None:
        """Get existing stadium by name or create new one."""
        if not stadium_name:
            return None

        # Try to find existing stadium
        result = await self.db.execute(
            select(Stadium).where(Stadium.name == stadium_name)
        )
        stadium = result.scalar_one_or_none()

        if stadium:
            return stadium.id

        # Create new stadium
        new_stadium = Stadium(name=stadium_name)
        self.db.add(new_stadium)
        await self.db.flush()
        return new_stadium.id

    async def _get_home_stadium_for_team(self, team_id: int) -> int | None:
        """Get home stadium ID for a team from the teams table."""
        if self._team_stadium_cache is None:
            result = await self.db.execute(
                select(Team.id, Team.stadium_id).where(Team.stadium_id.is_not(None))
            )
            self._team_stadium_cache = {row[0]: row[1] for row in result.all()}

        return self._team_stadium_cache.get(team_id)

    def _invalidate_caches(self) -> None:
        """Invalidate all internal caches."""
        self._country_cache = None
        self._team_stadium_cache = None
