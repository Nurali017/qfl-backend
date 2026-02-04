import logging
from uuid import UUID
from datetime import datetime, date, time
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)
from sqlalchemy import select, func, or_
from sqlalchemy.dialects.postgresql import insert


def parse_date(value) -> date | None:
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


def parse_time(value) -> time | None:
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

import re
from app.services.sota_client import get_sota_client
from app.services.file_storage import FileStorageService
from app.models import (
    Tournament,
    Season,
    Team,
    Country,
    Player,
    PlayerTeam,
    Game,
    GameTeamStats,
    GamePlayerStats,
    GameEvent,
    GameEventType,
    ScoreTable,
    TeamSeasonStats,
    PlayerSeasonStats,
    Stadium,
    Referee,
    Coach,
    TeamCoach,
    GameReferee,
    GameLineup,
)
from app.models.coach import CoachRole
from app.models.game_referee import RefereeRole
from app.models.game_lineup import LineupType
from app.config import get_settings

settings = get_settings()


# Define all known fields for each model to separate known from unknown
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


class SyncService:
    """Service for synchronizing data from SOTA API to local database."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = get_sota_client()
        self._country_cache: dict[str, int] | None = None

    async def _get_country_cache(self) -> dict[str, int]:
        """Build a cache mapping country names (lowercase) to country IDs."""
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
        if not hasattr(self, '_team_stadium_cache'):
            result = await self.db.execute(
                select(Team.id, Team.stadium_id).where(Team.stadium_id.is_not(None))
            )
            self._team_stadium_cache = {row[0]: row[1] for row in result.all()}

        return self._team_stadium_cache.get(team_id)

    async def sync_tournaments(self) -> int:
        """Sync tournaments from SOTA API with all 3 languages."""
        # Fetch data in all 3 languages
        tournaments_ru = await self.client.get_tournaments(language="ru")
        tournaments_kz = await self.client.get_tournaments(language="kk")
        tournaments_en = await self.client.get_tournaments(language="en")

        # Build lookup dicts
        kz_by_id = {t["id"]: t for t in tournaments_kz}
        en_by_id = {t["id"]: t for t in tournaments_en}

        count = 0
        for t in tournaments_ru:
            t_id = t["id"]
            t_kz = kz_by_id.get(t_id, {})
            t_en = en_by_id.get(t_id, {})

            stmt = insert(Tournament).values(
                id=t_id,
                name=t["name"],  # Russian as default
                name_kz=t_kz.get("name"),
                name_en=t_en.get("name"),
                country_code=t.get("country_code"),
                country_name=t.get("country_name"),  # Russian as default
                country_name_kz=t_kz.get("country_name"),
                country_name_en=t_en.get("country_name"),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "name": stmt.excluded.name,
                    "name_kz": stmt.excluded.name_kz,
                    "name_en": stmt.excluded.name_en,
                    "country_code": stmt.excluded.country_code,
                    "country_name": stmt.excluded.country_name,
                    "country_name_kz": stmt.excluded.country_name_kz,
                    "country_name_en": stmt.excluded.country_name_en,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        return count

    async def sync_seasons(self) -> int:
        """Sync seasons from SOTA API with all 3 languages."""
        # Fetch data in all 3 languages
        seasons_ru = await self.client.get_seasons(language="ru")
        seasons_kz = await self.client.get_seasons(language="kk")
        seasons_en = await self.client.get_seasons(language="en")

        # Build lookup dicts
        kz_by_id = {s["id"]: s for s in seasons_kz}
        en_by_id = {s["id"]: s for s in seasons_en}

        count = 0
        for s in seasons_ru:
            s_id = s["id"]
            s_kz = kz_by_id.get(s_id, {})
            s_en = en_by_id.get(s_id, {})

            stmt = insert(Season).values(
                id=s_id,
                name=s["name"],  # Russian as default
                name_kz=s_kz.get("name"),
                name_en=s_en.get("name"),
                tournament_id=s.get("tournament_id"),
                date_start=parse_date(s.get("date_start")),
                date_end=parse_date(s.get("date_end")),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "name": stmt.excluded.name,
                    "name_kz": stmt.excluded.name_kz,
                    "name_en": stmt.excluded.name_en,
                    "tournament_id": stmt.excluded.tournament_id,
                    "date_start": stmt.excluded.date_start,
                    "date_end": stmt.excluded.date_end,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        return count

    async def sync_teams(self) -> int:
        """Sync teams from SOTA API with all 3 languages."""
        # Fetch data in all 3 languages
        teams_ru = await self.client.get_teams(language="ru")
        teams_kz = await self.client.get_teams(language="kk")
        teams_en = await self.client.get_teams(language="en")

        # Build lookup dicts by team id
        kz_by_id = {t["id"]: t for t in teams_kz}
        en_by_id = {t["id"]: t for t in teams_en}

        count = 0
        for t in teams_ru:
            team_id = t["id"]
            t_kz = kz_by_id.get(team_id, {})
            t_en = en_by_id.get(team_id, {})

            stmt = insert(Team).values(
                id=team_id,
                name=t["name"],  # Russian as default
                name_kz=t_kz.get("name"),
                name_en=t_en.get("name"),
                logo_url=t.get("logo"),
                logo_updated_at=datetime.utcnow() if t.get("logo") else None,
                city=t.get("city"),  # Russian as default
                city_kz=t_kz.get("city"),
                city_en=t_en.get("city"),
                updated_at=datetime.utcnow(),
            )
            update_dict = {
                "name": stmt.excluded.name,
                "name_kz": stmt.excluded.name_kz,
                "name_en": stmt.excluded.name_en,
                "city": stmt.excluded.city,
                "city_kz": stmt.excluded.city_kz,
                "city_en": stmt.excluded.city_en,
                "updated_at": stmt.excluded.updated_at,
            }

            if t.get("logo"):
                update_dict["logo_url"] = stmt.excluded.logo_url
                update_dict["logo_updated_at"] = stmt.excluded.logo_updated_at

            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_=update_dict,
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        return count

    async def sync_team_logos(self) -> int:
        """Sync team logos from MinIO storage to database."""
        # Mapping for team name normalization (team name -> logo name in MinIO)
        LOGO_NAME_MAP = {
            "jenis": "zhenis",
            # Map DB team names to MinIO file names
            "kairat": "kayrat",
            "ulytau": "ulytai",
            "atyrau": "atyrai",
            "elimai": "elimai",
        }

        def normalize_name(name: str) -> str:
            """Normalize team name for matching."""
            # Remove suffixes like -M, -W, M, W, Zhastar
            name = re.sub(r'\s*[-]?\s*(M|W|Zhastar)$', '', name, flags=re.IGNORECASE)
            return name.lower().strip()

        # Get all logos from MinIO
        logos = await FileStorageService.list_team_logos()
        logo_map = {logo["team_name"].lower(): logo["url"] for logo in logos}

        # Get all teams from DB
        result = await self.db.execute(select(Team))
        teams = result.scalars().all()

        count = 0
        for team in teams:
            normalized = normalize_name(team.name)
            # Check if we need to map the name
            mapped_name = LOGO_NAME_MAP.get(normalized, normalized)

            logo_url = logo_map.get(mapped_name)
            if logo_url and logo_url != team.logo_url:
                team.logo_url = logo_url
                team.logo_updated_at = datetime.utcnow()
                count += 1

        await self.db.commit()
        return count

    async def sync_players(self, season_id: int) -> int:
        """Sync players for a specific season with all 3 languages."""
        # Fetch data in all 3 languages
        players_ru = await self.client.get_players(season_id, language="ru")
        players_kz = await self.client.get_players(season_id, language="kk")
        players_en = await self.client.get_players(season_id, language="en")

        # Build lookup dicts by player id
        kz_by_id = {p["id"]: p for p in players_kz}
        en_by_id = {p["id"]: p for p in players_en}

        count = 0
        for p in players_ru:
            player_id = UUID(p["id"])
            p_kz = kz_by_id.get(p["id"], {})
            p_en = en_by_id.get(p["id"], {})

            # Find country_id by country_name (try Russian first, then English)
            country_id = await self._find_country_id(p.get("country_name"))
            if not country_id:
                country_id = await self._find_country_id(p_en.get("country_name"))

            stmt = insert(Player).values(
                id=player_id,
                first_name=p.get("first_name"),  # Russian as default
                first_name_kz=p_kz.get("first_name"),
                first_name_en=p_en.get("first_name"),
                last_name=p.get("last_name"),  # Russian as default
                last_name_kz=p_kz.get("last_name"),
                last_name_en=p_en.get("last_name"),
                birthday=parse_date(p.get("birthday")),
                player_type=p.get("type"),
                country_id=country_id,
                photo_url=p.get("photo"),
                age=p.get("age"),
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
                "age": stmt.excluded.age,
                "top_role": stmt.excluded.top_role,
                "top_role_en": stmt.excluded.top_role_en,
                "updated_at": stmt.excluded.updated_at,
            }

            if p.get("photo"):
                update_dict["photo_url"] = stmt.excluded.photo_url

            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_=update_dict,
            )
            await self.db.execute(stmt)

            # Insert player-team associations
            for team_id in p.get("teams", []):
                pt_stmt = insert(PlayerTeam).values(
                    player_id=player_id,
                    team_id=team_id,
                    season_id=season_id,
                )
                pt_stmt = pt_stmt.on_conflict_do_nothing(
                    index_elements=["player_id", "team_id", "season_id"]
                )
                await self.db.execute(pt_stmt)

            count += 1

        await self.db.commit()
        return count

    async def sync_games(self, season_id: int) -> int:
        """Sync games for a specific season."""
        games_data = await self.client.get_games(season_id)
        count = 0

        for g in games_data:
            game_id = UUID(g["id"])
            home_team = g.get("home_team", {})
            away_team = g.get("away_team", {})

            # Get or create stadium from SOTA
            stadium_name = g.get("stadium")
            stadium_id = await self._get_or_create_stadium(stadium_name)

            # Fallback: Use home team's stadium if SOTA doesn't provide one
            if not stadium_id and home_team.get("id"):
                stadium_id = await self._get_home_stadium_for_team(home_team["id"])

            stmt = insert(Game).values(
                id=game_id,
                date=parse_date(g["date"]),
                time=parse_time(g.get("time")),
                tour=g.get("tour"),
                season_id=g.get("season_id"),
                home_team_id=home_team.get("id") if home_team else None,
                away_team_id=away_team.get("id") if away_team else None,
                home_score=home_team.get("score") if home_team else None,
                away_score=away_team.get("score") if away_team else None,
                has_stats=g.get("has_stats", False),
                stadium=stadium_name,
                stadium_id=stadium_id,
                visitors=g.get("visitors"),
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "date": stmt.excluded.date,
                    "time": stmt.excluded.time,
                    "tour": stmt.excluded.tour,
                    "home_score": stmt.excluded.home_score,
                    "away_score": stmt.excluded.away_score,
                    "has_stats": stmt.excluded.has_stats,
                    "stadium": stmt.excluded.stadium,
                    "stadium_id": stmt.excluded.stadium_id,
                    "visitors": stmt.excluded.visitors,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        return count

    async def sync_game_stats(self, game_id: str) -> dict:
        """Sync statistics for a specific game."""
        stats_data = await self.client.get_game_stats(game_id)
        game_uuid = UUID(game_id)

        # Build team name to ID mapping from team stats
        team_name_to_id = {}
        for ts in stats_data.get("teams", []):
            if isinstance(ts, dict) and ts.get("id") and ts.get("name"):
                team_name_to_id[ts["name"]] = ts["id"]

        # Sync team stats
        team_count = 0
        for ts in stats_data.get("teams", []):
            stats = ts.get("stats", {})

            # Extract extra stats (fields not in our known list)
            extra_stats = {k: v for k, v in stats.items() if k not in GAME_TEAM_STATS_FIELDS}

            stmt = insert(GameTeamStats).values(
                game_id=game_uuid,
                team_id=ts["id"],
                possession=stats.get("possession"),
                possession_percent=stats.get("possession_percent"),
                shots=stats.get("shot"),
                shots_on_goal=stats.get("shots_on_goal"),
                shots_off_goal=stats.get("shots_off_goal"),
                passes=stats.get("pass"),
                pass_accuracy=stats.get("pass_accuracy"),
                fouls=stats.get("foul"),
                yellow_cards=stats.get("yellow_cards"),
                red_cards=stats.get("red_cards"),
                corners=stats.get("corner"),
                offsides=stats.get("offside"),
                extra_stats=extra_stats if extra_stats else None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["game_id", "team_id"],
                set_={
                    "possession": stmt.excluded.possession,
                    "possession_percent": stmt.excluded.possession_percent,
                    "shots": stmt.excluded.shots,
                    "shots_on_goal": stmt.excluded.shots_on_goal,
                    "shots_off_goal": stmt.excluded.shots_off_goal,
                    "passes": stmt.excluded.passes,
                    "pass_accuracy": stmt.excluded.pass_accuracy,
                    "fouls": stmt.excluded.fouls,
                    "yellow_cards": stmt.excluded.yellow_cards,
                    "red_cards": stmt.excluded.red_cards,
                    "corners": stmt.excluded.corners,
                    "offsides": stmt.excluded.offsides,
                    "extra_stats": stmt.excluded.extra_stats,
                },
            )
            await self.db.execute(stmt)
            team_count += 1

        # Sync player stats
        player_count = 0
        for ps in stats_data.get("players", []):
            player_id = UUID(ps["id"])
            stats = ps.get("stats", {})

            # Ensure player exists (don't update photo_url here - preserve scraped photos)
            # Prepare values - only include non-null names to avoid overwriting existing data
            values_dict = {
                "id": player_id,
                "updated_at": datetime.utcnow(),
            }
            if ps.get("first_name"):
                values_dict["first_name"] = ps.get("first_name")
            if ps.get("last_name"):
                values_dict["last_name"] = ps.get("last_name")

            player_stmt = insert(Player).values(**values_dict)

            # Only update fields that are non-null in the incoming data
            update_dict = {"updated_at": player_stmt.excluded.updated_at}
            if ps.get("first_name"):
                update_dict["first_name"] = player_stmt.excluded.first_name
            if ps.get("last_name"):
                update_dict["last_name"] = player_stmt.excluded.last_name
            # NOTE: We don't update photo_url here to preserve photos from kffleague.kz scraper

            player_stmt = player_stmt.on_conflict_do_update(
                index_elements=["id"],
                set_=update_dict,
            )
            await self.db.execute(player_stmt)

            # Get team_id from mapping or directly from player stats
            team_id = ps.get("team_id") or team_name_to_id.get(ps.get("team"))
            if not team_id:
                continue  # Skip if we can't determine team

            # Extract extra stats (fields not in our known list)
            extra_stats = {k: v for k, v in stats.items() if k not in GAME_PLAYER_STATS_FIELDS}

            stmt = insert(GamePlayerStats).values(
                game_id=game_uuid,
                player_id=player_id,
                team_id=team_id,
                minutes_played=ps.get("minutes_played"),
                started=ps.get("started", False),
                position=ps.get("position"),
                # NOTE: goals/assists removed - calculated from game_events
                shots=stats.get("shot", 0),
                shots_on_goal=stats.get("shots_on_goal", 0),
                shots_off_goal=stats.get("shots_off_goal", 0),
                passes=stats.get("pass", 0),
                pass_accuracy=stats.get("pass_accuracy"),
                duel=stats.get("duel", 0),
                tackle=stats.get("tackle", 0),
                corner=stats.get("corner", 0),
                offside=stats.get("offside", 0),
                foul=stats.get("foul", 0),
                yellow_cards=stats.get("yellow_cards", 0),
                red_cards=stats.get("red_cards", 0),
                extra_stats=extra_stats if extra_stats else None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["game_id", "player_id"],
                set_={
                    "team_id": stmt.excluded.team_id,
                    "minutes_played": stmt.excluded.minutes_played,
                    "started": stmt.excluded.started,
                    "position": stmt.excluded.position,
                    "shots": stmt.excluded.shots,
                    "shots_on_goal": stmt.excluded.shots_on_goal,
                    "shots_off_goal": stmt.excluded.shots_off_goal,
                    "passes": stmt.excluded.passes,
                    "pass_accuracy": stmt.excluded.pass_accuracy,
                    "duel": stmt.excluded.duel,
                    "tackle": stmt.excluded.tackle,
                    "corner": stmt.excluded.corner,
                    "offside": stmt.excluded.offside,
                    "foul": stmt.excluded.foul,
                    "yellow_cards": stmt.excluded.yellow_cards,
                    "red_cards": stmt.excluded.red_cards,
                    "extra_stats": stmt.excluded.extra_stats,
                },
            )
            await self.db.execute(stmt)
            player_count += 1

        await self.db.commit()
        return {"teams": team_count, "players": player_count}

    async def sync_game_events(self, game_id: str) -> dict:
        """
        Sync events for a specific game from SOTA /em/ endpoint.

        Works for both live and completed games.
        """
        game_uuid = UUID(game_id)

        # Get game with team info
        result = await self.db.execute(
            select(Game).where(Game.id == game_uuid)
        )
        game = result.scalar_one_or_none()
        if not game:
            return {"error": f"Game {game_id} not found", "events_added": 0}

        # Load teams
        home_team = None
        away_team = None
        if game.home_team_id:
            result = await self.db.execute(select(Team).where(Team.id == game.home_team_id))
            home_team = result.scalar_one_or_none()
        if game.away_team_id:
            result = await self.db.execute(select(Team).where(Team.id == game.away_team_id))
            away_team = result.scalar_one_or_none()

        # Get existing events
        result = await self.db.execute(
            select(GameEvent).where(GameEvent.game_id == game_uuid)
        )
        existing_events = list(result.scalars().all())

        # Create deduplication set
        existing_signatures = set()
        for e in existing_events:
            player_key = str(e.player_id) if e.player_id else e.player_name
            sig = (e.half, e.minute, e.event_type.value, player_key)
            existing_signatures.add(sig)

        # Fetch events from SOTA
        events_data = await self.client.get_live_match_events(game_id)

        # Action type mapping
        ACTION_TYPE_MAP = {
            "ГОЛ": GameEventType.goal,
            "ГОЛЕВОЙ ПАС": GameEventType.assist,
            "ЖК": GameEventType.yellow_card,
            "КК": GameEventType.red_card,
            "ЗАМЕНА": GameEventType.substitution,
        }

        events_added = 0
        for event_data in events_data:
            action = event_data.get("action", "")
            event_type = ACTION_TYPE_MAP.get(action)
            if not event_type:
                continue

            half = event_data.get("half", 1)
            minute = event_data.get("time", 0)
            first_name1 = event_data.get("first_name1", "")
            last_name1 = event_data.get("last_name1", "")
            player_name = f"{first_name1} {last_name1}".strip()

            # Determine team ID (normalized comparison)
            team_name = event_data.get("team1", "")
            team_name_normalized = team_name.strip().lower() if team_name else ""
            team_id = None
            if home_team and home_team.name and home_team.name.strip().lower() == team_name_normalized:
                team_id = game.home_team_id
            elif away_team and away_team.name and away_team.name.strip().lower() == team_name_normalized:
                team_id = game.away_team_id

            # Find player ID from lineup
            player_id = await self._find_player_id_from_lineup(
                game_uuid, first_name1, last_name1, team_id
            )

            # Check for duplicate
            player_key = str(player_id) if player_id else player_name
            sig = (half, minute, event_type.value, player_key)
            if sig in existing_signatures:
                continue

            # Find player2 ID
            first_name2 = event_data.get("first_name2", "")
            last_name2 = event_data.get("last_name2", "")
            team2_name = event_data.get("team2", "")
            team2_id = None
            if team2_name:
                team2_normalized = team2_name.strip().lower()
                if home_team and home_team.name and home_team.name.strip().lower() == team2_normalized:
                    team2_id = game.home_team_id
                elif away_team and away_team.name and away_team.name.strip().lower() == team2_normalized:
                    team2_id = game.away_team_id
            player2_id = await self._find_player_id_from_lineup(
                game_uuid, first_name2, last_name2, team2_id
            )

            # Create event
            event = GameEvent(
                game_id=game_uuid,
                half=half,
                minute=minute,
                event_type=event_type,
                team_id=team_id,
                team_name=team_name,
                player_id=player_id,
                player_number=self._parse_number(event_data.get("number1")),
                player_name=player_name,
                player2_id=player2_id,
                player2_number=self._parse_number(event_data.get("number2")),
                player2_name=f"{first_name2} {last_name2}".strip(),
                player2_team_name=team2_name,
            )

            self.db.add(event)
            existing_signatures.add(sig)
            events_added += 1

        if events_added > 0:
            await self.db.commit()
            logger.info(f"Game {game_id}: added {events_added} events")

        return {"game_id": game_id, "events_added": events_added}

    async def _find_player_id_from_lineup(
        self, game_id: UUID, first_name: str, last_name: str, team_id: int | None
    ) -> UUID | None:
        """Find player ID by name from game lineup."""
        if not first_name and not last_name:
            return None

        query = (
            select(Player.id)
            .join(GameLineup, GameLineup.player_id == Player.id)
            .where(
                GameLineup.game_id == game_id,
                Player.first_name == first_name,
                Player.last_name == last_name,
            )
        )
        if team_id:
            query = query.where(GameLineup.team_id == team_id)

        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    def _parse_number(self, value) -> int | None:
        """Parse player number from various formats."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    async def sync_all_game_events(self, season_id: int | None = None) -> dict:
        """
        Sync events for all games in a season.

        If season_id is None, uses current season from settings.
        """
        if season_id is None:
            season_id = settings.current_season_id

        # Get all games for the season
        result = await self.db.execute(
            select(Game).where(Game.season_id == season_id)
        )
        games = list(result.scalars().all())

        total_events = 0
        games_synced = 0
        errors = []

        for game in games:
            try:
                result = await self.sync_game_events(str(game.id))
                if "error" not in result:
                    total_events += result.get("events_added", 0)
                    games_synced += 1
                else:
                    errors.append({"game_id": str(game.id), "error": result["error"]})
            except Exception as e:
                logger.error(f"Failed to sync events for game {game.id}: {e}")
                errors.append({"game_id": str(game.id), "error": str(e)})

        return {
            "games_synced": games_synced,
            "total_events_added": total_events,
            "errors": errors,
        }

    async def sync_game_metadata_from_live(self, season_id: int | None = None) -> dict:
        """
        Sync stadium and time for games from live SOTA /em/ endpoints.

        Stadium and time are extracted from /em/<game_id>-team-home.json
        using special markers like STADIUM, TIME, VENUE, DATE.
        """
        if season_id is None:
            season_id = settings.current_season_id

        # Get games without stadium or time
        result = await self.db.execute(
            select(Game).where(
                Game.season_id == season_id,
                or_(Game.stadium_id.is_(None), Game.time.is_(None)),
            )
        )
        games = list(result.scalars().all())

        updated = 0
        errors = []

        for game in games:
            try:
                # Try to get live lineup data (may contain stadium and time)
                home_data = await self.client.get_live_team_lineup(str(game.id), "home")

                # Extract stadium and time from special markers
                stadium_name = None
                time_str = None

                for item in home_data:
                    number = item.get("number", "")

                    # Check for stadium markers
                    if number in ["STADIUM", "VENUE"]:
                        stadium_name = item.get("first_name") or item.get("full_name")

                    # Check for time markers
                    if number in ["TIME", "DATE"]:
                        time_str = item.get("first_name") or item.get("full_name")

                # Update game if we found new data
                game_updated = False

                if stadium_name and not game.stadium_id:
                    stadium_id = await self._get_or_create_stadium(stadium_name)
                    if stadium_id:
                        game.stadium_id = stadium_id
                        game.stadium = stadium_name
                        game_updated = True

                if time_str and not game.time:
                    parsed_time = parse_time(time_str)
                    if parsed_time:
                        game.time = parsed_time
                        game_updated = True

                if game_updated:
                    updated += 1

            except Exception as e:
                # Many games won't have live endpoints - this is expected
                if "404" not in str(e):
                    logger.warning(f"Failed to sync metadata for game {game.id}: {e}")
                errors.append({"game_id": str(game.id), "error": str(e)})

        await self.db.commit()

        return {
            "games_checked": len(games),
            "metadata_updated": updated,
            "errors_count": len(errors),
        }

    async def sync_game_formations(self, season_id: int | None = None) -> dict:
        """
        Sync formations for all games using live SOTA endpoints.

        Formations are extracted from /em/<game_id>-team-home.json and
        /em/<game_id>-team-away.json endpoints.
        """
        if season_id is None:
            season_id = settings.current_season_id

        # Get games without formations
        result = await self.db.execute(
            select(Game).where(
                Game.season_id == season_id,
                Game.home_formation.is_(None),
            )
        )
        games = list(result.scalars().all())

        updated = 0
        errors = []

        for game in games:
            try:
                # Try to get live lineup data (contains formations)
                home_data = await self.client.get_live_team_lineup(str(game.id), "home")
                away_data = await self.client.get_live_team_lineup(str(game.id), "away")

                # Extract formations
                home_formation = None
                away_formation = None

                for item in home_data:
                    if item.get("number") == "FORMATION":
                        home_formation = item.get("first_name")
                        break

                for item in away_data:
                    if item.get("number") == "FORMATION":
                        away_formation = item.get("first_name")
                        break

                if home_formation or away_formation:
                    game.home_formation = home_formation
                    game.away_formation = away_formation
                    updated += 1

            except Exception as e:
                # Many games won't have live endpoints - this is expected
                if "404" not in str(e):
                    logger.warning(f"Failed to sync formations for game {game.id}: {e}")
                errors.append({"game_id": str(game.id), "error": str(e)})

        await self.db.commit()

        return {
            "games_checked": len(games),
            "formations_updated": updated,
            "errors_count": len(errors),
        }

    async def sync_score_table(self, season_id: int) -> int:
        """Sync league table for a season."""
        table_data = await self.client.get_score_table(season_id)
        count = 0

        # Handle different response formats
        # API returns: {"result": "success", "data": {"table": [...]}}
        if isinstance(table_data, list):
            entries = table_data
        elif isinstance(table_data, dict):
            # Try nested data.table first
            data = table_data.get("data", {})
            if isinstance(data, dict):
                entries = data.get("table", [])
            else:
                entries = table_data.get("table", table_data.get("results", []))
        else:
            entries = []

        for idx, entry in enumerate(entries, start=1):
            # Skip non-dict entries
            if not isinstance(entry, dict):
                continue

            # Get team_id - API uses 'id' for team ID
            team_id = entry.get("team_id") or entry.get("id")
            if not team_id:
                continue

            # Parse goals string like "53:19" into scored and conceded
            goals_str = entry.get("goals", "0:0")
            goals_scored, goals_conceded = 0, 0
            if isinstance(goals_str, str) and ":" in goals_str:
                parts = goals_str.split(":")
                goals_scored = int(parts[0]) if parts[0].isdigit() else 0
                goals_conceded = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

            form_list = entry.get("form", [])
            form_str = "".join(form_list) if isinstance(form_list, list) else (form_list or "")

            # Position is determined by array order (API returns sorted by points)
            stmt = insert(ScoreTable).values(
                season_id=season_id,
                team_id=team_id,
                position=idx,
                games_played=entry.get("matches") or entry.get("games_played"),
                wins=entry.get("wins"),
                draws=entry.get("draws"),
                losses=entry.get("losses"),
                goals_scored=entry.get("goals_scored") or goals_scored,
                goals_conceded=entry.get("goals_conceded") or goals_conceded,
                goal_difference=(goals_scored - goals_conceded) if goals_scored or goals_conceded else entry.get("goal_difference"),
                points=entry.get("points"),
                form=form_str,
                updated_at=datetime.utcnow(),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["season_id", "team_id"],
                set_={
                    "position": stmt.excluded.position,
                    "games_played": stmt.excluded.games_played,
                    "wins": stmt.excluded.wins,
                    "draws": stmt.excluded.draws,
                    "losses": stmt.excluded.losses,
                    "goals_scored": stmt.excluded.goals_scored,
                    "goals_conceded": stmt.excluded.goals_conceded,
                    "goal_difference": stmt.excluded.goal_difference,
                    "points": stmt.excluded.points,
                    "form": stmt.excluded.form,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            await self.db.execute(stmt)
            count += 1

        await self.db.commit()
        return count

    async def sync_team_season_stats(self, season_id: int) -> int:
        """
        Sync season stats for ALL teams in a season from SOTA API v2.

        Uses v2 endpoint which provides 92 metrics including:
        - xG, xGA, xG per match
        - Possession, pass ratio, duel ratio
        - All detailed attacking/defending stats
        """
        # Get all teams from score_table for this season
        score_table_result = await self.db.execute(
            select(ScoreTable).where(ScoreTable.season_id == season_id)
        )
        score_table_entries = {st.team_id: st for st in score_table_result.scalars().all()}

        if not score_table_entries:
            return 0

        count = 0
        for team_id in score_table_entries.keys():
            try:
                # Get all metrics from SOTA v2 API
                stats = await self.client.get_team_season_stats_v2(team_id, season_id)

                # Extract extra stats (fields not in our known list)
                extra_stats = {k: v for k, v in stats.items() if k not in TEAM_SEASON_STATS_FIELDS}

                stmt = insert(TeamSeasonStats).values(
                    team_id=team_id,
                    season_id=season_id,
                    # Basic stats
                    games_played=stats.get("games_played"),
                    games_total=stats.get("games_total"),
                    wins=stats.get("win"),
                    draws=stats.get("draw"),
                    losses=stats.get("match_loss"),
                    goals_scored=stats.get("goal"),
                    goals_conceded=stats.get("goals_conceded"),
                    goals_difference=stats.get("goals_difference"),
                    points=stats.get("points"),
                    # xG
                    xg=stats.get("xg"),
                    xg_per_match=stats.get("xg_per_match"),
                    opponent_xg=stats.get("opponent_xg"),
                    # Shots
                    shots=stats.get("shot"),
                    shots_on_goal=stats.get("shots_on_goal"),
                    shots_on_goal_per_match=stats.get("shots_on_goal_per_match"),
                    shots_off_goal=stats.get("shots_off_goal"),
                    shot_per_90=stats.get("shot_per_90"),
                    shot_per_match=stats.get("shot_per_match"),
                    shot_to_goal=stats.get("shot_to_goal"),
                    goal_to_shot_ratio=stats.get("goal_to_shot_ratio"),
                    shots_against_penalty=stats.get("shots_against_penalty"),
                    # Possession
                    possession_avg=stats.get("possession_percent_average"),
                    # Passes
                    passes=stats.get("pass"),
                    pass_per_match=stats.get("pass_per_match"),
                    pass_accuracy_avg=stats.get("pass_ratio"),
                    pass_forward=stats.get("pass_forward"),
                    pass_forward_per_match=stats.get("pass_forward_per_match"),
                    pass_forward_ratio=stats.get("pass_forward_ratio"),
                    pass_long=stats.get("pass_long"),
                    pass_long_per_match=stats.get("pass_long_per_match"),
                    pass_long_ratio=stats.get("pass_long_ratio"),
                    pass_progressive=stats.get("pass_progressive"),
                    pass_progressive_per_match=stats.get("pass_progressive_per_match"),
                    pass_cross=stats.get("pass_cross"),
                    pass_cross_per_match=stats.get("pass_cross_per_match"),
                    pass_cross_ratio=stats.get("pass_cross_ratio"),
                    pass_to_box=stats.get("pass_to_box"),
                    pass_to_box_per_match=stats.get("pass_to_box_per_match"),
                    pass_to_box_ratio=stats.get("pass_to_box_ratio"),
                    pass_to_3rd=stats.get("pass_to_3rd"),
                    pass_to_3rd_acc=stats.get("pass_to_3rd_acc"),
                    pass_to_3rd_per_match=stats.get("pass_to_3rd_per_match"),
                    pass_to_3rd_ratio=stats.get("pass_to_3rd_ratio"),
                    key_pass=stats.get("key_pass"),
                    key_pass_per_match=stats.get("key_pass_per_match"),
                    key_pass_ratio=stats.get("key_pass_ratio"),
                    goal_pass=stats.get("goal_pass"),
                    # Free kicks
                    freekick_pass=stats.get("freekick_pass"),
                    freekick_pass_per_match=stats.get("freekick_pass_per_match"),
                    freekick_shot=stats.get("freekick_shot"),
                    freekick_shot_per_match=stats.get("freekick_shot_per_match"),
                    # Duels
                    duel=stats.get("duel"),
                    duel_per_match=stats.get("duel_per_match"),
                    duel_ratio=stats.get("duel_ratio"),
                    aerial_duel_offence=stats.get("aerial_duel_offence"),
                    aerial_duel_offence_per_match=stats.get("aerial_duel_offence_per_match"),
                    aerial_duel_offence_ratio=stats.get("aerial_duel_offence_ratio"),
                    aerial_duel_defence=stats.get("aerial_duel_defence"),
                    aerial_duel_defence_per_match=stats.get("aerial_duel_defence_per_match"),
                    aerial_duel_defence_ratio=stats.get("aerial_duel_defence_ratio"),
                    ground_duel_offence=stats.get("ground_duel_offence"),
                    ground_duel_offence_per_match=stats.get("ground_duel_offence_per_match"),
                    ground_duel_offence_ratio=stats.get("ground_duel_offence_ratio"),
                    ground_duel_defence=stats.get("ground_duel_defence"),
                    ground_duel_defence_per_match=stats.get("ground_duel_defence_per_match"),
                    ground_duel_defence_ratio=stats.get("ground_duel_defence_ratio"),
                    # Defense
                    tackle=stats.get("tackle"),
                    tackle_per_match=stats.get("tackle_per_match"),
                    tackle1_1=stats.get("tackle1-1"),
                    tackle1_1_per_match=stats.get("tackle1-1_per_match"),
                    tackle1_1_ratio=stats.get("tackle1-1_ratio"),
                    interception=stats.get("interception"),
                    interception_per_match=stats.get("interception_per_match"),
                    recovery=stats.get("recovery"),
                    recovery_per_match=stats.get("recovery_per_match"),
                    # Dribbles
                    dribble=stats.get("dribble"),
                    dribble_per_match=stats.get("dribble_per_match"),
                    dribble_ratio=stats.get("dribble_ratio"),
                    # Penalties
                    penalty=stats.get("penalty"),
                    penalty_ratio=stats.get("penalty_ratio"),
                    save_penalty_ratio=stats.get("save_penalty_ratio"),
                    # Discipline
                    fouls=stats.get("foul"),
                    foul_taken=stats.get("foul_taken"),
                    yellow_cards=stats.get("yellow_cards"),
                    second_yellow_cards=stats.get("second_yellow_cards"),
                    red_cards=stats.get("red_cards"),
                    # Set pieces
                    corners=stats.get("corner"),
                    corner_per_match=stats.get("corner_per_match"),
                    offsides=stats.get("offside"),
                    out=stats.get("out"),
                    # Visitors
                    visitor_total=stats.get("visitor_total"),
                    average_visitors=stats.get("average_visitors"),
                    # Extra stats for unknown fields
                    extra_stats=extra_stats if extra_stats else None,
                    updated_at=datetime.utcnow(),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["team_id", "season_id"],
                    set_={
                        "games_played": stmt.excluded.games_played,
                        "games_total": stmt.excluded.games_total,
                        "wins": stmt.excluded.wins,
                        "draws": stmt.excluded.draws,
                        "losses": stmt.excluded.losses,
                        "goals_scored": stmt.excluded.goals_scored,
                        "goals_conceded": stmt.excluded.goals_conceded,
                        "goals_difference": stmt.excluded.goals_difference,
                        "points": stmt.excluded.points,
                        "xg": stmt.excluded.xg,
                        "xg_per_match": stmt.excluded.xg_per_match,
                        "opponent_xg": stmt.excluded.opponent_xg,
                        "shots": stmt.excluded.shots,
                        "shots_on_goal": stmt.excluded.shots_on_goal,
                        "shots_on_goal_per_match": stmt.excluded.shots_on_goal_per_match,
                        "shots_off_goal": stmt.excluded.shots_off_goal,
                        "shot_per_90": stmt.excluded.shot_per_90,
                        "shot_per_match": stmt.excluded.shot_per_match,
                        "shot_to_goal": stmt.excluded.shot_to_goal,
                        "goal_to_shot_ratio": stmt.excluded.goal_to_shot_ratio,
                        "shots_against_penalty": stmt.excluded.shots_against_penalty,
                        "possession_avg": stmt.excluded.possession_avg,
                        "passes": stmt.excluded.passes,
                        "pass_per_match": stmt.excluded.pass_per_match,
                        "pass_accuracy_avg": stmt.excluded.pass_accuracy_avg,
                        "pass_forward": stmt.excluded.pass_forward,
                        "pass_forward_per_match": stmt.excluded.pass_forward_per_match,
                        "pass_forward_ratio": stmt.excluded.pass_forward_ratio,
                        "pass_long": stmt.excluded.pass_long,
                        "pass_long_per_match": stmt.excluded.pass_long_per_match,
                        "pass_long_ratio": stmt.excluded.pass_long_ratio,
                        "pass_progressive": stmt.excluded.pass_progressive,
                        "pass_progressive_per_match": stmt.excluded.pass_progressive_per_match,
                        "pass_cross": stmt.excluded.pass_cross,
                        "pass_cross_per_match": stmt.excluded.pass_cross_per_match,
                        "pass_cross_ratio": stmt.excluded.pass_cross_ratio,
                        "pass_to_box": stmt.excluded.pass_to_box,
                        "pass_to_box_per_match": stmt.excluded.pass_to_box_per_match,
                        "pass_to_box_ratio": stmt.excluded.pass_to_box_ratio,
                        "pass_to_3rd": stmt.excluded.pass_to_3rd,
                        "pass_to_3rd_acc": stmt.excluded.pass_to_3rd_acc,
                        "pass_to_3rd_per_match": stmt.excluded.pass_to_3rd_per_match,
                        "pass_to_3rd_ratio": stmt.excluded.pass_to_3rd_ratio,
                        "key_pass": stmt.excluded.key_pass,
                        "key_pass_per_match": stmt.excluded.key_pass_per_match,
                        "key_pass_ratio": stmt.excluded.key_pass_ratio,
                        "goal_pass": stmt.excluded.goal_pass,
                        "freekick_pass": stmt.excluded.freekick_pass,
                        "freekick_pass_per_match": stmt.excluded.freekick_pass_per_match,
                        "freekick_shot": stmt.excluded.freekick_shot,
                        "freekick_shot_per_match": stmt.excluded.freekick_shot_per_match,
                        "duel": stmt.excluded.duel,
                        "duel_per_match": stmt.excluded.duel_per_match,
                        "duel_ratio": stmt.excluded.duel_ratio,
                        "aerial_duel_offence": stmt.excluded.aerial_duel_offence,
                        "aerial_duel_offence_per_match": stmt.excluded.aerial_duel_offence_per_match,
                        "aerial_duel_offence_ratio": stmt.excluded.aerial_duel_offence_ratio,
                        "aerial_duel_defence": stmt.excluded.aerial_duel_defence,
                        "aerial_duel_defence_per_match": stmt.excluded.aerial_duel_defence_per_match,
                        "aerial_duel_defence_ratio": stmt.excluded.aerial_duel_defence_ratio,
                        "ground_duel_offence": stmt.excluded.ground_duel_offence,
                        "ground_duel_offence_per_match": stmt.excluded.ground_duel_offence_per_match,
                        "ground_duel_offence_ratio": stmt.excluded.ground_duel_offence_ratio,
                        "ground_duel_defence": stmt.excluded.ground_duel_defence,
                        "ground_duel_defence_per_match": stmt.excluded.ground_duel_defence_per_match,
                        "ground_duel_defence_ratio": stmt.excluded.ground_duel_defence_ratio,
                        "tackle": stmt.excluded.tackle,
                        "tackle_per_match": stmt.excluded.tackle_per_match,
                        "tackle1_1": stmt.excluded.tackle1_1,
                        "tackle1_1_per_match": stmt.excluded.tackle1_1_per_match,
                        "tackle1_1_ratio": stmt.excluded.tackle1_1_ratio,
                        "interception": stmt.excluded.interception,
                        "interception_per_match": stmt.excluded.interception_per_match,
                        "recovery": stmt.excluded.recovery,
                        "recovery_per_match": stmt.excluded.recovery_per_match,
                        "dribble": stmt.excluded.dribble,
                        "dribble_per_match": stmt.excluded.dribble_per_match,
                        "dribble_ratio": stmt.excluded.dribble_ratio,
                        "penalty": stmt.excluded.penalty,
                        "penalty_ratio": stmt.excluded.penalty_ratio,
                        "save_penalty_ratio": stmt.excluded.save_penalty_ratio,
                        "fouls": stmt.excluded.fouls,
                        "foul_taken": stmt.excluded.foul_taken,
                        "yellow_cards": stmt.excluded.yellow_cards,
                        "second_yellow_cards": stmt.excluded.second_yellow_cards,
                        "red_cards": stmt.excluded.red_cards,
                        "corners": stmt.excluded.corners,
                        "corner_per_match": stmt.excluded.corner_per_match,
                        "offsides": stmt.excluded.offsides,
                        "out": stmt.excluded.out,
                        "visitor_total": stmt.excluded.visitor_total,
                        "average_visitors": stmt.excluded.average_visitors,
                        "extra_stats": stmt.excluded.extra_stats,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                await self.db.execute(stmt)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to sync team season stats for team {team_id}: {e}")
                continue  # Skip teams without v2 stats

        await self.db.commit()
        return count

    async def sync_player_season_stats(self, season_id: int) -> int:
        """
        Sync season stats for ALL players in a season from SOTA API v2.

        Uses v2 endpoint which provides 50+ metrics including:
        - xG, xG per 90
        - Duels, aerial/ground duels
        - Dribbles, tackles, interceptions
        - Key passes, progressive passes
        """
        # Get all players in this season with their team
        player_teams_result = await self.db.execute(
            select(PlayerTeam.player_id, PlayerTeam.team_id)
            .where(PlayerTeam.season_id == season_id)
        )
        player_teams = {pt.player_id: pt.team_id for pt in player_teams_result.fetchall()}

        if not player_teams:
            return 0

        count = 0
        for player_id, team_id in player_teams.items():
            try:
                # Get all metrics from SOTA v2 API
                stats = await self.client.get_player_season_stats(str(player_id), season_id)

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
        return count

    async def sync_pre_game_lineup(self, game_id: str) -> dict:
        """
        Sync pre-game lineup data for a specific game.
        Includes referees, coaches, and player lineups.

        SOTA API response format:
        {
            "referees": {"main": "Name", "1st_assistant": "Name", ...},
            "home_team": {"id": "123", "lineup": [...], "substitutes": [...], "coach": {...}},
            "away_team": {"id": "123", "lineup": [...], "substitutes": [...], "coach": {...}}
        }
        """
        try:
            lineup_data = await self.client.get_pre_game_lineup(game_id)
        except Exception as e:
            logger.warning(f"Failed to fetch pre-game lineup for game {game_id}: {e}")
            return {"referees": 0, "coaches": 0, "lineups": 0}

        game_uuid = UUID(game_id)
        result = {"referees": 0, "coaches": 0, "lineups": 0}

        # Get game to know home/away team IDs
        game_result = await self.db.execute(
            select(Game).where(Game.id == game_uuid)
        )
        game = game_result.scalar_one_or_none()
        if not game:
            return result

        # Map SOTA referee keys to our enum values
        referee_role_map = {
            "main": RefereeRole.main,
            "1st_assistant": RefereeRole.first_assistant,
            "2nd_assistant": RefereeRole.second_assistant,
            "4th_referee": RefereeRole.fourth_referee,
            "video_assistant_main": RefereeRole.var_main,
            "video_assistant_1": RefereeRole.var_assistant,
            "match_inspector": RefereeRole.match_inspector,
        }

        # Sync referees - SOTA returns dict with role as key and name as value
        referees_data = lineup_data.get("referees", {})
        if isinstance(referees_data, dict):
            # Normalize names for comparison (handle Kazakh/Russian spelling variations)
            def normalize(s: str) -> str:
                return (s.lower().strip()
                    .replace('ё', 'е').replace('ә', 'а').replace('ұ', 'у')
                    .replace('і', 'и').replace('ғ', 'г').replace('қ', 'к')
                    .replace('ң', 'н').replace('ө', 'о').replace('ү', 'у')
                    .replace('ы', 'и').replace('һ', 'х').replace('й', 'и')
                )

            def is_similar(s1: str, s2: str, max_diff: int = 2) -> bool:
                """Check if two strings are similar (allow max_diff character differences)."""
                n1, n2 = normalize(s1), normalize(s2)
                if n1 == n2:
                    return True
                if abs(len(n1) - len(n2)) > max_diff:
                    return False
                diff = sum(1 for a, b in zip(n1, n2) if a != b) + abs(len(n1) - len(n2))
                return diff <= max_diff

            # Load all referees once before the loop
            existing = await self.db.execute(select(Referee))
            all_refs = list(existing.scalars().all())

            for role_key, name in referees_data.items():
                if not name:
                    continue

                role = referee_role_map.get(role_key)
                if not role:
                    continue

                # Parse name (format: "First Last" or just "Last")
                name_parts = name.split() if name else []
                first_name = name_parts[0] if len(name_parts) > 1 else ""
                last_name = name_parts[-1] if name_parts else name

                # Check if referee exists (try both name orders and fuzzy match)
                existing_ref = None
                for ref in all_refs:
                    ref_fn = ref.first_name or ''
                    ref_ln = ref.last_name or ''
                    if is_similar(ref_fn, first_name) and is_similar(ref_ln, last_name):
                        existing_ref = ref
                        break
                    if is_similar(ref_fn, last_name) and is_similar(ref_ln, first_name):
                        existing_ref = ref
                        break

                if existing_ref:
                    ref_id = existing_ref.id
                else:
                    # Create new referee and add to cache
                    ref_stmt = insert(Referee).values(
                        first_name=first_name,
                        last_name=last_name,
                    )
                    ref_result = await self.db.execute(ref_stmt)
                    ref_id = ref_result.inserted_primary_key[0]
                    # Add to in-memory list so subsequent iterations find it
                    new_ref = Referee(id=ref_id, first_name=first_name, last_name=last_name)
                    all_refs.append(new_ref)

                # Create game-referee association
                gr_stmt = insert(GameReferee).values(
                    game_id=game_uuid,
                    referee_id=ref_id,
                    role=role,
                )
                gr_stmt = gr_stmt.on_conflict_do_nothing()
                await self.db.execute(gr_stmt)
                result["referees"] += 1

        # Sync coaches for home and away teams
        # SOTA API returns coach, first_assistant, second_assistant in each team object
        coach_role_map = {
            "coach": CoachRole.head_coach,
            "first_assistant": CoachRole.assistant,
            "second_assistant": CoachRole.assistant,
        }

        for team_key, team_id in [("home_team", game.home_team_id), ("away_team", game.away_team_id)]:
            team_data = lineup_data.get(team_key, {})
            if not isinstance(team_data, dict):
                continue

            # Process each coach role
            for role_key, role_enum in coach_role_map.items():
                coach_data = team_data.get(role_key)
                if not coach_data or not isinstance(coach_data, dict):
                    continue

                first_name = coach_data.get("first_name") or ""
                last_name_raw = coach_data.get("last_name", [])
                # last_name comes as array from SOTA API
                if isinstance(last_name_raw, list):
                    last_name = last_name_raw[0] if last_name_raw else ""
                else:
                    last_name = str(last_name_raw) if last_name_raw else ""

                # Skip if both names are empty or None
                if not first_name or not last_name:
                    continue

                # Find existing coach by name
                existing = await self.db.execute(
                    select(Coach).where(
                        Coach.first_name == first_name,
                        Coach.last_name == last_name
                    )
                )
                coach = existing.scalar_one_or_none()

                if not coach:
                    # Create new coach
                    coach_stmt = insert(Coach).values(
                        first_name=first_name,
                        last_name=last_name,
                    )
                    coach_result = await self.db.execute(coach_stmt)
                    coach_id = coach_result.inserted_primary_key[0]
                else:
                    coach_id = coach.id

                # Create or update team-coach association
                # Use game date as start_date to track when coach was last seen
                tc_stmt = insert(TeamCoach).values(
                    team_id=team_id,
                    coach_id=coach_id,
                    season_id=game.season_id,
                    role=role_enum,
                    is_active=True,
                    start_date=game.date,
                )
                # Update with latest game date if coach already exists
                tc_stmt = tc_stmt.on_conflict_do_update(
                    index_elements=['team_id', 'coach_id', 'season_id', 'role'],
                    set_={
                        'start_date': tc_stmt.excluded.start_date,
                        'is_active': True,
                    }
                )
                await self.db.execute(tc_stmt)
                result["coaches"] += 1

        # Ensure player exists in DB before inserting lineup (auto-create if missing)
        async def ensure_player_exists(player_data: dict, team_id: int):
            pid_str = player_data.get("id")
            if not pid_str:
                return
            try:
                pid = UUID(pid_str)
            except (ValueError, TypeError):
                return
            existing = await self.db.execute(select(Player.id).where(Player.id == pid))
            if existing.scalar_one_or_none() is None:
                first_name = player_data.get("first_name", "")
                last_name_raw = player_data.get("last_name", [])
                last_name = last_name_raw[0] if isinstance(last_name_raw, list) and last_name_raw else str(last_name_raw) if last_name_raw else ""
                stmt = insert(Player).values(
                    id=pid,
                    first_name=first_name,
                    last_name=last_name,
                ).on_conflict_do_nothing()
                await self.db.execute(stmt)
                # Also link to team
                pt_stmt = insert(PlayerTeam).values(
                    player_id=pid,
                    team_id=team_id,
                    season_id=game.season_id,
                ).on_conflict_do_nothing()
                await self.db.execute(pt_stmt)
                logger.info(f"Auto-created missing player {first_name} {last_name} ({pid})")

        # Sync lineups for home and away teams
        # SOTA API returns all players in "lineup" field, with substitutes field empty
        # Starters = first 10 field players (is_gk=false) + first goalkeeper (is_gk=true)
        for team_key, team_id in [("home_team", game.home_team_id), ("away_team", game.away_team_id)]:
            team_data = lineup_data.get(team_key, {})
            if not isinstance(team_data, dict):
                continue

            # Get all players from lineup
            all_players = team_data.get("lineup", [])

            # Also check substitutes field (in case SOTA API changes behavior)
            explicit_substitutes = team_data.get("substitutes", [])

            # If substitutes are explicitly provided, use them
            if explicit_substitutes:
                starters = all_players
                substitutes = explicit_substitutes
            else:
                # Build starters: first 10 field players + first goalkeeper
                field_players = [p for p in all_players if not p.get("is_gk")]
                goalkeepers = [p for p in all_players if p.get("is_gk")]

                # Starters = first 10 field players + first goalkeeper
                starter_field = field_players[:10]
                starter_gk = goalkeepers[:1] if goalkeepers else []
                starters = starter_gk + starter_field  # GK first, then field players

                # Substitutes = remaining field players + remaining goalkeepers
                sub_field = field_players[10:]
                sub_gk = goalkeepers[1:]
                substitutes = sub_gk + sub_field

            # Process starters
            for player in starters:
                player_id_str = player.get("id")
                if not player_id_str:
                    continue

                try:
                    player_id = UUID(player_id_str)
                except (ValueError, TypeError):
                    continue

                await ensure_player_exists(player, team_id)

                gl_stmt = insert(GameLineup).values(
                    game_id=game_uuid,
                    team_id=team_id,
                    player_id=player_id,
                    lineup_type=LineupType.starter,
                    shirt_number=player.get("number"),
                    is_captain=player.get("is_captain", False),
                )
                gl_stmt = gl_stmt.on_conflict_do_update(
                    index_elements=["game_id", "player_id"],
                    set_={
                        "lineup_type": gl_stmt.excluded.lineup_type,
                        "shirt_number": gl_stmt.excluded.shirt_number,
                        "is_captain": gl_stmt.excluded.is_captain,
                    },
                )
                await self.db.execute(gl_stmt)
                result["lineups"] += 1

            # Process substitutes
            for player in substitutes:
                player_id_str = player.get("id")
                if not player_id_str:
                    continue

                try:
                    player_id = UUID(player_id_str)
                except (ValueError, TypeError):
                    continue

                await ensure_player_exists(player, team_id)

                gl_stmt = insert(GameLineup).values(
                    game_id=game_uuid,
                    team_id=team_id,
                    player_id=player_id,
                    lineup_type=LineupType.substitute,
                    shirt_number=player.get("number"),
                    is_captain=False,
                )
                gl_stmt = gl_stmt.on_conflict_do_update(
                    index_elements=["game_id", "player_id"],
                    set_={
                        "lineup_type": gl_stmt.excluded.lineup_type,
                        "shirt_number": gl_stmt.excluded.shirt_number,
                        "is_captain": gl_stmt.excluded.is_captain,
                    },
                )
                await self.db.execute(gl_stmt)
                result["lineups"] += 1

        # Update game has_lineup flag
        if result["lineups"] > 0:
            await self.db.execute(
                Game.__table__.update()
                .where(Game.id == game_uuid)
                .values(has_lineup=True)
            )

        await self.db.commit()

        # Also sync positions from live endpoint
        await self.sync_live_lineup_positions(game_id)

        return result

    async def sync_live_lineup_positions(self, game_id: str) -> dict:
        """
        Sync player positions from live endpoint for a specific game.
        Updates amplua and field_position fields in GameLineup.
        If a player from live endpoint doesn't exist in GameLineup, adds them.

        SOTA live endpoint returns:
        - amplua: position category (Gk, D, DM, M, AM, F) - only for starters
        - position: field side (C, L, R, LC, RC) - only for starters
        - Players without amplua are substitutes
        """
        game_uuid = UUID(game_id)
        result = {"positions_updated": 0, "players_added": 0}

        # Get game to know home/away team IDs
        game_result = await self.db.execute(
            select(Game).where(Game.id == game_uuid)
        )
        game = game_result.scalar_one_or_none()
        if not game:
            return result

        for side, team_id in [("home", game.home_team_id), ("away", game.away_team_id)]:
            if not team_id:
                continue

            try:
                live_data = await self.client.get_live_team_lineup(game_id, side)
            except Exception as e:
                logger.warning(f"Failed to fetch live lineup for game {game_id} {side}: {e}")
                continue

            for player in live_data:
                # Get shirt number - skip metadata entries (TEAM, FORMATION, COACH, etc.)
                shirt_number = player.get("number")
                if not isinstance(shirt_number, int):
                    continue

                player_id_str = player.get("id")
                amplua = player.get("amplua")  # Gk, D, DM, M, AM, F - only starters have this
                field_position = player.get("position")  # C, L, R, LC, RC
                is_captain = player.get("capitan", False)

                # Determine lineup type: players with amplua are starters
                lineup_type = LineupType.starter if amplua else LineupType.substitute

                # Try to update existing entry by shirt_number + team_id
                update_result = await self.db.execute(
                    GameLineup.__table__.update()
                    .where(
                        GameLineup.game_id == game_uuid,
                        GameLineup.team_id == team_id,
                        GameLineup.shirt_number == shirt_number
                    )
                    .values(
                        amplua=amplua,
                        field_position=field_position,
                        lineup_type=lineup_type,
                        is_captain=is_captain
                    )
                )

                if update_result.rowcount > 0:
                    if amplua:
                        result["positions_updated"] += 1
                elif player_id_str:
                    # Entry doesn't exist - try to insert new one
                    try:
                        player_uuid = UUID(player_id_str)
                        # Check if player exists in our database
                        player_exists = await self.db.execute(
                            select(Player.id).where(Player.id == player_uuid)
                        )
                        if player_exists.scalar_one_or_none():
                            # Insert new GameLineup entry
                            await self.db.execute(
                                GameLineup.__table__.insert().values(
                                    game_id=game_uuid,
                                    team_id=team_id,
                                    player_id=player_uuid,
                                    shirt_number=shirt_number,
                                    lineup_type=lineup_type,
                                    amplua=amplua,
                                    field_position=field_position,
                                    is_captain=is_captain
                                )
                            )
                            result["players_added"] += 1
                            if amplua:
                                result["positions_updated"] += 1
                    except (ValueError, TypeError):
                        continue

        await self.db.commit()
        return result

    async def full_sync(self, season_id: int | None = None) -> dict:
        """Perform a full synchronization."""
        if season_id is None:
            season_id = settings.current_season_id

        results = {
            "tournaments": await self.sync_tournaments(),
            "seasons": await self.sync_seasons(),
            "teams": await self.sync_teams(),
            "players": await self.sync_players(season_id),
            "games": await self.sync_games(season_id),
            "score_table": await self.sync_score_table(season_id),
            "team_season_stats": await self.sync_team_season_stats(season_id),
            "player_season_stats": await self.sync_player_season_stats(season_id),
        }

        # Sync stats and lineups for games with has_stats=True
        games_with_stats = await self.db.execute(
            select(Game.id).where(Game.season_id == season_id, Game.has_stats == True)
        )
        game_ids = [str(g[0]) for g in games_with_stats.fetchall()]

        stats_synced = 0
        lineups_synced = 0
        for game_id in game_ids:
            try:
                await self.sync_game_stats(game_id)
                stats_synced += 1
            except Exception as e:
                logger.warning(f"Failed to sync stats for game {game_id}: {e}")

            try:
                # Sync pre-game lineup (referees, coaches, lineups)
                lineup_result = await self.sync_pre_game_lineup(game_id)
                if lineup_result["lineups"] > 0:
                    lineups_synced += 1
            except Exception as e:
                logger.warning(f"Failed to sync lineup for game {game_id}: {e}")

        results["game_stats_synced"] = stats_synced
        results["game_lineups_synced"] = lineups_synced

        # Sync stadium and time from live /em/ endpoints
        metadata_result = await self.sync_game_metadata_from_live(season_id)
        results["metadata_synced"] = metadata_result["metadata_updated"]

        return results
