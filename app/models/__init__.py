from app.models.tournament import Tournament
from app.models.season import Season
from app.models.team import Team
from app.models.country import Country
from app.models.player import Player
from app.models.player_team import PlayerTeam
from app.models.game import Game
from app.models.game_team_stats import GameTeamStats
from app.models.game_player_stats import GamePlayerStats
from app.models.score_table import ScoreTable
from app.models.team_season_stats import TeamSeasonStats
from app.models.player_season_stats import PlayerSeasonStats
from app.models.page import Page, Language
from app.models.news import News, NewsLike
from app.models.admin_user import AdminUser
from app.models.admin_session import AdminSession

# New models
from app.models.stadium import Stadium
from app.models.referee import Referee
from app.models.coach import Coach, TeamCoach, CoachRole
from app.models.game_referee import GameReferee, RefereeRole
from app.models.game_lineup import GameLineup, LineupType
from app.models.game_event import GameEvent, GameEventType

# Legacy migration models
from app.models.championship import Championship
from app.models.city import City
from app.models.club import Club
from app.models.stage import Stage
from app.models.playoff_bracket import PlayoffBracket
from app.models.team_tournament import TeamTournament
from app.models.partner import Partner

__all__ = [
    "Tournament",
    "Season",
    "Team",
    "Country",
    "Player",
    "PlayerTeam",
    "Game",
    "GameTeamStats",
    "GamePlayerStats",
    "ScoreTable",
    "TeamSeasonStats",
    "PlayerSeasonStats",
    "Page",
    "News",
    "NewsLike",
    "AdminUser",
    "AdminSession",
    "Language",
    # New models
    "Stadium",
    "Referee",
    "Coach",
    "TeamCoach",
    "CoachRole",
    "GameReferee",
    "RefereeRole",
    "GameLineup",
    "LineupType",
    "GameEvent",
    "GameEventType",
    # Legacy migration models
    "Championship",
    "City",
    "Club",
    "Stage",
    "PlayoffBracket",
    "TeamTournament",
    "Partner",
]
