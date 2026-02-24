"""
Schemas for Head-to-Head (H2H) statistics between two teams.
"""

from datetime import date
from pydantic import BaseModel


class H2HOverallStats(BaseModel):
    """Overall head-to-head statistics between two teams."""
    total_matches: int
    team1_wins: int
    draws: int
    team2_wins: int
    team1_goals: int
    team2_goals: int
    team1_home_wins: int
    team1_away_wins: int
    team2_home_wins: int
    team2_away_wins: int


class FormGuideMatch(BaseModel):
    """Single match in form guide (W/D/L)."""
    game_id: int
    date: date
    result: str  # "W", "D", or "L"
    opponent_id: int
    opponent_name: str
    opponent_logo_url: str | None
    home_score: int | None
    away_score: int | None
    was_home: bool  # True if team played at home


class FormGuide(BaseModel):
    """Last 5 matches for a team."""
    team_id: int
    team_name: str
    matches: list[FormGuideMatch]  # Max 5 recent matches


class SeasonTableEntry(BaseModel):
    """Team's position in the season table."""
    position: int | None
    team_id: int
    team_name: str
    logo_url: str | None
    games_played: int
    wins: int
    draws: int
    losses: int
    goals_scored: int
    goals_conceded: int
    goal_difference: int
    points: int
    clean_sheets: int  # Games with 0 goals conceded


class PreviousMeeting(BaseModel):
    """Previous match between two teams."""
    game_id: int
    date: date
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    home_score: int | None
    away_score: int | None
    tour: int | None
    season_name: str | None
    home_team_logo: str | None
    away_team_logo: str | None


# --- Phase 1: Fun Facts & Aggregated Match Stats ---

class H2HBiggestWin(BaseModel):
    """Biggest win for a team in H2H history."""
    game_id: int
    date: date
    score: str  # e.g. "4-0"
    goal_difference: int


class H2HGoalsByHalf(BaseModel):
    """Goals scored by each team broken down by half."""
    team1_first_half: int
    team1_second_half: int
    team2_first_half: int
    team2_second_half: int


class H2HFunFacts(BaseModel):
    """Interesting facts about H2H history."""
    avg_goals_per_match: float  # Average total goals per match
    over_2_5_percent: float  # % of matches with total > 2.5
    btts_percent: float  # % of matches where Both Teams To Score
    team1_biggest_win: H2HBiggestWin | None
    team2_biggest_win: H2HBiggestWin | None
    team1_unbeaten_streak: int  # Max consecutive matches without loss
    team2_unbeaten_streak: int
    goals_by_half: H2HGoalsByHalf | None  # None if no GameEvent data
    team1_worst_defeat: H2HBiggestWin | None
    team2_worst_defeat: H2HBiggestWin | None


class H2HTeamMatchStats(BaseModel):
    """Aggregated match stats for one team across H2H matches."""
    avg_possession: float | None
    avg_shots: float | None
    avg_shots_on_goal: float | None
    avg_corners: float | None
    avg_fouls: float | None
    total_yellow_cards: int
    total_red_cards: int


class H2HAggregatedMatchStats(BaseModel):
    """Aggregated match statistics from GameTeamStats for H2H matches."""
    matches_with_stats: int  # How many matches have GameTeamStats data
    team1: H2HTeamMatchStats
    team2: H2HTeamMatchStats


# --- Phase 2: Top Performers ---

class H2HTopPerformer(BaseModel):
    """A player who performed well in H2H matches."""
    player_id: int
    player_name: str
    team_id: int
    photo_url: str | None
    count: int  # Goals or assists


class H2HTopPerformers(BaseModel):
    """Top scorers and assisters in H2H history."""
    top_scorers: list[H2HTopPerformer]
    top_assisters: list[H2HTopPerformer]


# --- Phase 3: Enhanced Season Stats ---

class H2HEnhancedSeasonTeamStats(BaseModel):
    """Enhanced season stats for one team."""
    xg: float | None
    xg_per_match: float | None
    possession_avg: float | None
    pass_accuracy_avg: float | None
    duel_ratio: float | None  # Duels won %
    shots_per_match: float | None


class H2HEnhancedSeasonStats(BaseModel):
    """Enhanced season comparison from TeamSeasonStats."""
    team1: H2HEnhancedSeasonTeamStats | None
    team2: H2HEnhancedSeasonTeamStats | None


class HeadToHeadResponse(BaseModel):
    """Complete head-to-head response."""
    team1_id: int
    team1_name: str
    team2_id: int
    team2_name: str
    season_id: int

    # Overall stats (all-time)
    overall: H2HOverallStats

    # Form guide (last 5 matches for each team in current season)
    form_guide: dict[str, FormGuide]  # Keys: "team1", "team2"

    # Season table positions
    season_table: list[SeasonTableEntry]

    # Previous meetings (chronological, most recent first)
    previous_meetings: list[PreviousMeeting]

    # Phase 1: Fun facts & aggregated match stats
    fun_facts: H2HFunFacts | None = None
    match_stats: H2HAggregatedMatchStats | None = None

    # Phase 2: Top performers
    top_performers: H2HTopPerformers | None = None

    # Phase 3: Enhanced season stats
    enhanced_season_stats: H2HEnhancedSeasonStats | None = None
