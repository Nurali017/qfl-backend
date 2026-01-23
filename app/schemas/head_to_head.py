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


class FormGuideMatch(BaseModel):
    """Single match in form guide (W/D/L)."""
    game_id: str
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
    game_id: str
    date: date
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    home_score: int | None
    away_score: int | None
    tour: int | None
    season_name: str | None


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
