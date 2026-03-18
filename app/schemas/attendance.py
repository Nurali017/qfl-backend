"""Attendance statistics schemas."""

from pydantic import BaseModel


class AttendanceSummary(BaseModel):
    total_matches: int = 0
    total_attendance: int = 0
    average_attendance: float = 0.0


class TopAttendanceMatch(BaseModel):
    game_id: int
    tour: int | None = None
    date: str | None = None
    home_team_id: int | None = None
    home_team_name: str | None = None
    home_team_logo: str | None = None
    home_score: int | None = None
    away_team_id: int | None = None
    away_team_name: str | None = None
    away_team_logo: str | None = None
    away_score: int | None = None
    visitors: int = 0
    stadium_name: str | None = None
    city: str | None = None


class AttendanceByTeam(BaseModel):
    team_id: int
    team_name: str | None = None
    team_logo: str | None = None
    home_matches: int = 0
    home_attendance: int = 0
    away_matches: int = 0
    away_attendance: int = 0
    total_matches: int = 0
    total_attendance: int = 0
    average_attendance: float = 0.0


class AttendanceByStadium(BaseModel):
    stadium_id: int
    stadium_name: str | None = None
    city: str | None = None
    matches: int = 0
    total_attendance: int = 0
    average_attendance: float = 0.0


class AttendanceByTour(BaseModel):
    tour: int
    matches: int = 0
    total_attendance: int = 0
    average_attendance: float = 0.0


class AttendanceResponse(BaseModel):
    season_id: int
    max_completed_round: int | None = None
    summary: AttendanceSummary
    top_matches: list[TopAttendanceMatch]
    by_team: list[AttendanceByTeam]
    by_stadium: list[AttendanceByStadium]
    by_tour: list[AttendanceByTour]
