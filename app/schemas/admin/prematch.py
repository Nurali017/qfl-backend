from typing import Optional

from pydantic import BaseModel


class PrematchPlayerMatch(BaseModel):
    pdf_name: str
    pdf_shirt_number: int
    lineup_type: str  # "starter" or "substitute"
    is_goalkeeper: bool = False
    is_captain: bool = False
    matched: bool
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    match_method: Optional[str] = None  # "name+number", "name", "shirt_number", "last_name"


class PrematchTeamResult(BaseModel):
    team_id: int
    team_name: Optional[str] = None
    players: list[PrematchPlayerMatch]
    matched_count: int
    unmatched_count: int


class PrematchImportResponse(BaseModel):
    game_id: int
    home: PrematchTeamResult
    away: PrematchTeamResult
    total_matched: int
    total_unmatched: int
    lineup_created: bool
    warnings: list[str] = []
