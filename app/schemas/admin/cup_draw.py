from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from app.schemas.cup_draw import CupDrawTeamBrief


class AddPairRequest(BaseModel):
    team1_id: int
    team2_id: int
    sort_order: int
    side: Literal["left", "right", "center"]


class DrawPairResponse(BaseModel):
    team1_id: int
    team2_id: int
    sort_order: int
    side: str
    is_published: bool
    team1: CupDrawTeamBrief | None = None
    team2: CupDrawTeamBrief | None = None


class CupDrawResponse(BaseModel):
    id: int
    season_id: int
    round_key: str
    status: str
    pairs: list[DrawPairResponse]
    created_at: datetime
    updated_at: datetime


class CupDrawListResponse(BaseModel):
    items: list[CupDrawResponse]


class ParticipantTeamResponse(BaseModel):
    team_id: int
    team_name: str
    team_logo: str | None = None
