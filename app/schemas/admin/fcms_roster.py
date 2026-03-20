from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel


class FcmsRosterLogListItem(BaseModel):
    id: int
    competition_name: str
    competition_id: int
    season_id: int
    status: str
    teams_synced: int
    total_auto_updates: int
    total_new_players: int
    total_auto_deactivated: int
    total_deregistered: int
    triggered_by: str
    started_at: datetime
    completed_at: datetime | None = None

    class Config:
        from_attributes = True


class FcmsRosterLogDetail(FcmsRosterLogListItem):
    results: list[dict] | None = None
    resolved_items: dict = {}
    error_message: str | None = None


class FcmsRosterLogsListResponse(BaseModel):
    items: list[FcmsRosterLogListItem]
    total: int


class FcmsCreatePlayerRequest(BaseModel):
    log_id: int
    item_key: str
    first_name: str
    last_name: str
    first_name_en: str | None = None
    last_name_en: str | None = None
    birthday: date | None = None
    fcms_person_id: int | None = None
    country_id: int | None = None
    team_id: int
    season_id: int
    number: int | None = None
    amplua: int


class FcmsLinkPlayerRequest(BaseModel):
    log_id: int
    item_key: str
    player_id: int
    fcms_person_id: int
    team_id: int
    season_id: int
    number: int | None = None
    amplua: int


class FcmsResolveRequest(BaseModel):
    item_key: str
    action: Literal["dismiss"]


class FcmsTriggerResponse(BaseModel):
    task_id: str
    message: str
