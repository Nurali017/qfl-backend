from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class SeasonParticipantResponse(BaseModel):
    id: int
    team_id: int
    team_name: str | None = None
    team_logo: FileUrl = None
    season_id: int
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    sort_order: int = 0

    class Config:
        from_attributes = True


class SeasonParticipantListResponse(BaseModel):
    items: list[SeasonParticipantResponse]
    total: int


class SeasonGroupsResponse(BaseModel):
    season_id: int
    groups: dict[str, list[SeasonParticipantResponse]]
