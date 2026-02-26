from pydantic import BaseModel


class AdminSeasonParticipantCreateRequest(BaseModel):
    team_id: int
    season_id: int
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    stadium_id: int | None = None
    sort_order: int = 0


class AdminSeasonParticipantUpdateRequest(BaseModel):
    group_name: str | None = None
    is_disqualified: bool | None = None
    fine_points: int | None = None
    stadium_id: int | None = None
    sort_order: int | None = None


class AdminSeasonParticipantResponse(BaseModel):
    id: int
    team_id: int
    season_id: int
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    stadium_id: int | None = None
    sort_order: int = 0

    model_config = {"from_attributes": True}


class AdminSeasonParticipantsListResponse(BaseModel):
    items: list[AdminSeasonParticipantResponse]
    total: int


class AdminSeasonParticipantsBulkSetRequest(BaseModel):
    season_id: int
    team_ids: list[int]
    replace: bool = True


class AdminSeasonParticipantsBulkSetResponse(BaseModel):
    season_id: int
    total: int
    item_ids: list[int]
