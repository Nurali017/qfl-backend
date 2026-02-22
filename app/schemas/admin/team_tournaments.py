from pydantic import BaseModel


class AdminTeamTournamentCreateRequest(BaseModel):
    team_id: int
    season_id: int
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    stadium_id: int | None = None
    sort_order: int = 0


class AdminTeamTournamentUpdateRequest(BaseModel):
    group_name: str | None = None
    is_disqualified: bool | None = None
    fine_points: int | None = None
    stadium_id: int | None = None
    sort_order: int | None = None


class AdminTeamTournamentResponse(BaseModel):
    id: int
    team_id: int
    season_id: int
    group_name: str | None = None
    is_disqualified: bool = False
    fine_points: int = 0
    stadium_id: int | None = None
    sort_order: int = 0

    model_config = {"from_attributes": True}


class AdminTeamTournamentsListResponse(BaseModel):
    items: list[AdminTeamTournamentResponse]
    total: int
