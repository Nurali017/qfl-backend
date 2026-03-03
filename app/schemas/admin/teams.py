from pydantic import BaseModel


class AdminTeamUpdateRequest(BaseModel):
    stadium_id: int | None = None


class AdminTeamListItem(BaseModel):
    id: int
    name: str
    stadium_id: int | None = None
    stadium_name: str | None = None


class AdminTeamsListResponse(BaseModel):
    items: list[AdminTeamListItem]
    total: int
