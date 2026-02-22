from pydantic import BaseModel


class StageResponse(BaseModel):
    id: int
    season_id: int
    name: str
    stage_number: int | None = None
    sort_order: int = 0

    class Config:
        from_attributes = True


class StageListResponse(BaseModel):
    items: list[StageResponse]
    total: int
