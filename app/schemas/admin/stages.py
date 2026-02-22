from pydantic import BaseModel


class AdminStageCreateRequest(BaseModel):
    season_id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    stage_number: int | None = None
    sort_order: int = 0


class AdminStageUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    stage_number: int | None = None
    sort_order: int | None = None


class AdminStageResponse(BaseModel):
    id: int
    season_id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    stage_number: int | None = None
    sort_order: int = 0

    model_config = {"from_attributes": True}


class AdminStagesListResponse(BaseModel):
    items: list[AdminStageResponse]
    total: int
