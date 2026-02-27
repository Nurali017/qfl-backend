from datetime import date

from pydantic import BaseModel


class AdminSeasonCreateRequest(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    championship_id: int
    date_start: date | None = None
    date_end: date | None = None
    frontend_code: str | None = None
    tournament_type: str | None = None
    tournament_format: str | None = None
    has_table: bool = False
    has_bracket: bool = False
    sponsor_name: str | None = None
    sponsor_name_kz: str | None = None
    logo: str | None = None
    current_round: int | None = None
    total_rounds: int | None = None
    sort_order: int = 0
    colors: dict | None = None
    final_stage_ids: list[int] | None = None
    sync_enabled: bool = True
    is_visible: bool = True
    is_current: bool = False


class AdminSeasonUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    championship_id: int | None = None
    date_start: date | None = None
    date_end: date | None = None
    sync_enabled: bool | None = None
    is_visible: bool | None = None
    is_current: bool | None = None
    frontend_code: str | None = None
    tournament_type: str | None = None
    tournament_format: str | None = None
    has_table: bool | None = None
    has_bracket: bool | None = None
    sponsor_name: str | None = None
    sponsor_name_kz: str | None = None
    logo: str | None = None
    current_round: int | None = None
    total_rounds: int | None = None
    sort_order: int | None = None
    colors: dict | None = None
    final_stage_ids: list[int] | None = None


class AdminSeasonResponse(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    championship_id: int | None = None
    frontend_code: str | None = None
    tournament_type: str | None = None
    tournament_format: str | None = None
    has_table: bool = False
    has_bracket: bool = False
    sponsor_name: str | None = None
    sponsor_name_kz: str | None = None
    logo: str | None = None
    current_round: int | None = None
    total_rounds: int | None = None
    sort_order: int = 0
    colors: dict | None = None
    final_stage_ids: list[int] | None = None
    sync_enabled: bool = True
    is_visible: bool = True
    is_current: bool = False

    class Config:
        from_attributes = True


class AdminSeasonsListResponse(BaseModel):
    items: list[AdminSeasonResponse]
    total: int
