from pydantic import BaseModel

from app.schemas.admin.players import AdminMetaCountry


class AdminRefereeCreateRequest(BaseModel):
    first_name: str
    last_name: str
    first_name_kz: str | None = None
    first_name_ru: str | None = None
    first_name_en: str | None = None
    last_name_kz: str | None = None
    last_name_ru: str | None = None
    last_name_en: str | None = None
    country_id: int | None = None
    photo_url: str | None = None


class AdminRefereeUpdateRequest(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    first_name_kz: str | None = None
    first_name_ru: str | None = None
    first_name_en: str | None = None
    last_name_kz: str | None = None
    last_name_ru: str | None = None
    last_name_en: str | None = None
    country_id: int | None = None
    photo_url: str | None = None


class AdminRefereeResponse(BaseModel):
    id: int
    first_name: str
    last_name: str
    first_name_kz: str | None = None
    first_name_ru: str | None = None
    first_name_en: str | None = None
    last_name_kz: str | None = None
    last_name_ru: str | None = None
    last_name_en: str | None = None
    country_id: int | None = None
    country_name: str | None = None
    photo_url: str | None = None
    game_assignments_count: int = 0

    model_config = {"from_attributes": True}


class AdminRefereesListResponse(BaseModel):
    items: list[AdminRefereeResponse]
    total: int


class AdminRefereesMetaResponse(BaseModel):
    countries: list[AdminMetaCountry]
