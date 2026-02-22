from pydantic import BaseModel


class AdminCityCreateRequest(BaseModel):
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    country_id: int | None = None


class AdminCityUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    country_id: int | None = None


class AdminCityResponse(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    country_id: int | None = None

    model_config = {"from_attributes": True}


class AdminCitiesListResponse(BaseModel):
    items: list[AdminCityResponse]
    total: int
