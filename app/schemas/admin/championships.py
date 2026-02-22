from pydantic import BaseModel


class AdminChampionshipCreateRequest(BaseModel):
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    short_name: str | None = None
    short_name_kz: str | None = None
    short_name_en: str | None = None
    slug: str | None = None
    sort_order: int = 0
    is_active: bool = True


class AdminChampionshipUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    short_name: str | None = None
    short_name_kz: str | None = None
    short_name_en: str | None = None
    slug: str | None = None
    sort_order: int | None = None
    is_active: bool | None = None


class AdminChampionshipResponse(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    short_name: str | None = None
    short_name_kz: str | None = None
    short_name_en: str | None = None
    slug: str | None = None
    sort_order: int = 0
    is_active: bool = True

    model_config = {"from_attributes": True}


class AdminChampionshipsListResponse(BaseModel):
    items: list[AdminChampionshipResponse]
    total: int
