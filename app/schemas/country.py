from pydantic import BaseModel, Field

from app.utils.file_urls import FileUrl


class CountryBase(BaseModel):
    code: str = Field(..., min_length=2, max_length=2, description="ISO 3166-1 alpha-2 code")
    name: str = Field(..., max_length=100, description="Country name in Russian")
    name_kz: str | None = Field(None, max_length=100)
    name_en: str | None = Field(None, max_length=100)


class CountryCreate(CountryBase):
    pass


class CountryUpdate(BaseModel):
    code: str | None = Field(None, min_length=2, max_length=2)
    name: str | None = Field(None, max_length=100)
    name_kz: str | None = None
    name_en: str | None = None


class CountryResponse(CountryBase):
    id: int
    flag_url: FileUrl = None
    is_active: bool = True

    class Config:
        from_attributes = True


class CountryListResponse(BaseModel):
    items: list[CountryResponse]
    total: int


class CountryInPlayer(BaseModel):
    """Minimal country info for nested responses (player, coach, referee)."""
    id: int
    code: str
    name: str
    flag_url: FileUrl = None

    class Config:
        from_attributes = True
