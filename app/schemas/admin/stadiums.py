import datetime as dt

from pydantic import BaseModel, Field, field_validator


class AdminStadiumResponse(BaseModel):
    id: int
    legacy_id: int | None = None
    name: str
    name_kz: str | None = None
    name_ru: str | None = None
    name_en: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_ru: str | None = None
    city_en: str | None = None
    city_id: int | None = None
    capacity: int | None = None
    field_type: str | None = None
    address: str | None = None
    address_kz: str | None = None
    address_en: str | None = None
    photo_url: str | None = None
    created_at: dt.datetime | None = None
    updated_at: dt.datetime | None = None

    # Reference counts (populated on detail/delete checks; 0 for list)
    games_count: int = 0
    teams_count: int = 0
    clubs_count: int = 0
    participants_count: int = 0


def _normalize_field_type(value: str | None) -> str | None:
    """Empty string from the form means "no value"."""
    if value is None:
        return None
    value = value.strip()
    return value or None


class AdminStadiumCreateRequest(BaseModel):
    name: str
    name_kz: str | None = None
    name_ru: str | None = None
    name_en: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_ru: str | None = None
    city_en: str | None = None
    city_id: int | None = None
    capacity: int | None = Field(default=None, ge=0)
    field_type: str | None = None
    address: str | None = None
    address_kz: str | None = None
    address_en: str | None = None
    photo_url: str | None = None

    @field_validator("name")
    @classmethod
    def _name_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("name must not be empty")
        return v.strip()

    @field_validator("field_type")
    @classmethod
    def _field_type_blank_to_none(cls, v: str | None) -> str | None:
        return _normalize_field_type(v)


class AdminStadiumUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_ru: str | None = None
    name_en: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_ru: str | None = None
    city_en: str | None = None
    city_id: int | None = None
    capacity: int | None = Field(default=None, ge=0)
    field_type: str | None = None
    address: str | None = None
    address_kz: str | None = None
    address_en: str | None = None
    photo_url: str | None = None

    @field_validator("name")
    @classmethod
    def _name_not_blank(cls, v: str | None) -> str:
        # Omitting `name` keeps it unset (PATCH leaves it untouched); an explicit
        # null/blank would violate the NOT NULL column, so reject it here as 422.
        if v is None or not v.strip():
            raise ValueError("name must not be empty")
        return v.strip()

    @field_validator("field_type")
    @classmethod
    def _field_type_blank_to_none(cls, v: str | None) -> str | None:
        return _normalize_field_type(v)


class AdminStadiumsListResponse(BaseModel):
    items: list[AdminStadiumResponse]
    total: int
