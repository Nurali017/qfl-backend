from pydantic import BaseModel


class CityResponse(BaseModel):
    id: int
    name: str
    country_id: int | None = None
    country_name: str | None = None

    class Config:
        from_attributes = True


class CityListResponse(BaseModel):
    items: list[CityResponse]
    total: int
