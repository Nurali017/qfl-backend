from pydantic import BaseModel


class TournamentBase(BaseModel):
    id: int
    name: str
    country_code: str | None = None
    country_name: str | None = None


class TournamentResponse(TournamentBase):
    championship_id: int | None = None
    championship_name: str | None = None

    class Config:
        from_attributes = True


class TournamentFromSOTA(BaseModel):
    id: int
    name: str
    country_code: str | None = None
    country_name: str | None = None
