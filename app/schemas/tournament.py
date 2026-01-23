from pydantic import BaseModel


class TournamentBase(BaseModel):
    id: int
    name: str
    country_code: str | None = None
    country_name: str | None = None


class TournamentResponse(TournamentBase):
    class Config:
        from_attributes = True


class TournamentFromSOTA(BaseModel):
    id: int
    name: str
    country_code: str | None = None
    country_name: str | None = None
