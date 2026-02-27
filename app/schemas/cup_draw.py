from pydantic import BaseModel



class CupDrawTeamBrief(BaseModel):
    id: int
    name: str
    logo_url: str | None = None

    class Config:
        from_attributes = True
