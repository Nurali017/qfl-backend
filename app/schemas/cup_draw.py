from pydantic import BaseModel

from app.utils.file_urls import FileUrl


class CupDrawTeamBrief(BaseModel):
    id: int
    name: str
    logo_url: FileUrl = None

    class Config:
        from_attributes = True
