from pydantic import BaseModel


class FrontMapEntry(BaseModel):
    season_id: int | None = None
    stages: dict[str, int] | None = None


class FrontMapResponse(BaseModel):
    items: dict[str, FrontMapEntry]
