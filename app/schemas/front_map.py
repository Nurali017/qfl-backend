from pydantic import BaseModel


class FrontMapEntry(BaseModel):
    season_id: int | None = None
    name: str | None = None
    tournament_type: str | None = None
    tournament_format: str | None = None
    has_table: bool = False
    has_bracket: bool = False
    sponsor_name: str | None = None
    logo: str | None = None
    colors: dict | None = None
    current_round: int | None = None
    total_rounds: int | None = None
    sort_order: int = 0


class FrontMapResponse(BaseModel):
    items: dict[str, FrontMapEntry]
