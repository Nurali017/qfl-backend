from uuid import UUID

from pydantic import BaseModel


class AdminPlayoffBracketCreateRequest(BaseModel):
    season_id: int
    round_name: str
    side: str = "left"
    sort_order: int = 1
    game_id: UUID | None = None
    is_visible: bool = True
    is_third_place: bool = False


class AdminPlayoffBracketUpdateRequest(BaseModel):
    round_name: str | None = None
    side: str | None = None
    sort_order: int | None = None
    game_id: UUID | None = None
    is_visible: bool | None = None
    is_third_place: bool | None = None


class AdminPlayoffBracketResponse(BaseModel):
    id: int
    season_id: int
    round_name: str
    side: str = "left"
    sort_order: int = 1
    game_id: UUID | None = None
    is_visible: bool = True
    is_third_place: bool = False

    model_config = {"from_attributes": True}


class AdminPlayoffBracketsListResponse(BaseModel):
    items: list[AdminPlayoffBracketResponse]
    total: int
