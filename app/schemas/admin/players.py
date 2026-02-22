from datetime import date
from uuid import UUID

from pydantic import BaseModel, Field


class AdminPlayerTeamBindingInput(BaseModel):
    team_id: int
    season_id: int
    number: int | None = None


class AdminPlayerTeamBindingResponse(AdminPlayerTeamBindingInput):
    team_name: str | None = None
    season_name: str | None = None


class AdminPlayerCreateRequest(BaseModel):
    sota_id: UUID | None = None
    first_name: str | None = None
    first_name_kz: str | None = None
    first_name_en: str | None = None
    last_name: str | None = None
    last_name_kz: str | None = None
    last_name_en: str | None = None
    birthday: date | None = None
    player_type: str | None = None
    country_id: int | None = None
    photo_url: str | None = None
    age: int | None = None
    top_role: str | None = None
    top_role_kz: str | None = None
    top_role_en: str | None = None
    team_bindings: list[AdminPlayerTeamBindingInput] = Field(default_factory=list)


class AdminPlayerUpdateRequest(BaseModel):
    sota_id: UUID | None = None
    first_name: str | None = None
    first_name_kz: str | None = None
    first_name_en: str | None = None
    last_name: str | None = None
    last_name_kz: str | None = None
    last_name_en: str | None = None
    birthday: date | None = None
    player_type: str | None = None
    country_id: int | None = None
    photo_url: str | None = None
    age: int | None = None
    top_role: str | None = None
    top_role_kz: str | None = None
    top_role_en: str | None = None
    team_bindings: list[AdminPlayerTeamBindingInput] | None = None


class AdminPlayerResponse(BaseModel):
    id: int
    sota_id: UUID | None = None
    first_name: str | None = None
    first_name_kz: str | None = None
    first_name_en: str | None = None
    last_name: str | None = None
    last_name_kz: str | None = None
    last_name_en: str | None = None
    birthday: date | None = None
    player_type: str | None = None
    country_id: int | None = None
    photo_url: str | None = None
    age: int | None = None
    top_role: str | None = None
    top_role_kz: str | None = None
    top_role_en: str | None = None
    team_bindings: list[AdminPlayerTeamBindingResponse] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class AdminPlayersListResponse(BaseModel):
    items: list[AdminPlayerResponse]
    total: int


class AdminMetaCountry(BaseModel):
    id: int
    code: str | None = None
    name: str


class AdminMetaTeam(BaseModel):
    id: int
    name: str


class AdminMetaSeason(BaseModel):
    id: int
    name: str


class AdminPlayersMetaResponse(BaseModel):
    countries: list[AdminMetaCountry]
    teams: list[AdminMetaTeam]
    seasons: list[AdminMetaSeason]
