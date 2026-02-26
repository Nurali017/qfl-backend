from uuid import UUID

from pydantic import BaseModel


class AdminContractListItem(BaseModel):
    id: int
    player_id: int
    player_last_name: str | None
    player_first_name: str | None
    player_sota_id: UUID | None
    team_id: int
    team_name: str | None
    season_id: int
    season_name: str | None
    role: int | None           # 1=Игрок, 2=Тренер, 3=Сотрудник, 4=Администрация
    amplua: int | None         # 1=Вратарь, 2=Защитник, 3=Полузащитник, 4=Нападающий
    number: int | None
    position_ru: str | None
    position_kz: str | None
    position_en: str | None
    photo_url: str | None
    is_active: bool
    is_hidden: bool


class AdminContractCreateRequest(BaseModel):
    player_id: int
    team_id: int
    season_id: int
    role: int = 1
    amplua: int | None = None
    number: int | None = None
    position_ru: str | None = None
    position_kz: str | None = None
    position_en: str | None = None
    photo_url: str | None = None
    is_active: bool = True
    is_hidden: bool = False


class AdminContractUpdateRequest(BaseModel):
    player_id: int | None = None
    team_id: int | None = None
    season_id: int | None = None
    role: int | None = None
    amplua: int | None = None
    number: int | None = None
    position_ru: str | None = None
    position_kz: str | None = None
    position_en: str | None = None
    photo_url: str | None = None
    is_active: bool | None = None
    is_hidden: bool | None = None


class AdminContractResponse(AdminContractListItem):
    pass


class AdminContractsListResponse(BaseModel):
    items: list[AdminContractListItem]
    total: int


class AdminContractMetaPlayer(BaseModel):
    id: int
    last_name: str | None
    first_name: str | None
    sota_id: UUID | None


class AdminContractMetaTeam(BaseModel):
    id: int
    name: str


class AdminContractMetaSeason(BaseModel):
    id: int
    name: str
    championship_name: str | None


class AdminContractMetaResponse(BaseModel):
    players: list[AdminContractMetaPlayer]
    teams: list[AdminContractMetaTeam]
    seasons: list[AdminContractMetaSeason]
