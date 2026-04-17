from pydantic import BaseModel


class AdminTeamUpdateRequest(BaseModel):
    name: str | None = None
    name_kz: str | None = None
    name_en: str | None = None
    website: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_en: str | None = None
    stadium_id: int | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None
    logo_url: str | None = None
    # Foundation year lives on the parent Club row; edits from the team
    # form write through to team.club.founded_year. Teams without a club
    # reject writes with 400.
    founded_year: int | None = None


class AdminTeamListItem(BaseModel):
    id: int
    name: str
    stadium_id: int | None = None
    stadium_name: str | None = None


class AdminTeamsListResponse(BaseModel):
    items: list[AdminTeamListItem]
    total: int


class AdminTeamDetailResponse(BaseModel):
    id: int
    name: str
    name_kz: str | None = None
    name_en: str | None = None
    website: str | None = None
    city: str | None = None
    city_kz: str | None = None
    city_en: str | None = None
    stadium_id: int | None = None
    stadium_name: str | None = None
    primary_color: str | None = None
    secondary_color: str | None = None
    accent_color: str | None = None
    logo_url: str | None = None
    # Resolved from the parent Club row via team.club.founded_year.
    # Null when the team has no associated club.
    founded_year: int | None = None
    has_club: bool = False
