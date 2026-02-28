from pydantic import BaseModel


class AdminCoachMetaCoach(BaseModel):
    id: int
    last_name: str | None
    first_name: str | None
    photo_url: str | None


class AdminCoachMetaTeam(BaseModel):
    id: int
    name: str


class AdminCoachMetaSeason(BaseModel):
    id: int
    name: str
    championship_name: str | None


class AdminCoachMetaCountry(BaseModel):
    id: int
    code: str | None
    name: str


class AdminCoachMetaResponse(BaseModel):
    coaches: list[AdminCoachMetaCoach]
    teams: list[AdminCoachMetaTeam]
    seasons: list[AdminCoachMetaSeason]
    countries: list[AdminCoachMetaCountry]


class AdminCoachAssignmentListItem(BaseModel):
    id: int
    coach_id: int
    coach_first_name: str | None
    coach_last_name: str | None
    coach_photo_url: str | None
    team_id: int
    team_name: str | None
    season_id: int | None
    season_name: str | None
    role: str
    is_active: bool
    start_date: str | None
    end_date: str | None


class AdminCoachAssignmentsListResponse(BaseModel):
    items: list[AdminCoachAssignmentListItem]
    total: int


class AdminInlineCoachCreateRequest(BaseModel):
    first_name: str
    last_name: str
    first_name_kz: str | None = None
    first_name_ru: str | None = None
    first_name_en: str | None = None
    last_name_kz: str | None = None
    last_name_ru: str | None = None
    last_name_en: str | None = None
    photo_url: str | None = None
    country_id: int | None = None


class AdminCoachAssignmentCreateRequest(BaseModel):
    coach_id: int | None = None
    inline_coach: AdminInlineCoachCreateRequest | None = None
    team_id: int
    season_id: int | None = None
    role: str = "head_coach"
    is_active: bool = True
    start_date: str | None = None
    end_date: str | None = None


class AdminCoachAssignmentUpdateRequest(BaseModel):
    coach_id: int | None = None
    team_id: int | None = None
    season_id: int | None = None
    role: str | None = None
    is_active: bool | None = None
    start_date: str | None = None
    end_date: str | None = None


class AdminCoachAssignmentResponse(AdminCoachAssignmentListItem):
    pass


class AdminCoachBulkCopyRequest(BaseModel):
    source_season_id: int
    target_season_id: int
    team_id: int
    excluded_coach_ids: list[int] = []
    override_role: str | None = None


class AdminCoachBulkCopyResponse(BaseModel):
    created: int
    skipped: int
    excluded: int
