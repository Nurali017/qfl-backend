from app.schemas.admin.auth import LoginRequest, LoginResponse, MeResponse
from app.schemas.admin.users import AdminPasswordResetRequest, AdminUserCreateRequest, AdminUserResponse, AdminUserUpdateRequest
from app.schemas.admin.news import (
    AdminNewsArticleTypeUpdateRequest,
    AdminNewsClassifyRequest,
    AdminNewsClassifyResponse,
    AdminNewsMaterialCreateRequest,
    AdminNewsMaterialResponse,
    AdminNewsMaterialUpdateRequest,
    AdminNewsTranslationCreateRequest,
)
from app.schemas.admin.pages import (
    AdminPageMaterialResponse,
    AdminPageMaterialUpdateRequest,
    AdminPageTranslationCreateRequest,
)
from app.schemas.admin.players import (
    AdminPlayerCreateRequest,
    AdminPlayerResponse,
    AdminPlayersListResponse,
    AdminPlayersMetaResponse,
    AdminPlayerUpdateRequest,
)
from app.schemas.admin.championships import (
    AdminChampionshipCreateRequest,
    AdminChampionshipUpdateRequest,
    AdminChampionshipResponse,
    AdminChampionshipsListResponse,
)
from app.schemas.admin.stages import (
    AdminStageCreateRequest,
    AdminStageUpdateRequest,
    AdminStageResponse,
    AdminStagesListResponse,
)
from app.schemas.admin.clubs import (
    AdminClubCreateRequest,
    AdminClubUpdateRequest,
    AdminClubResponse,
    AdminClubsListResponse,
)
from app.schemas.admin.cities import (
    AdminCityCreateRequest,
    AdminCityUpdateRequest,
    AdminCityResponse,
    AdminCitiesListResponse,
)
from app.schemas.admin.partners import (
    AdminPartnerCreateRequest,
    AdminPartnerUpdateRequest,
    AdminPartnerResponse,
    AdminPartnersListResponse,
)
from app.schemas.admin.season_participants import (
    AdminSeasonParticipantCreateRequest,
    AdminSeasonParticipantUpdateRequest,
    AdminSeasonParticipantResponse,
    AdminSeasonParticipantsListResponse,
    AdminSeasonParticipantsBulkSetRequest,
    AdminSeasonParticipantsBulkSetResponse,
)
from app.schemas.admin.seasons import (
    AdminSeasonCreateRequest,
    AdminSeasonUpdateRequest,
    AdminSeasonResponse,
    AdminSeasonsListResponse,
)
from app.schemas.admin.cup_draw import (
    AddPairRequest,
    CupDrawListResponse,
    CupDrawResponse,
    DrawPairResponse,
    ParticipantTeamResponse,
)
from app.schemas.admin.games import (
    AdminGameResponse,
    AdminGameUpdateRequest,
    AdminGamesListResponse,
)

__all__ = [
    "LoginRequest",
    "LoginResponse",
    "MeResponse",
    "AdminPasswordResetRequest",
    "AdminUserCreateRequest",
    "AdminUserResponse",
    "AdminUserUpdateRequest",
    "AdminNewsArticleTypeUpdateRequest",
    "AdminNewsClassifyRequest",
    "AdminNewsClassifyResponse",
    "AdminNewsMaterialCreateRequest",
    "AdminNewsMaterialResponse",
    "AdminNewsMaterialUpdateRequest",
    "AdminNewsTranslationCreateRequest",
    "AdminPageMaterialResponse",
    "AdminPageMaterialUpdateRequest",
    "AdminPageTranslationCreateRequest",
    "AdminPlayerCreateRequest",
    "AdminPlayerResponse",
    "AdminPlayersListResponse",
    "AdminPlayersMetaResponse",
    "AdminPlayerUpdateRequest",
    "AdminChampionshipCreateRequest",
    "AdminChampionshipUpdateRequest",
    "AdminChampionshipResponse",
    "AdminChampionshipsListResponse",
    "AdminStageCreateRequest",
    "AdminStageUpdateRequest",
    "AdminStageResponse",
    "AdminStagesListResponse",
    "AdminClubCreateRequest",
    "AdminClubUpdateRequest",
    "AdminClubResponse",
    "AdminClubsListResponse",
    "AdminCityCreateRequest",
    "AdminCityUpdateRequest",
    "AdminCityResponse",
    "AdminCitiesListResponse",
    "AdminPartnerCreateRequest",
    "AdminPartnerUpdateRequest",
    "AdminPartnerResponse",
    "AdminPartnersListResponse",
    "AdminSeasonParticipantCreateRequest",
    "AdminSeasonParticipantUpdateRequest",
    "AdminSeasonParticipantResponse",
    "AdminSeasonParticipantsListResponse",
    "AdminSeasonParticipantsBulkSetRequest",
    "AdminSeasonParticipantsBulkSetResponse",
    "AdminSeasonCreateRequest",
    "AdminSeasonUpdateRequest",
    "AdminSeasonResponse",
    "AdminSeasonsListResponse",
    "AddPairRequest",
    "CupDrawListResponse",
    "CupDrawResponse",
    "DrawPairResponse",
    "ParticipantTeamResponse",
    "AdminGameResponse",
    "AdminGameUpdateRequest",
    "AdminGamesListResponse",
]
