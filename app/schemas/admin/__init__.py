from app.schemas.admin.auth import LoginRequest, LoginResponse, MeResponse
from app.schemas.admin.users import AdminPasswordResetRequest, AdminUserCreateRequest, AdminUserResponse, AdminUserUpdateRequest
from app.schemas.admin.news import (
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

__all__ = [
    "LoginRequest",
    "LoginResponse",
    "MeResponse",
    "AdminPasswordResetRequest",
    "AdminUserCreateRequest",
    "AdminUserResponse",
    "AdminUserUpdateRequest",
    "AdminNewsMaterialCreateRequest",
    "AdminNewsMaterialResponse",
    "AdminNewsMaterialUpdateRequest",
    "AdminNewsTranslationCreateRequest",
    "AdminPageMaterialResponse",
    "AdminPageMaterialUpdateRequest",
    "AdminPageTranslationCreateRequest",
]
