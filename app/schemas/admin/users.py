from datetime import datetime

from pydantic import BaseModel

from app.schemas.admin.common import AdminRole


class AdminUserCreateRequest(BaseModel):
    email: str
    password: str
    role: AdminRole
    is_active: bool = True


class AdminUserUpdateRequest(BaseModel):
    email: str | None = None
    role: AdminRole | None = None
    is_active: bool | None = None


class AdminPasswordResetRequest(BaseModel):
    new_password: str


class AdminUserResponse(BaseModel):
    id: int
    email: str
    role: AdminRole
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
