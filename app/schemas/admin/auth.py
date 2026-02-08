from pydantic import BaseModel

from app.schemas.admin.common import AdminRole


class LoginRequest(BaseModel):
    email: str
    password: str


class AuthUserResponse(BaseModel):
    id: int
    email: str
    role: AdminRole
    is_active: bool


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: AuthUserResponse


class MeResponse(AuthUserResponse):
    pass
