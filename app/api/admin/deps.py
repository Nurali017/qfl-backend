from collections.abc import Callable

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import AdminUser
from app.schemas.admin.common import AdminRole
from app.security.jwt import AccessTokenError, decode_access_token

bearer_scheme = HTTPBearer(auto_error=False)


def _unauthorized(detail: str = "Unauthorized") -> HTTPException:
    return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


async def get_current_admin(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> AdminUser:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise _unauthorized("Missing bearer token")

    try:
        payload = decode_access_token(credentials.credentials)
        user_id = int(payload["sub"])
    except (AccessTokenError, ValueError):
        raise _unauthorized("Invalid access token")

    result = await db.execute(select(AdminUser).where(AdminUser.id == user_id, AdminUser.is_active == True))
    user = result.scalar_one_or_none()
    if not user:
        raise _unauthorized("Admin user not found or inactive")

    return user


def require_roles(*allowed_roles: AdminRole) -> Callable:
    async def _role_guard(current_admin: AdminUser = Depends(get_current_admin)) -> AdminUser:
        if current_admin.role not in allowed_roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return current_admin

    return _role_guard
