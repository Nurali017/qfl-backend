from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import get_current_admin
from app.api.deps import get_db
from app.config import get_settings
from app.models import AdminSession, AdminUser
from app.schemas.admin.auth import AuthUserResponse, LoginRequest, LoginResponse, MeResponse
from app.security import create_access_token, generate_refresh_token, hash_refresh_token, verify_password

settings = get_settings()
router = APIRouter(prefix="/auth", tags=["admin-auth"])


def _set_refresh_cookie(response: Response, refresh_token: str) -> None:
    response.set_cookie(
        key=settings.admin_refresh_cookie_name,
        value=refresh_token,
        max_age=settings.admin_refresh_ttl_days * 24 * 60 * 60,
        httponly=True,
        secure=settings.admin_cookie_secure,
        samesite=settings.admin_cookie_samesite,
        domain=settings.admin_cookie_domain,
        path="/",
    )


def _clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.admin_refresh_cookie_name,
        domain=settings.admin_cookie_domain,
        path="/",
        secure=settings.admin_cookie_secure,
        samesite=settings.admin_cookie_samesite,
    )


def _to_user_response(user: AdminUser) -> AuthUserResponse:
    return AuthUserResponse(id=user.id, email=user.email, role=user.role, is_active=user.is_active)


async def _create_session(
    db: AsyncSession,
    user: AdminUser,
    request: Request,
) -> tuple[str, AdminSession]:
    refresh_token = generate_refresh_token()
    token_hash = hash_refresh_token(refresh_token)
    expires_at = datetime.utcnow() + timedelta(days=settings.admin_refresh_ttl_days)

    session = AdminSession(
        user_id=user.id,
        refresh_token_hash=token_hash,
        expires_at=expires_at,
        user_agent=request.headers.get("user-agent"),
        ip_address=request.client.host if request.client else None,
    )
    db.add(session)
    await db.flush()
    return refresh_token, session


@router.post("/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(AdminUser).where(AdminUser.email == payload.email.lower()))
    user = result.scalar_one_or_none()

    if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    access_token = create_access_token(user_id=user.id, role=user.role)
    refresh_token, _session = await _create_session(db, user, request)
    await db.commit()

    _set_refresh_cookie(response, refresh_token)
    return LoginResponse(access_token=access_token, user=_to_user_response(user))


@router.post("/refresh", response_model=LoginResponse)
async def refresh(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    refresh_token = request.cookies.get(settings.admin_refresh_cookie_name)
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token missing")

    token_hash = hash_refresh_token(refresh_token)
    now = datetime.utcnow()

    result = await db.execute(
        select(AdminSession, AdminUser)
        .join(AdminUser, AdminUser.id == AdminSession.user_id)
        .where(
            AdminSession.refresh_token_hash == token_hash,
            AdminSession.revoked_at.is_(None),
            AdminSession.expires_at > now,
            AdminUser.is_active == True,
        )
    )
    row = result.first()
    if not row:
        _clear_refresh_cookie(response)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token")

    session, user = row
    session.revoked_at = now

    new_access = create_access_token(user_id=user.id, role=user.role)
    new_refresh_token, _new_session = await _create_session(db, user, request)
    await db.commit()

    _set_refresh_cookie(response, new_refresh_token)
    return LoginResponse(access_token=new_access, user=_to_user_response(user))


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    refresh_token = request.cookies.get(settings.admin_refresh_cookie_name)
    if refresh_token:
        token_hash = hash_refresh_token(refresh_token)
        result = await db.execute(
            select(AdminSession).where(
                AdminSession.refresh_token_hash == token_hash,
                AdminSession.revoked_at.is_(None),
            )
        )
        session = result.scalar_one_or_none()
        if session:
            session.revoked_at = datetime.utcnow()
            await db.commit()

    _clear_refresh_cookie(response)
    return {"message": "Logged out"}


@router.get("/me", response_model=MeResponse)
async def me(current_admin: AdminUser = Depends(get_current_admin)):
    return MeResponse(
        id=current_admin.id,
        email=current_admin.email,
        role=current_admin.role,
        is_active=current_admin.is_active,
    )
