from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.admin.deps import require_roles
from app.api.deps import get_db
from app.models import AdminUser
from app.schemas.admin.users import (
    AdminPasswordResetRequest,
    AdminUserCreateRequest,
    AdminUserResponse,
    AdminUserUpdateRequest,
)
from app.security import hash_password

router = APIRouter(prefix="/users", tags=["admin-users"])


def _normalize_email(email: str) -> str:
    return email.strip().lower()


@router.get("", response_model=list[AdminUserResponse])
async def list_users(
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    result = await db.execute(select(AdminUser).order_by(AdminUser.id.asc()))
    return result.scalars().all()


@router.post("", response_model=AdminUserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: AdminUserCreateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    email = _normalize_email(payload.email)
    existing = await db.execute(select(AdminUser).where(AdminUser.email == email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="User with this email already exists")

    user = AdminUser(
        email=email,
        password_hash=hash_password(payload.password),
        role=payload.role,
        is_active=payload.is_active,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


@router.patch("/{user_id}", response_model=AdminUserResponse)
async def update_user(
    user_id: int,
    payload: AdminUserUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    result = await db.execute(select(AdminUser).where(AdminUser.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.email is not None:
        email = _normalize_email(payload.email)
        existing = await db.execute(select(AdminUser).where(AdminUser.email == email, AdminUser.id != user_id))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="User with this email already exists")
        user.email = email

    if payload.role is not None:
        user.role = payload.role
    if payload.is_active is not None:
        user.is_active = payload.is_active

    await db.commit()
    await db.refresh(user)
    return user


@router.post("/{user_id}/reset-password")
async def reset_password(
    user_id: int,
    payload: AdminPasswordResetRequest,
    db: AsyncSession = Depends(get_db),
    _admin: AdminUser = Depends(require_roles("superadmin")),
):
    result = await db.execute(select(AdminUser).where(AdminUser.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.password_hash = hash_password(payload.new_password)
    await db.commit()
    return {"message": "Password updated"}
