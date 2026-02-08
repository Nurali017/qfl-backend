#!/usr/bin/env python3
"""Bootstrap initial admin user for qfl-admin."""

import asyncio
import sys
from pathlib import Path

from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models import AdminUser
from app.security import hash_password


async def bootstrap() -> None:
    settings = get_settings()
    email = settings.admin_bootstrap_email.strip().lower()
    password = settings.admin_bootstrap_password

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(AdminUser).where(AdminUser.email == email))
        existing = result.scalar_one_or_none()

        if existing:
            existing.password_hash = hash_password(password)
            existing.role = "superadmin"
            existing.is_active = True
            await db.commit()
            print(f"Updated existing admin user: {email}")
            return

        user = AdminUser(
            email=email,
            password_hash=hash_password(password),
            role="superadmin",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        print(f"Created superadmin user: {email}")


if __name__ == "__main__":
    asyncio.run(bootstrap())
