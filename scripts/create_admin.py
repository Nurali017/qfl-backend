"""
Create an admin user.
Run from project root:  python -m scripts.create_admin --email e@x.com --password secret
"""

import argparse
import asyncio

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.admin_user import AdminUser
from app.security.passwords import hash_password

VALID_ROLES = ("superadmin", "editor", "operator")


async def create_user(email: str, password: str, role: str) -> None:
    async with AsyncSessionLocal() as session:
        exists = await session.scalar(
            select(AdminUser.id).where(AdminUser.email == email)
        )
        if exists:
            print(f"User {email} already exists, skipping.")
            return

        user = AdminUser(
            email=email.lower().strip(),
            password_hash=hash_password(password),
            role=role,
            is_active=True,
        )
        session.add(user)
        await session.commit()
        print(f"Created {role} user: {email}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create admin user")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--role", default="editor", choices=VALID_ROLES)
    args = parser.parse_args()
    asyncio.run(create_user(args.email, args.password, args.role))


if __name__ == "__main__":
    main()
