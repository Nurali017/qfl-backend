from datetime import datetime, timedelta, timezone

import jwt

from app.config import get_settings

settings = get_settings()
ALGORITHM = "HS256"


class AccessTokenError(ValueError):
    pass


def create_access_token(*, user_id: int, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "role": role,
        "typ": "access",
        "iat": now,
        "exp": now + timedelta(minutes=settings.admin_access_ttl_minutes),
    }
    return jwt.encode(payload, settings.admin_jwt_secret, algorithm=ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        payload = jwt.decode(
            token,
            settings.admin_jwt_secret,
            algorithms=[ALGORITHM],
        )
    except jwt.ExpiredSignatureError as exc:
        raise AccessTokenError("Access token has expired") from exc
    except jwt.InvalidTokenError as exc:
        raise AccessTokenError("Invalid access token") from exc

    if payload.get("typ") != "access":
        raise AccessTokenError("Invalid token type")

    sub = payload.get("sub")
    role = payload.get("role")
    if not sub or not role:
        raise AccessTokenError("Token payload is incomplete")

    return payload
