from datetime import datetime, timedelta, timezone
import base64
import hashlib
import hmac
import json

from app.config import get_settings

settings = get_settings()
ALGORITHM = "HS256"


class AccessTokenError(ValueError):
    pass


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("utf-8")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("utf-8"))


def _sign(data: str) -> str:
    signature = hmac.new(
        settings.admin_jwt_secret.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return _b64url_encode(signature)


def create_access_token(*, user_id: int, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "role": role,
        "typ": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.admin_access_ttl_minutes)).timestamp()),
    }
    header = {"alg": ALGORITHM, "typ": "JWT"}
    encoded_header = _b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    encoded_payload = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_payload}"
    signature = _sign(signing_input)
    return f"{signing_input}.{signature}"


def decode_access_token(token: str) -> dict:
    try:
        encoded_header, encoded_payload, signature = token.split(".")
    except ValueError as exc:
        raise AccessTokenError("Invalid access token") from exc

    signing_input = f"{encoded_header}.{encoded_payload}"
    expected_signature = _sign(signing_input)
    if not hmac.compare_digest(signature, expected_signature):
        raise AccessTokenError("Invalid access token signature")

    try:
        header = json.loads(_b64url_decode(encoded_header))
        payload = json.loads(_b64url_decode(encoded_payload))
    except (ValueError, json.JSONDecodeError) as exc:
        raise AccessTokenError("Malformed token payload") from exc

    if header.get("alg") != ALGORITHM:
        raise AccessTokenError("Unexpected token algorithm")

    exp = payload.get("exp")
    if not isinstance(exp, int):
        raise AccessTokenError("Token exp claim is missing")
    if int(datetime.now(timezone.utc).timestamp()) >= exp:
        raise AccessTokenError("Access token has expired")

    if payload.get("typ") != "access":
        raise AccessTokenError("Invalid token type")

    sub = payload.get("sub")
    role = payload.get("role")
    if not sub or not role:
        raise AccessTokenError("Token payload is incomplete")

    return payload
