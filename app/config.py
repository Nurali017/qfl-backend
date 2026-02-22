from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/qfl_db"

    # MinIO (S3-compatible object storage)
    minio_endpoint: str = "localhost:9000"
    minio_public_endpoint: str = "localhost:9000"  # For public URLs (browser access)
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin123"
    minio_bucket: str = "qfl-files"
    minio_secure: bool = False  # True for HTTPS

    # SOTA API
    sota_api_email: str
    sota_api_password: str
    sota_api_base_url: str = "https://sota.id/api"
    lineup_live_refresh_ttl_seconds: int = 30
    lineup_live_refresh_timeout_seconds: int = 3

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    live_events_channel: str = "qfl:live-events"

    # Current season (default for API when season_id not specified)
    current_season_id: int = 61

    # Seasons to sync automatically (Celery tasks)
    # IDs from SOTA API database:
    # 61=Premier League, 85=First League, 71=Cup,
    # 80=Second League SW, 81=Second League NE, 157=Second League Final,
    # 84=Women's League
    sync_season_ids: list[int] = [61, 85, 71, 80, 81, 84, 157]

    # CORS
    allowed_origins: str = "*"  # Comma-separated origins, e.g. "https://kff.1sportkz.com"

    # Admin auth (JWT + refresh cookie)
    admin_jwt_secret: str = "change-me-admin-jwt-secret"
    admin_access_ttl_minutes: int = 30
    admin_refresh_ttl_days: int = 14
    admin_refresh_cookie_name: str = "admin_refresh_token"
    admin_cookie_secure: bool = False
    admin_cookie_samesite: str = "lax"
    admin_cookie_domain: str | None = None
    admin_bootstrap_email: str = "admin@qfl.local"
    admin_bootstrap_password: str = "ChangeMe123!"

    # OpenAI API Configuration
    openai_api_key: str = ""  # Set to empty string by default, add your key to .env
    openai_model: str = "gpt-4o-mini"
    openai_max_retries: int = 3
    openai_timeout: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()
