from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/qfl_db"
    database_pool_size: int = 5
    database_max_overflow: int = 10
    database_pool_class: str = ""  # "" = QueuePool (default), "null" = NullPool

    # MinIO (S3-compatible object storage)
    minio_endpoint: str = "localhost:9000"
    minio_public_endpoint: str = "localhost:9000"  # For public URLs (browser access)
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin123"
    minio_bucket: str = "qfl-files"
    minio_secure: bool = False  # True for HTTPS

    # SOTA API
    sota_enabled: bool = True
    sota_api_email: str = ""
    sota_api_password: str = ""
    sota_api_base_url: str = "https://sota.id/api"
    lineup_live_refresh_ttl_seconds: int = 30
    lineup_live_refresh_timeout_seconds: int = 3

    # Redis (Celery broker)
    redis_url: str = "redis://localhost:6379/0"

    # Current season (default for API when season_id not specified)
    current_season_id: int = 200

    # Local season IDs to sync automatically (Celery tasks)
    sync_season_ids: list[int] = [61, 85, 71, 80, 84, 200, 203, 204]

    # CORS
    allowed_origins: str = "*"  # Comma-separated origins, e.g. "https://kffleague.kz"

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

    # Anthropic API Configuration
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-20250514"

    # Telegram notifications
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_notifications_enabled: bool = False

    # Weather (Open-Meteo, no API key needed)
    weather_enabled: bool = False

    # Serper (Google Search) API for ticket search
    serper_api_key: str = ""
    ticket_search_enabled: bool = False

    # FCMS (FIFA CMS) API
    fcms_enabled: bool = False
    fcms_base_url: str = "https://api-standard.fcms.ma.services"
    fcms_auth_url: str = "https://auth-standard.fcms.ma.services/auth/signin"
    fcms_email: str = ""
    fcms_password: str = ""
    fcms_customer_code: str = "kaz"
    fcms_competition_season_map: str = "3517:200,3585:204,3596:203,3597:203"

    # YouTube auto-link
    youtube_api_key: str = ""
    youtube_channel_id: str = ""
    youtube_auto_link_enabled: bool = False

    # Frontend revalidation
    revalidation_secret: str = ""
    frontend_internal_url: str = "http://qfl-frontend:3000"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
