from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/qfl_db"
    database_pool_size: int = 5
    database_max_overflow: int = 10
    database_pool_class: str = ""  # "" = QueuePool (default), "null" = NullPool
    web_statement_timeout_ms: int = 30000

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
    sync_season_ids: list[int] = [61, 85, 71, 80, 84, 200, 202, 203, 204, 205]

    # Seasons eligible for v2 extended stats (xG, passes, duels, dribbles).
    # Excludes Вторая Лига — SOTA doesn't collect detailed analytics for it.
    extended_stats_season_ids: list[int] = [61, 85, 71, 80, 200, 204]

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

    # Telegram notifications (admin operations)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_notifications_enabled: bool = False

    # Public Telegram posts (t.me/kffleague channel)
    telegram_public_posts_enabled: bool = False
    telegram_public_chat_id: str = ""  # Falls back to telegram_chat_id when empty
    telegram_tour_announce_enabled: bool = False  # Daily 21:00 beat for Scenario 0
    telegram_match_start_enabled: bool = False     # Scenario 1 (match start) — opt-in

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
    fcms_competition_season_map: str = "3517:200,3585:204,3596:203,3597:203,3598:202"

    # YouTube auto-link
    youtube_api_key: str = ""
    youtube_channel_id: str = ""
    youtube_reserve_channel_ids: str = ""  # Comma-separated reserve channel IDs
    youtube_auto_link_enabled: bool = False
    # YouTube view_count sync — limit to specific seasons (comma-separated IDs)
    # Default: 2026 seasons only (PL, Super Cup, Cup, 2L, 1L, Women)
    youtube_stats_season_ids: str = "200,201,202,203,204,205"

    # Frontend revalidation
    revalidation_secret: str = ""
    frontend_internal_url: str = "http://qfl-frontend:3000"

    # Google Drive — goal video ingest during live matches
    google_drive_enabled: bool = False
    google_service_account_file: str = "/app/secrets/google-sa.json"
    google_drive_goals_folder_id: str = ""
    goal_video_ai_fallback_enabled: bool = True
    goal_video_ai_model: str = "claude-haiku-4-5-20251001"
    goal_video_sync_interval_minutes: int = 5
    # Re-encode incoming clips with ffmpeg (libx264 CRF 20) before uploading to
    # MinIO — roughly halves the file size without perceptible quality loss.
    goal_video_transcode_enabled: bool = True
    goal_video_transcode_crf: str = "20"
    goal_video_transcode_preset: str = "medium"
    # "0" = let libx264 pick (usually all cores). On a dedicated media host we
    # want all cores; on a shared box you may want to cap it.
    goal_video_transcode_threads: str = "0"

    # SOTA sync guardrails / diagnostics
    sota_dead_season_min_404: int = 30
    sota_dead_season_404_ratio: float = 0.8
    sota_dead_season_ttl_seconds: int = 3600
    debug_sync_timings: bool = False


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
