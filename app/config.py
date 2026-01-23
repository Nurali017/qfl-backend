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

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Current season
    current_season_id: int = 61

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
