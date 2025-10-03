"""
Configuration Utility - Environment Variables Management

Centralized configuration loading from .env files using pydantic-settings.
Type-safe access to all environment variables with validation.

Usage:
    from utils.config import settings

    api_base = settings.USERS_API_BASE
    redis_url = settings.REDIS_URL

TODO:
- Implement Settings class with Pydantic BaseSettings
- Add validation for required variables
- Support multiple environments (dev, staging, prod)
- Add configuration documentation
"""

from functools import lru_cache
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    TODO: Complete all settings fields based on .env.example
    """

    # API Configuration
    USERS_API_BASE: str = Field(default="https://dummyjson.com/users")
    API_TIMEOUT: int = Field(default=30)

    # Scheduler Configuration
    EXTRACT_SCHEDULE_CRON: str = Field(default="0 3 * * *")
    EXTRACT_LIMIT: int = Field(default=100)
    EXTRACT_MAX_RETRIES: int = Field(default=3)

    # File System Paths
    DATA_DIR: str = Field(default="/app/data")
    STATE_DIR: str = Field(default="/app/data/state")
    RAW_DIR: str = Field(default="/app/data/raw_users")
    PROCESSED_DIR: str = Field(default="/app/data/processed_users")
    DLQ_DIR: str = Field(default="/app/data/dlq")
    DEPARTMENTS_CSV: str = Field(default="/data/departments.csv")

    # Redis Configuration
    REDIS_URL: str = Field(default="redis://redis:6379/0")
    REDIS_MAX_CONNECTIONS: int = Field(default=10)
    REDIS_CHANNEL_RAW: str = Field(default="files.raw_users")
    REDIS_CHANNEL_PROCESSED: str = Field(default="files.processed_users")
    REDIS_CHANNEL_DLQ: str = Field(default="files.dlq")

    # Database Configuration
    SQLITE_URL: str = Field(default="sqlite:////app/data/db/app.db")
    SQLITE_PATH: str = Field(default="/data/db/app.db")

    # SFTP Configuration
    SFTP_HOST: str = Field(default="")
    SFTP_PORT: int = Field(default=22)
    SFTP_USERNAME: str = Field(default="")
    SFTP_KEY_PATH: str = Field(default="/run/secrets/id_rsa")
    SFTP_KEY_PASSPHRASE: str | None = Field(default=None)
    SFTP_REMOTE_BASE: str = Field(default="/upload")
    SFTP_TIMEOUT: int = Field(default=15)

    # Backend API Configuration
    API_PORT: int = Field(default=8000)
    API_HOST: str = Field(default="0.0.0.0")
    API_WORKERS: int = Field(default=4)

    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO")
    LOG_FORMAT: str = Field(default="json")

    # Application Metadata
    ENVIRONMENT: str = Field(default="production")
    APP_NAME: str = Field(default="tiis-backend")
    APP_VERSION: str = Field(default="0.1.0")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance.

    Returns:
        Singleton Settings instance
    """
    return Settings()


# Global settings instance
settings = get_settings()
