from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "miso-travelhub-service-payments"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_debug: bool = False

    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    gcp_tasks_enabled: bool = False
    gcp_project_id: str = ""
    gcp_location: str = "us-central1"
    gcp_tasks_queue: str = "payments-queue"
    gcp_tasks_target_url: str = ""
    gcp_tasks_service_account_email: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
