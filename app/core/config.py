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

    # Kafka producer (broker corre en una VM de GCP).
    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = "payments-events"
    kafka_client_id: str = "miso-travelhub-service-payments"
    kafka_acks: str = "all"
    kafka_request_timeout_ms: int = 10000
    # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    kafka_security_protocol: str = "PLAINTEXT"
    # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 (solo si security_protocol incluye SASL)
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
