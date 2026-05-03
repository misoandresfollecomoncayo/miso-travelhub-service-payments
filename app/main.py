import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _log_database_config() -> None:
    s = get_settings()
    if not s.database_url:
        logger.warning(
            "DATABASE_URL is not set. /payment-webhook/process will return 503."
        )
        return
    # Avoid logging credentials embedded in the URL.
    scheme, _, rest = s.database_url.partition("://")
    host_part = rest.split("@", 1)[-1] if "@" in rest else rest
    logger.info("Database configured: %s://***@%s", scheme or "?", host_part)


def _log_cloud_tasks_config() -> None:
    s = get_settings()
    if not s.gcp_tasks_enabled:
        logger.warning(
            "Cloud Tasks DISABLED (GCP_TASKS_ENABLED=false). "
            "Webhook payloads will NOT be enqueued."
        )
        return

    missing = [
        name
        for name, value in (
            ("GCP_PROJECT_ID", s.gcp_project_id),
            ("GCP_TASKS_QUEUE", s.gcp_tasks_queue),
            ("GCP_TASKS_TARGET_URL", s.gcp_tasks_target_url),
        )
        if not value
    ]
    if missing:
        logger.error(
            "Cloud Tasks ENABLED but misconfigured. Missing: %s",
            ", ".join(missing),
        )
        return

    logger.info(
        "Cloud Tasks ENABLED: project=%s location=%s queue=%s target=%s sa=%s",
        s.gcp_project_id,
        s.gcp_location,
        s.gcp_tasks_queue,
        s.gcp_tasks_target_url,
        s.gcp_tasks_service_account_email or "(none)",
    )


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        debug=settings.app_debug,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api/v1")

    _log_database_config()
    _log_cloud_tasks_config()

    return app


app = create_app()
