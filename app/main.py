import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.services.kafka_producer import KafkaPaymentPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _log_kafka_config() -> None:
    s = get_settings()
    if not s.kafka_enabled:
        logger.warning(
            "Kafka DISABLED (KAFKA_ENABLED=false). "
            "/payment-webhook will return 503."
        )
        return

    missing = [
        name
        for name, value in (
            ("KAFKA_BOOTSTRAP_SERVERS", s.kafka_bootstrap_servers),
            ("KAFKA_TOPIC", s.kafka_topic),
        )
        if not value
    ]
    if missing:
        logger.error(
            "Kafka ENABLED but misconfigured. Missing: %s", ", ".join(missing)
        )
        return

    logger.info(
        "Kafka ENABLED: bootstrap=%s topic=%s protocol=%s sasl=%s",
        s.kafka_bootstrap_servers,
        s.kafka_topic,
        s.kafka_security_protocol,
        s.kafka_sasl_mechanism or "(none)",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    publisher = KafkaPaymentPublisher(settings)
    try:
        await publisher.start()
    except Exception:
        # Don't fail container startup just because Kafka is unreachable —
        # the endpoint will return 503 until the broker comes back.
        logger.exception("Kafka producer failed to start; endpoint will 503")
    app.state.payment_event_publisher = publisher
    try:
        yield
    finally:
        await publisher.stop()


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        debug=settings.app_debug,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api/v1")

    _log_kafka_config()

    return app


app = create_app()
