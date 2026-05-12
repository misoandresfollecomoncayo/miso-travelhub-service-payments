import logging
from typing import Any, Protocol

from fastapi import Request

from app.core.config import Settings
from app.schemas.payment_webhook import PaymentWebhookPayload

logger = logging.getLogger(__name__)


class KafkaConfigError(RuntimeError):
    pass


class KafkaPublishError(RuntimeError):
    pass


class PaymentEventPublisher(Protocol):
    async def publish_payment_webhook(
        self, payload: PaymentWebhookPayload
    ) -> str | None: ...


class KafkaPaymentPublisher:
    """Async Kafka producer for payment-webhook events.

    Uses aiokafka. The producer is long-lived: started at app startup,
    stopped at shutdown. One instance is reused across all requests.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: Any | None = None
        self._started = False

    def _validate(self) -> None:
        s = self._settings
        if not s.kafka_bootstrap_servers:
            raise KafkaConfigError(
                "Kafka misconfigured: KAFKA_BOOTSTRAP_SERVERS is empty"
            )
        if not s.kafka_topic:
            raise KafkaConfigError("Kafka misconfigured: KAFKA_TOPIC is empty")
        protocol = s.kafka_security_protocol.upper()
        if protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise KafkaConfigError(
                f"Kafka misconfigured: unsupported security_protocol={protocol}"
            )
        if protocol.startswith("SASL") and (
            not s.kafka_sasl_mechanism
            or not s.kafka_sasl_username
            or not s.kafka_sasl_password
        ):
            raise KafkaConfigError(
                "Kafka misconfigured: SASL requires mechanism + username + password"
            )

    def _build_producer_kwargs(self) -> dict[str, Any]:
        s = self._settings
        kwargs: dict[str, Any] = {
            "bootstrap_servers": s.kafka_bootstrap_servers,
            "client_id": s.kafka_client_id,
            "acks": s.kafka_acks,
            "request_timeout_ms": s.kafka_request_timeout_ms,
            "enable_idempotence": True,
            "security_protocol": s.kafka_security_protocol.upper(),
        }
        if kwargs["security_protocol"].startswith("SASL"):
            kwargs["sasl_mechanism"] = s.kafka_sasl_mechanism
            kwargs["sasl_plain_username"] = s.kafka_sasl_username
            kwargs["sasl_plain_password"] = s.kafka_sasl_password
        return kwargs

    async def start(self) -> None:
        if self._started:
            return
        s = self._settings
        if not s.kafka_enabled:
            logger.warning(
                "Kafka DISABLED (KAFKA_ENABLED=false). "
                "/payment-webhook will return 503."
            )
            return

        self._validate()

        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(**self._build_producer_kwargs())
        await self._producer.start()
        self._started = True
        logger.info(
            "Kafka producer started bootstrap=%s topic=%s protocol=%s",
            s.kafka_bootstrap_servers,
            s.kafka_topic,
            s.kafka_security_protocol,
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            logger.info("Kafka producer stopped")
        self._producer = None
        self._started = False

    async def publish_payment_webhook(
        self, payload: PaymentWebhookPayload
    ) -> str | None:
        s = self._settings
        if not s.kafka_enabled:
            raise KafkaConfigError("Kafka is disabled (KAFKA_ENABLED=false)")
        if self._producer is None:
            raise KafkaConfigError("Kafka producer was not started")

        from aiokafka.errors import KafkaError

        body = payload.model_dump_json().encode("utf-8")
        # Partition by transactionId so retries land on the same partition
        # and downstream consumers can use it for ordering / dedup.
        key = payload.transactionId.encode("utf-8")

        try:
            metadata = await self._producer.send_and_wait(
                s.kafka_topic, value=body, key=key
            )
        except KafkaError as exc:
            logger.exception(
                "Kafka publish failed tx=%s topic=%s",
                payload.transactionId,
                s.kafka_topic,
            )
            raise KafkaPublishError(str(exc)) from exc

        record_ref = f"{metadata.topic}-{metadata.partition}@{metadata.offset}"
        logger.info(
            "Kafka published tx=%s status=%s record=%s",
            payload.transactionId,
            payload.status.value,
            record_ref,
        )
        return record_ref


def get_payment_event_publisher(request: Request) -> PaymentEventPublisher:
    """FastAPI dependency that returns the app-scoped Kafka publisher."""
    publisher = getattr(request.app.state, "payment_event_publisher", None)
    if publisher is None:
        raise KafkaConfigError(
            "PaymentEventPublisher not initialized on app.state"
        )
    return publisher
