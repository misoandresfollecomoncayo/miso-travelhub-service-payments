"""Unit tests for KafkaPaymentPublisher with a fake AIOKafkaProducer.

These tests never reach a real broker — `aiokafka.AIOKafkaProducer` is
monkeypatched with a fake that records calls and can be told to fail.
"""

import os
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.schemas.payment_webhook import PaymentWebhookPayload, PaymentWebhookStatus
from app.services.kafka_producer import (
    KafkaConfigError,
    KafkaPaymentPublisher,
    KafkaPublishError,
    get_payment_event_publisher,
)


# --- helpers ----------------------------------------------------------------


def _build_settings(**overrides) -> Settings:
    defaults: dict = {
        "kafka_enabled": True,
        "kafka_bootstrap_servers": "broker:9092",
        "kafka_topic": "payments-queue",
        "kafka_client_id": "test-client",
        "kafka_acks": "all",
        "kafka_request_timeout_ms": 10000,
        "kafka_security_protocol": "PLAINTEXT",
        "kafka_sasl_mechanism": "",
        "kafka_sasl_username": "",
        "kafka_sasl_password": "",
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _build_payload() -> PaymentWebhookPayload:
    return PaymentWebhookPayload(
        status=PaymentWebhookStatus.APPROVED,
        message="ok",
        invoiceId="INV-1",
        amount=Decimal("123.45"),
        currency="COP",
        cardHolder="JOHN",
        maskedCard="**** **** **** 1234",
        transactionId="TX-1",
        processedAt=datetime(2026, 5, 2, 18, 13, 14, tzinfo=timezone.utc),
    )


class FakeAIOKafkaProducer:
    """Drop-in replacement for aiokafka.AIOKafkaProducer."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[dict] = []
        self.fail_send = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic, value, key):
        if self.fail_send:
            from aiokafka.errors import KafkaError

            raise KafkaError("simulated broker failure")
        self.sent.append({"topic": topic, "value": value, "key": key})
        return SimpleNamespace(
            topic=topic, partition=0, offset=len(self.sent) - 1
        )


@pytest.fixture
def fake_kafka(monkeypatch):
    """Patches `aiokafka.AIOKafkaProducer` and exposes the created instance."""
    holder: dict = {}

    def factory(**kwargs):
        instance = FakeAIOKafkaProducer(**kwargs)
        holder["instance"] = instance
        return instance

    monkeypatch.setattr("aiokafka.AIOKafkaProducer", factory)
    return holder


@pytest.fixture(autouse=True)
def _isolate_kafka_env(monkeypatch):
    """Avoid leaking KAFKA_* env vars from the user's shell into tests."""
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)


# --- _validate paths --------------------------------------------------------


async def test_start_raises_when_bootstrap_missing() -> None:
    pub = KafkaPaymentPublisher(_build_settings(kafka_bootstrap_servers=""))
    with pytest.raises(KafkaConfigError, match="BOOTSTRAP"):
        await pub.start()


async def test_start_raises_when_topic_missing() -> None:
    pub = KafkaPaymentPublisher(_build_settings(kafka_topic=""))
    with pytest.raises(KafkaConfigError, match="TOPIC"):
        await pub.start()


async def test_start_raises_on_unknown_protocol() -> None:
    pub = KafkaPaymentPublisher(
        _build_settings(kafka_security_protocol="WEIRD")
    )
    with pytest.raises(KafkaConfigError, match="security_protocol"):
        await pub.start()


async def test_start_raises_when_sasl_creds_incomplete() -> None:
    pub = KafkaPaymentPublisher(
        _build_settings(
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="",
            kafka_sasl_username="",
            kafka_sasl_password="",
        )
    )
    with pytest.raises(KafkaConfigError, match="SASL"):
        await pub.start()


# --- start / stop -----------------------------------------------------------


async def test_start_no_op_when_disabled(fake_kafka) -> None:
    pub = KafkaPaymentPublisher(_build_settings(kafka_enabled=False))
    await pub.start()
    assert "instance" not in fake_kafka
    # Idempotent
    await pub.start()


async def test_start_creates_and_starts_producer(fake_kafka) -> None:
    pub = KafkaPaymentPublisher(_build_settings())
    await pub.start()

    instance = fake_kafka["instance"]
    assert instance.started is True
    assert instance.kwargs["bootstrap_servers"] == "broker:9092"
    assert instance.kwargs["client_id"] == "test-client"
    assert instance.kwargs["acks"] == "all"
    assert instance.kwargs["security_protocol"] == "PLAINTEXT"
    assert instance.kwargs["enable_idempotence"] is True

    # Second call is idempotent; doesn't create a new producer
    await pub.start()
    assert fake_kafka["instance"] is instance


async def test_start_passes_sasl_credentials(fake_kafka) -> None:
    pub = KafkaPaymentPublisher(
        _build_settings(
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_username="u",
            kafka_sasl_password="p",
        )
    )
    await pub.start()

    instance = fake_kafka["instance"]
    assert instance.kwargs["sasl_mechanism"] == "PLAIN"
    assert instance.kwargs["sasl_plain_username"] == "u"
    assert instance.kwargs["sasl_plain_password"] == "p"


async def test_stop_calls_producer_stop(fake_kafka) -> None:
    pub = KafkaPaymentPublisher(_build_settings())
    await pub.start()
    instance = fake_kafka["instance"]

    await pub.stop()
    assert instance.stopped is True


async def test_stop_is_noop_when_never_started() -> None:
    pub = KafkaPaymentPublisher(_build_settings(kafka_enabled=False))
    # Should not raise even though start() was never invoked
    await pub.stop()


# --- publish_payment_webhook -----------------------------------------------


async def test_publish_raises_config_error_when_disabled() -> None:
    pub = KafkaPaymentPublisher(_build_settings(kafka_enabled=False))
    with pytest.raises(KafkaConfigError, match="disabled"):
        await pub.publish_payment_webhook(_build_payload())


async def test_publish_raises_config_error_when_not_started() -> None:
    pub = KafkaPaymentPublisher(_build_settings())
    with pytest.raises(KafkaConfigError, match="not started"):
        await pub.publish_payment_webhook(_build_payload())


async def test_publish_success(fake_kafka) -> None:
    pub = KafkaPaymentPublisher(_build_settings())
    await pub.start()

    record_ref = await pub.publish_payment_webhook(_build_payload())

    assert record_ref == "payments-queue-0@0"

    instance = fake_kafka["instance"]
    assert len(instance.sent) == 1
    sent = instance.sent[0]
    assert sent["topic"] == "payments-queue"
    assert sent["key"] == b"TX-1"
    # The body is the JSON-serialized payload — must include the txid.
    assert b'"transactionId":"TX-1"' in sent["value"]


async def test_publish_wraps_broker_error(fake_kafka) -> None:
    pub = KafkaPaymentPublisher(_build_settings())
    await pub.start()
    fake_kafka["instance"].fail_send = True

    with pytest.raises(KafkaPublishError, match="simulated broker failure"):
        await pub.publish_payment_webhook(_build_payload())


# --- get_payment_event_publisher dependency --------------------------------


def test_get_payment_event_publisher_returns_state_attribute() -> None:
    sentinel = object()
    state = SimpleNamespace(payment_event_publisher=sentinel)
    app = SimpleNamespace(state=state)
    request = SimpleNamespace(app=app)

    assert get_payment_event_publisher(request) is sentinel


def test_get_payment_event_publisher_raises_when_missing() -> None:
    state = SimpleNamespace()  # no payment_event_publisher attribute
    app = SimpleNamespace(state=state)
    request = SimpleNamespace(app=app)

    with pytest.raises(KafkaConfigError, match="not initialized"):
        get_payment_event_publisher(request)
