"""Tests for app/main.py — _log_kafka_config and the lifespan handler."""

import logging
import os

import pytest
from fastapi import FastAPI

from app.core.config import get_settings
from app.main import _log_kafka_config, create_app, lifespan


@pytest.fixture(autouse=True)
def _isolate_kafka_env(monkeypatch):
    """Strip KAFKA_* env vars and reset the cached Settings between tests."""
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# --- _log_kafka_config -----------------------------------------------------


def test_log_kafka_config_warns_when_disabled(monkeypatch, caplog):
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    get_settings.cache_clear()

    with caplog.at_level(logging.WARNING, logger="app.main"):
        _log_kafka_config()

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Kafka DISABLED" in m for m in messages)


def test_log_kafka_config_errors_when_missing_fields(monkeypatch, caplog):
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")
    monkeypatch.setenv("KAFKA_TOPIC", "")
    get_settings.cache_clear()

    with caplog.at_level(logging.ERROR, logger="app.main"):
        _log_kafka_config()

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("misconfigured" in m for m in messages)
    assert any("KAFKA_BOOTSTRAP_SERVERS" in m for m in messages)
    assert any("KAFKA_TOPIC" in m for m in messages)


def test_log_kafka_config_logs_info_when_ok(monkeypatch, caplog):
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "host:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "payments-queue")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    get_settings.cache_clear()

    with caplog.at_level(logging.INFO, logger="app.main"):
        _log_kafka_config()

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Kafka ENABLED" in m for m in messages)
    assert any("payments-queue" in m for m in messages)


# --- lifespan --------------------------------------------------------------


async def test_lifespan_sets_publisher_on_state_when_disabled(monkeypatch):
    """With Kafka disabled, lifespan still attaches a publisher and yields."""
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    get_settings.cache_clear()

    app = FastAPI()
    async with lifespan(app):
        assert hasattr(app.state, "payment_event_publisher")
        publisher = app.state.payment_event_publisher
        assert publisher is not None


async def test_lifespan_swallows_start_failure(monkeypatch, caplog):
    """If publisher.start() raises (e.g. misconfig), lifespan logs and yields."""
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")  # forces KafkaConfigError
    monkeypatch.setenv("KAFKA_TOPIC", "payments-queue")
    get_settings.cache_clear()

    app = FastAPI()
    with caplog.at_level(logging.ERROR, logger="app.main"):
        async with lifespan(app):
            # Container should still be up; endpoint will 503 until kafka recovers.
            assert hasattr(app.state, "payment_event_publisher")

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("failed to start" in m for m in messages)


# --- create_app smoke test -------------------------------------------------


def test_create_app_returns_fastapi_instance(monkeypatch):
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    get_settings.cache_clear()

    app = create_app()
    assert isinstance(app, FastAPI)
    # The /api/v1 router should be mounted with health and payment-webhook.
    paths = {route.path for route in app.routes}
    assert "/api/v1/health" in paths
    assert "/api/v1/payment-webhook" in paths
