"""Unit tests for the New Relic bootstrap.

The agent is never actually started against New Relic during tests —
we patch ``newrelic.agent.initialize`` so no network calls happen.
"""

from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from app.core import observability


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Each test starts with a clean ``_initialized`` flag."""
    observability._initialized = False
    yield
    observability._initialized = False


def test_returns_false_when_license_key_missing(monkeypatch) -> None:
    monkeypatch.delenv("NEW_RELIC_LICENSE_KEY", raising=False)

    assert observability.initialize_newrelic() is False
    assert observability.is_initialized() is False


def test_returns_false_when_license_key_is_blank(monkeypatch) -> None:
    monkeypatch.setenv("NEW_RELIC_LICENSE_KEY", "   ")

    assert observability.initialize_newrelic() is False
    assert observability.is_initialized() is False


def test_initializes_agent_when_license_key_present(monkeypatch) -> None:
    monkeypatch.setenv("NEW_RELIC_LICENSE_KEY", "fake-key-1234")
    monkeypatch.setenv("NEW_RELIC_APP_NAME", "test-app")
    monkeypatch.setenv("NEW_RELIC_ENVIRONMENT", "staging")

    with patch("newrelic.agent.initialize") as mock_init:
        result = observability.initialize_newrelic()

    assert result is True
    assert observability.is_initialized() is True
    mock_init.assert_called_once_with(environment="staging")


def test_passes_environment_none_when_unset(monkeypatch) -> None:
    monkeypatch.setenv("NEW_RELIC_LICENSE_KEY", "fake-key")
    monkeypatch.delenv("NEW_RELIC_ENVIRONMENT", raising=False)

    with patch("newrelic.agent.initialize") as mock_init:
        observability.initialize_newrelic()

    mock_init.assert_called_once_with(environment=None)


def test_double_initialization_is_idempotent(monkeypatch) -> None:
    monkeypatch.setenv("NEW_RELIC_LICENSE_KEY", "fake-key")

    with patch("newrelic.agent.initialize") as mock_init:
        observability.initialize_newrelic()
        result = observability.initialize_newrelic()

    assert result is True
    assert mock_init.call_count == 1  # not called again on second invocation


def test_returns_false_when_newrelic_package_unimportable(monkeypatch) -> None:
    monkeypatch.setenv("NEW_RELIC_LICENSE_KEY", "fake-key")
    # Force `import newrelic.agent` to raise ImportError.
    monkeypatch.setitem(sys.modules, "newrelic.agent", None)

    result = observability.initialize_newrelic()

    assert result is False
    assert observability.is_initialized() is False
