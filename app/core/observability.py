"""New Relic agent bootstrap.

Call :func:`initialize_newrelic` *before* importing any framework or
library you want auto-instrumented (FastAPI, httpx, aiokafka, asyncpg,
etc.). The agent installs import hooks at init time, so anything that
gets imported *after* this call is candidate for auto-instrumentation.

The agent is configured entirely via ``NEW_RELIC_*`` environment
variables — no ``newrelic.ini`` file needed. The minimum required is
``NEW_RELIC_LICENSE_KEY``; without it, this module is a no-op (useful
for local development and tests).

Recognized env vars (most useful subset):

================================  ====================================
NEW_RELIC_LICENSE_KEY             (required to enable the agent)
NEW_RELIC_APP_NAME                Display name in the New Relic UI
NEW_RELIC_ENVIRONMENT             ``development`` / ``staging`` / ``production``
NEW_RELIC_LOG_LEVEL               ``info`` / ``debug`` / ``warning``
NEW_RELIC_LOG                     ``stdout`` / ``stderr`` / path
NEW_RELIC_DISTRIBUTED_TRACING_ENABLED  ``true`` (default) / ``false``
NEW_RELIC_HIGH_SECURITY           ``false`` (default) / ``true``
================================  ====================================
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_initialized: bool = False


def is_initialized() -> bool:
    return _initialized


def initialize_newrelic() -> bool:
    """Initialize the New Relic agent if a license key is present.

    Returns True if the agent was initialized (or had been already),
    False otherwise. Safe to call multiple times.
    """
    global _initialized
    if _initialized:
        return True

    license_key = os.getenv("NEW_RELIC_LICENSE_KEY", "").strip()
    if not license_key:
        logger.info(
            "NEW_RELIC_LICENSE_KEY not set; New Relic agent stays disabled"
        )
        return False

    try:
        import newrelic.agent
    except ImportError:
        logger.warning(
            "newrelic package not installed; cannot initialize agent"
        )
        return False

    # Config is read from NEW_RELIC_* env vars when no config_file is given.
    newrelic.agent.initialize(
        environment=os.getenv("NEW_RELIC_ENVIRONMENT") or None,
    )
    _initialized = True
    logger.info(
        "New Relic agent initialized: app_name=%s environment=%s",
        os.getenv("NEW_RELIC_APP_NAME", "(default)"),
        os.getenv("NEW_RELIC_ENVIRONMENT", "(none)"),
    )
    return True
