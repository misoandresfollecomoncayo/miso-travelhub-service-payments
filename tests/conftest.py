import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.schemas.payment_webhook import PaymentWebhookPayload
from app.services.kafka_producer import (
    KafkaConfigError,
    KafkaPublishError,
    get_payment_event_publisher,
)


class FakePaymentEventPublisher:
    def __init__(self) -> None:
        self.published: list[PaymentWebhookPayload] = []
        self.raise_config_error: bool = False
        self.raise_publish_error: bool = False

    async def publish_payment_webhook(
        self, payload: PaymentWebhookPayload
    ) -> str | None:
        if self.raise_config_error:
            raise KafkaConfigError("kafka disabled (test)")
        if self.raise_publish_error:
            raise KafkaPublishError("broker unreachable (test)")
        self.published.append(payload)
        return f"payments-events-0@{len(self.published) - 1}"


@pytest.fixture
def fake_publisher() -> FakePaymentEventPublisher:
    return FakePaymentEventPublisher()


@pytest_asyncio.fixture
async def client(fake_publisher: FakePaymentEventPublisher) -> AsyncClient:
    app.dependency_overrides[get_payment_event_publisher] = lambda: fake_publisher
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac
    finally:
        app.dependency_overrides.clear()
