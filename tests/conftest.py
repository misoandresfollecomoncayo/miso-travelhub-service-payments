import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.schemas.payment_webhook import PaymentWebhookPayload
from app.services.cloud_tasks import get_cloud_tasks_enqueuer


class FakeCloudTasksEnqueuer:
    def __init__(self) -> None:
        self.calls: list[PaymentWebhookPayload] = []
        self.raise_config_error: bool = False

    async def enqueue_payment_webhook(
        self, payload: PaymentWebhookPayload
    ) -> str | None:
        if self.raise_config_error:
            from app.services.cloud_tasks import CloudTasksConfigError

            raise CloudTasksConfigError("missing config (test)")
        self.calls.append(payload)
        return f"projects/test/locations/us-central1/queues/q/tasks/payment-{payload.transactionId}"


@pytest.fixture
def fake_enqueuer() -> FakeCloudTasksEnqueuer:
    return FakeCloudTasksEnqueuer()


@pytest_asyncio.fixture
async def client(fake_enqueuer: FakeCloudTasksEnqueuer) -> AsyncClient:
    app.dependency_overrides[get_cloud_tasks_enqueuer] = lambda: fake_enqueuer
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac
    finally:
        app.dependency_overrides.clear()
