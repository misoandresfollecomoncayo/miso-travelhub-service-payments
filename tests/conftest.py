from itertools import count

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.db.models import Payment
from app.main import app
from app.repositories.payment_repository import get_payment_repository
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


class FakePaymentRepository:
    def __init__(self) -> None:
        self.saved: list[Payment] = []
        self._ids = count(start=1)

    async def create_from_webhook(
        self, payload: PaymentWebhookPayload
    ) -> Payment | None:
        for existing in self.saved:
            if existing.transaction_id == payload.transactionId:
                return None

        payment = Payment(
            id=next(self._ids),
            status=payload.status.value,
            message=payload.message,
            invoice_id=payload.invoiceId,
            amount=payload.amount,
            currency=payload.currency,
            card_holder=payload.cardHolder,
            masked_card=payload.maskedCard,
            transaction_id=payload.transactionId,
            processed_at=payload.processedAt,
        )
        self.saved.append(payment)
        return payment


@pytest.fixture
def fake_enqueuer() -> FakeCloudTasksEnqueuer:
    return FakeCloudTasksEnqueuer()


@pytest.fixture
def fake_payment_repository() -> FakePaymentRepository:
    return FakePaymentRepository()


@pytest_asyncio.fixture
async def client(
    fake_enqueuer: FakeCloudTasksEnqueuer,
    fake_payment_repository: FakePaymentRepository,
) -> AsyncClient:
    app.dependency_overrides[get_cloud_tasks_enqueuer] = lambda: fake_enqueuer
    app.dependency_overrides[get_payment_repository] = (
        lambda: fake_payment_repository
    )
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac
    finally:
        app.dependency_overrides.clear()
