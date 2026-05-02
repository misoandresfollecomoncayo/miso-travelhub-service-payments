from decimal import Decimal

from httpx import AsyncClient

from tests.conftest import FakeCloudTasksEnqueuer


VALID_PAYLOAD = {
    "status": "DECLINED",
    "message": "Tarjeta no autorizada para esta operación",
    "invoiceId": "123",
    "amount": 123,
    "currency": "COP",
    "cardHolder": "QUANTUMSOFT INGENIERÍA SAS",
    "maskedCard": "**** **** **** 6880",
    "transactionId": "TX-1777745594424-314",
    "processedAt": "2026-05-02T18:13:14.424Z",
}


async def test_payment_webhook_enqueues_and_returns_202(
    client: AsyncClient, fake_enqueuer: FakeCloudTasksEnqueuer
) -> None:
    response = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert response.status_code == 202
    assert response.json() == {
        "received": True,
        "transactionId": VALID_PAYLOAD["transactionId"],
        "status": "DECLINED",
    }

    assert len(fake_enqueuer.calls) == 1
    enqueued = fake_enqueuer.calls[0]
    assert enqueued.transactionId == VALID_PAYLOAD["transactionId"]
    assert enqueued.invoiceId == VALID_PAYLOAD["invoiceId"]
    assert enqueued.amount == Decimal("123")
    assert enqueued.currency == "COP"


async def test_payment_webhook_invalid_status_does_not_enqueue(
    client: AsyncClient, fake_enqueuer: FakeCloudTasksEnqueuer
) -> None:
    payload = {**VALID_PAYLOAD, "status": "UNKNOWN"}

    response = await client.post("/api/v1/payment-webhook", json=payload)

    assert response.status_code == 422
    assert fake_enqueuer.calls == []


async def test_payment_webhook_missing_field(client: AsyncClient) -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "transactionId"}

    response = await client.post("/api/v1/payment-webhook", json=payload)
    assert response.status_code == 422


async def test_payment_webhook_returns_503_on_config_error(
    client: AsyncClient, fake_enqueuer: FakeCloudTasksEnqueuer
) -> None:
    fake_enqueuer.raise_config_error = True

    response = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert response.status_code == 503
    assert response.json() == {"detail": "payment queue unavailable"}


async def test_payment_webhook_process_worker(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payment-webhook/process", json=VALID_PAYLOAD
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "processed",
        "transactionId": VALID_PAYLOAD["transactionId"],
    }
