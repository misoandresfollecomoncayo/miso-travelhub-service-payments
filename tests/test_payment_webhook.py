from decimal import Decimal

from httpx import AsyncClient

from tests.conftest import FakePaymentEventPublisher


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


async def test_payment_webhook_publishes_and_returns_202(
    client: AsyncClient, fake_publisher: FakePaymentEventPublisher
) -> None:
    response = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert response.status_code == 202
    assert response.json() == {
        "received": True,
        "transactionId": VALID_PAYLOAD["transactionId"],
        "status": "DECLINED",
    }

    assert len(fake_publisher.published) == 1
    published = fake_publisher.published[0]
    assert published.transactionId == VALID_PAYLOAD["transactionId"]
    assert published.invoiceId == VALID_PAYLOAD["invoiceId"]
    assert published.amount == Decimal(str(VALID_PAYLOAD["amount"]))
    assert published.currency == VALID_PAYLOAD["currency"]
    assert published.cardHolder == VALID_PAYLOAD["cardHolder"]
    assert published.maskedCard == VALID_PAYLOAD["maskedCard"]


async def test_payment_webhook_invalid_status_does_not_publish(
    client: AsyncClient, fake_publisher: FakePaymentEventPublisher
) -> None:
    payload = {**VALID_PAYLOAD, "status": "UNKNOWN"}

    response = await client.post("/api/v1/payment-webhook", json=payload)

    assert response.status_code == 422
    assert fake_publisher.published == []


async def test_payment_webhook_missing_field(client: AsyncClient) -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "transactionId"}

    response = await client.post("/api/v1/payment-webhook", json=payload)
    assert response.status_code == 422


async def test_payment_webhook_returns_503_on_config_error(
    client: AsyncClient, fake_publisher: FakePaymentEventPublisher
) -> None:
    fake_publisher.raise_config_error = True

    response = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert response.status_code == 503
    assert response.json() == {"detail": "payment event bus unavailable"}
    assert fake_publisher.published == []


async def test_payment_webhook_returns_502_on_publish_error(
    client: AsyncClient, fake_publisher: FakePaymentEventPublisher
) -> None:
    fake_publisher.raise_publish_error = True

    response = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert response.status_code == 502
    assert response.json() == {"detail": "payment event publish failed"}
    assert fake_publisher.published == []
