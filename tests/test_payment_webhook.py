from decimal import Decimal

from httpx import AsyncClient

from tests.conftest import FakePaymentRepository


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


async def test_payment_webhook_persists_and_returns_202(
    client: AsyncClient, fake_payment_repository: FakePaymentRepository
) -> None:
    response = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert response.status_code == 202
    assert response.json() == {
        "received": True,
        "transactionId": VALID_PAYLOAD["transactionId"],
        "status": "DECLINED",
    }

    assert len(fake_payment_repository.saved) == 1
    saved = fake_payment_repository.saved[0]
    assert saved.transaction_id == VALID_PAYLOAD["transactionId"]
    assert saved.invoice_id == VALID_PAYLOAD["invoiceId"]
    assert saved.status == VALID_PAYLOAD["status"]
    assert saved.amount == Decimal(str(VALID_PAYLOAD["amount"]))
    assert saved.currency == VALID_PAYLOAD["currency"]
    assert saved.card_holder == VALID_PAYLOAD["cardHolder"]
    assert saved.masked_card == VALID_PAYLOAD["maskedCard"]


async def test_payment_webhook_invalid_status_does_not_persist(
    client: AsyncClient, fake_payment_repository: FakePaymentRepository
) -> None:
    payload = {**VALID_PAYLOAD, "status": "UNKNOWN"}

    response = await client.post("/api/v1/payment-webhook", json=payload)

    assert response.status_code == 422
    assert fake_payment_repository.saved == []


async def test_payment_webhook_missing_field(client: AsyncClient) -> None:
    payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "transactionId"}

    response = await client.post("/api/v1/payment-webhook", json=payload)
    assert response.status_code == 422


async def test_payment_webhook_is_idempotent(
    client: AsyncClient, fake_payment_repository: FakePaymentRepository
) -> None:
    first = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)
    second = await client.post("/api/v1/payment-webhook", json=VALID_PAYLOAD)

    assert first.status_code == 202
    assert second.status_code == 202
    assert len(fake_payment_repository.saved) == 1
