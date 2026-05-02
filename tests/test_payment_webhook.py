import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_payment_webhook_declined(client: AsyncClient) -> None:
    payload = {
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

    response = await client.post("/api/v1/payment-webhook", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["received"] is True
    assert body["transactionId"] == payload["transactionId"]
    assert body["status"] == "DECLINED"


@pytest.mark.asyncio
async def test_payment_webhook_invalid_status(client: AsyncClient) -> None:
    payload = {
        "status": "UNKNOWN",
        "message": "x",
        "invoiceId": "1",
        "amount": 10,
        "currency": "COP",
        "cardHolder": "x",
        "maskedCard": "x",
        "transactionId": "tx",
        "processedAt": "2026-05-02T18:13:14.424Z",
    }

    response = await client.post("/api/v1/payment-webhook", json=payload)
    assert response.status_code == 422
