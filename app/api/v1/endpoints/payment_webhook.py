import logging

from fastapi import APIRouter, status

from app.schemas.payment_webhook import PaymentWebhookAck, PaymentWebhookPayload

logger = logging.getLogger(__name__)

router = APIRouter(tags=["payment-webhook"])


@router.post(
    "/payment-webhook",
    response_model=PaymentWebhookAck,
    status_code=status.HTTP_200_OK,
)
async def payment_webhook(payload: PaymentWebhookPayload) -> PaymentWebhookAck:
    logger.info(
        "payment-webhook received: invoice=%s tx=%s status=%s amount=%s %s",
        payload.invoiceId,
        payload.transactionId,
        payload.status.value,
        payload.amount,
        payload.currency,
    )
    return PaymentWebhookAck(
        received=True,
        transactionId=payload.transactionId,
        status=payload.status,
    )
