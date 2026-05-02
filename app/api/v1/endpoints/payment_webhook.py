import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.schemas.payment_webhook import PaymentWebhookAck, PaymentWebhookPayload
from app.services.cloud_tasks import (
    CloudTasksConfigError,
    CloudTasksEnqueuer,
    get_cloud_tasks_enqueuer,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["payment-webhook"])


@router.post(
    "/payment-webhook",
    response_model=PaymentWebhookAck,
    status_code=status.HTTP_202_ACCEPTED,
)
async def payment_webhook(
    payload: PaymentWebhookPayload,
    enqueuer: CloudTasksEnqueuer = Depends(get_cloud_tasks_enqueuer),
) -> PaymentWebhookAck:
    try:
        task_name = await enqueuer.enqueue_payment_webhook(payload)
    except CloudTasksConfigError as exc:
        logger.error("cloud-tasks config error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="payment queue unavailable",
        ) from exc

    logger.info(
        "payment-webhook enqueued tx=%s status=%s task=%s",
        payload.transactionId,
        payload.status.value,
        task_name,
    )
    return PaymentWebhookAck(
        received=True,
        transactionId=payload.transactionId,
        status=payload.status,
    )


@router.post("/payment-webhook/process", status_code=status.HTTP_200_OK)
async def payment_webhook_process(
    payload: PaymentWebhookPayload,
) -> dict[str, str]:
    """Worker endpoint invoked by Cloud Tasks to actually process the payment event."""
    logger.info(
        "payment-webhook processing tx=%s invoice=%s status=%s amount=%s %s",
        payload.transactionId,
        payload.invoiceId,
        payload.status.value,
        payload.amount,
        payload.currency,
    )
    return {"status": "processed", "transactionId": payload.transactionId}
