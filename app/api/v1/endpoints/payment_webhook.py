import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import DatabaseConfigError
from app.repositories.payment_repository import (
    PaymentRepository,
    get_payment_repository,
)
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
    repository: PaymentRepository = Depends(get_payment_repository),
) -> dict[str, str]:
    """Worker endpoint invoked by Cloud Tasks to persist the payment event."""
    logger.info(
        "payment-webhook processing tx=%s invoice=%s status=%s amount=%s %s",
        payload.transactionId,
        payload.invoiceId,
        payload.status.value,
        payload.amount,
        payload.currency,
    )

    try:
        payment = await repository.create_from_webhook(payload)
    except DatabaseConfigError as exc:
        logger.error("database config error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="payment store unavailable",
        ) from exc
    except SQLAlchemyError as exc:
        logger.exception(
            "payment-webhook persistence failed tx=%s", payload.transactionId
        )
        # Returning 5xx so Cloud Tasks retries the delivery.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="payment persistence failed",
        ) from exc

    if payment is None:
        return {"status": "duplicate", "transactionId": payload.transactionId}

    logger.info(
        "payment-webhook stored id=%s tx=%s status=%s",
        payment.id,
        payload.transactionId,
        payload.status.value,
    )
    return {"status": "processed", "transactionId": payload.transactionId}
