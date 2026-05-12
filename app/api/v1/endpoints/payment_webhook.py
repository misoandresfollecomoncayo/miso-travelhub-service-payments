import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.schemas.payment_webhook import PaymentWebhookAck, PaymentWebhookPayload
from app.services.kafka_producer import (
    KafkaConfigError,
    KafkaPublishError,
    PaymentEventPublisher,
    get_payment_event_publisher,
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
    publisher: PaymentEventPublisher = Depends(get_payment_event_publisher),
) -> PaymentWebhookAck:
    """Receives the payment webhook and publishes it to Kafka.

    A worker living in a separate project consumes the topic and persists
    the event downstream. This service no longer touches the database.
    """
    logger.info(
        "payment-webhook received tx=%s invoice=%s status=%s amount=%s %s",
        payload.transactionId,
        payload.invoiceId,
        payload.status.value,
        payload.amount,
        payload.currency,
    )

    try:
        await publisher.publish_payment_webhook(payload)
    except KafkaConfigError as exc:
        logger.error("kafka config error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="payment event bus unavailable",
        ) from exc
    except KafkaPublishError as exc:
        # 5xx so the upstream webhook caller retries the delivery.
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="payment event publish failed",
        ) from exc

    return PaymentWebhookAck(
        received=True,
        transactionId=payload.transactionId,
        status=payload.status,
    )
