import logging
from functools import lru_cache
from typing import Any

from app.core.config import Settings, get_settings
from app.schemas.payment_webhook import PaymentWebhookPayload

logger = logging.getLogger(__name__)


class CloudTasksConfigError(RuntimeError):
    pass


class CloudTasksEnqueuer:
    """Async wrapper around google-cloud-tasks for the payments webhook queue."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Any | None = None

    def _get_client(self) -> Any:
        if self._client is None:
            from google.cloud import tasks_v2

            self._client = tasks_v2.CloudTasksAsyncClient()
        return self._client

    def _validate(self) -> None:
        s = self._settings
        missing = [
            name
            for name, value in (
                ("GCP_PROJECT_ID", s.gcp_project_id),
                ("GCP_TASKS_QUEUE", s.gcp_tasks_queue),
                ("GCP_TASKS_TARGET_URL", s.gcp_tasks_target_url),
            )
            if not value
        ]
        if missing:
            raise CloudTasksConfigError(
                f"Cloud Tasks misconfigured, missing: {', '.join(missing)}"
            )

    async def enqueue_payment_webhook(
        self, payload: PaymentWebhookPayload
    ) -> str | None:
        s = self._settings

        if not s.gcp_tasks_enabled:
            logger.warning(
                "cloud-tasks DISABLED; skipping enqueue tx=%s "
                "(set GCP_TASKS_ENABLED=true to enable)",
                payload.transactionId,
            )
            return None

        self._validate()

        from google.api_core import exceptions as gax_exceptions
        from google.cloud import tasks_v2

        client = self._get_client()
        parent = client.queue_path(
            s.gcp_project_id, s.gcp_location, s.gcp_tasks_queue
        )

        body = payload.model_dump_json().encode("utf-8")

        http_request = tasks_v2.HttpRequest(
            http_method=tasks_v2.HttpMethod.POST,
            url=s.gcp_tasks_target_url,
            headers={"Content-Type": "application/json"},
            body=body,
        )

        if s.gcp_tasks_service_account_email:
            http_request.oidc_token = tasks_v2.OidcToken(
                service_account_email=s.gcp_tasks_service_account_email,
                audience=s.gcp_tasks_target_url,
            )

        task_id = f"payment-{payload.transactionId}"
        task = tasks_v2.Task(
            name=client.task_path(
                s.gcp_project_id, s.gcp_location, s.gcp_tasks_queue, task_id
            ),
            http_request=http_request,
        )

        try:
            response = await client.create_task(parent=parent, task=task)
        except gax_exceptions.AlreadyExists:
            # Idempotent enqueue: same transactionId already submitted.
            logger.info(
                "cloud-tasks duplicate enqueue ignored tx=%s",
                payload.transactionId,
            )
            return task.name

        logger.info("cloud-tasks enqueued task=%s", response.name)
        return response.name


@lru_cache
def get_cloud_tasks_enqueuer() -> CloudTasksEnqueuer:
    return CloudTasksEnqueuer(get_settings())
