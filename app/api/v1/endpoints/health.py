import newrelic.agent
from fastapi import APIRouter, Depends

from app.core.config import Settings, get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    # Don't pollute the New Relic dashboard with liveness probe traffic.
    # No-op when no NR transaction is active (tests, NR disabled).
    newrelic.agent.ignore_transaction(flag=True)
    return {"status": "ok"}
