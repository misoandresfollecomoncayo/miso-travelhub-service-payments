from fastapi import APIRouter

from app.api.v1.endpoints import health, payment_webhook

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(payment_webhook.router)
