# miso-travelhub-service-payments

Microservicio de pagos del proyecto **TravelHub** (MISO). Construido con **FastAPI**, **SQLAlchemy 2.x async** y **PostgreSQL**.

## Stack

- Python 3.12
- FastAPI + Uvicorn
- SQLAlchemy 2.x (async) + asyncpg
- Pydantic v2 / pydantic-settings
- pytest + httpx
- Docker / docker-compose

## Estructura

```
app/
├── api/v1/
│   ├── endpoints/        # health, payments
│   └── router.py
├── core/config.py        # Settings (env vars)
├── db/                   # Base, sesión async
├── models/               # SQLAlchemy ORM
├── schemas/              # Pydantic
└── main.py               # create_app + lifespan
tests/
```

## Ejecución local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

API en `http://localhost:8000` — docs en `/docs`.

## Con Docker

```bash
docker compose up --build
```

## Tests

```bash
pytest
```

## Endpoints base

| Método | Ruta                       | Descripción            |
|--------|----------------------------|------------------------|
| GET    | `/api/v1/health`           | Liveness               |
| GET    | `/api/v1/health/db`        | Conectividad a la BD   |
| POST   | `/api/v1/payments`         | Crear pago             |
| GET    | `/api/v1/payments`         | Listar pagos           |
| GET    | `/api/v1/payments/{id}`    | Obtener pago por id    |
