# miso-travelhub-service-payments

Microservicio de pagos del proyecto **TravelHub** (MISO). Recibe el webhook de la pasarela de pagos y **publica el evento en un topic de Kafka**. Un worker independiente (otro proyecto) consume el topic y persiste el evento — este servicio no toca base de datos.

## Stack

- Python 3.12
- FastAPI + Uvicorn
- aiokafka (cliente Kafka async)
- Pydantic v2 / pydantic-settings
- pytest + httpx
- Docker
- Despliegue en Cloud Run via GitHub Actions

## Arquitectura

```
   Pasarela
      │
      │ POST /api/v1/payment-webhook
      ▼
┌──────────────────┐       publish        ┌──────────────────┐
│  Cloud Run       │ ───────────────────▶ │  Kafka (VM GCP)  │
│  (este servicio) │  topic=payments-     │  bootstrap=...   │
│                  │        queue         │  topic=payments- │
└──────────────────┘                      │       queue      │
                                          └────────┬─────────┘
                                                   │ consume
                                                   ▼
                                          ┌──────────────────┐
                                          │  Worker (otro    │
                                          │  proyecto)       │
                                          │  → persiste en DB│
                                          └──────────────────┘
```

Cada mensaje se publica con `key = transactionId`, lo que garantiza que todos los eventos de la misma transacción caigan en la misma partición (orden y deduplicación predecibles para el consumidor). El producer corre con `acks=all` y `enable_idempotence=True`.

## Estructura

```
app/
├── api/v1/
│   ├── endpoints/
│   │   ├── health.py
│   │   └── payment_webhook.py
│   └── router.py
├── core/config.py            # Settings (env vars)
├── schemas/payment_webhook.py# Pydantic
├── services/kafka_producer.py# AIOKafkaProducer wrapper + dependency
└── main.py                   # create_app + lifespan (start/stop producer)
tests/
```

## Endpoints

| Método | Ruta                          | Descripción                                    |
|--------|-------------------------------|------------------------------------------------|
| GET    | `/api/v1/health`              | Liveness probe                                 |
| POST   | `/api/v1/payment-webhook`     | Recibe el webhook y lo publica en Kafka. Responde **202 Accepted**. |

### Códigos de error de `/payment-webhook`

| Código | Cuándo                                              |
|--------|-----------------------------------------------------|
| 422    | Payload inválido (validación Pydantic)              |
| 502    | Publish a Kafka falló (caller debería reintentar)   |
| 503    | Kafka deshabilitado o mal configurado               |

### Payload aceptado

```json
{
  "status": "APPROVED | DECLINED | PENDING | FAILED | REFUNDED",
  "message": "...",
  "invoiceId": "...",
  "amount": 123.45,
  "currency": "COP | EUR | USD",
  "cardHolder": "...",
  "maskedCard": "**** **** **** 6880",
  "transactionId": "TX-...",
  "processedAt": "2026-05-02T18:13:14.424Z"
}
```

## Variables de entorno

Ver [.env.example](.env.example) para la lista completa.

| Variable                    | Default                              | Notas                                                              |
|-----------------------------|--------------------------------------|--------------------------------------------------------------------|
| `APP_ENV`                   | `development`                        |                                                                    |
| `APP_DEBUG`                 | `false`                              |                                                                    |
| `CORS_ORIGINS`              | `["*"]`                              | Lista JSON                                                         |
| `KAFKA_ENABLED`             | `false`                              | Si `false`, el endpoint responde 503                               |
| `KAFKA_BOOTSTRAP_SERVERS`   | —                                    | `host:9092` (coma-separados si hay varios)                         |
| `KAFKA_TOPIC`               | `payments-queue`                     |                                                                    |
| `KAFKA_CLIENT_ID`           | `miso-travelhub-service-payments`    |                                                                    |
| `KAFKA_ACKS`                | `all`                                |                                                                    |
| `KAFKA_REQUEST_TIMEOUT_MS`  | `10000`                              |                                                                    |
| `KAFKA_SECURITY_PROTOCOL`   | `PLAINTEXT`                          | `PLAINTEXT` \| `SSL` \| `SASL_PLAINTEXT` \| `SASL_SSL`             |
| `KAFKA_SASL_MECHANISM`      | —                                    | Requerido si protocol incluye SASL (`PLAIN`/`SCRAM-SHA-256`/...)   |
| `KAFKA_SASL_USERNAME`       | —                                    | Requerido si SASL                                                  |
| `KAFKA_SASL_PASSWORD`       | —                                    | Requerido si SASL — montar desde Secret Manager en producción      |
| `NEW_RELIC_LICENSE_KEY`     | —                                    | Vacío → agente desactivado. En prod montarlo desde Secret Manager  |
| `NEW_RELIC_APP_NAME`        | `miso-travelhub-service-payments`    | Nombre visible en la UI de New Relic                               |
| `NEW_RELIC_ENVIRONMENT`     | `development`                        | `development` \| `staging` \| `production`                         |
| `NEW_RELIC_LOG`             | `stdout`                             | `stdout` \| `stderr` \| ruta de archivo                            |
| `NEW_RELIC_LOG_LEVEL`       | `info`                               | `debug` \| `info` \| `warning` \| `error`                          |
| `NEW_RELIC_DISTRIBUTED_TRACING_ENABLED` | `true`                   |                                                                    |

## Ejecución local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # edita KAFKA_BOOTSTRAP_SERVERS, KAFKA_ENABLED, etc.
uvicorn app.main:app --reload
```

API en `http://localhost:8000` — docs interactivas en `/docs`.

## Con Docker

```bash
docker compose up --build
```

## Tests

```bash
pytest
```

Los tests usan `dependency_overrides` para inyectar un `FakePaymentEventPublisher`, por lo que **no requieren un broker Kafka real**.

## Despliegue

CI/CD vía GitHub Actions ([.github/workflows/deploy.yml](.github/workflows/deploy.yml)) que en cada push a `main`:

1. Ejecuta `pytest`.
2. Construye y empuja la imagen a Artifact Registry.
3. Despliega a Cloud Run.

### GitHub Actions Variables requeridas

Configurar en `Settings → Secrets and variables → Actions → Variables` (scope **Repository**):

| Variable                       | Ejemplo / propósito                                                       |
|--------------------------------|---------------------------------------------------------------------------|
| `GCP_PROJECT_ID`               | `gen-lang-client-0930444414`                                              |
| `GCP_REGION`                   | `us-central1`                                                             |
| `AR_REPOSITORY`                | Nombre del repo en Artifact Registry                                      |
| `SERVICE_NAME`                 | `payments-services`                                                       |
| `RUNTIME_SERVICE_ACCOUNT`      | SA que corre el contenedor (`payments-runtime@…`)                         |
| `KAFKA_ENABLED`                | `true` en producción                                                      |
| `KAFKA_BOOTSTRAP_SERVERS`      | IP/host de la VM con Kafka                                                |
| `KAFKA_TOPIC`                  | `payments-queue`                                                          |
| `KAFKA_CLIENT_ID`              | opcional                                                                  |
| `KAFKA_SECURITY_PROTOCOL`      | `PLAINTEXT` o `SASL_*`                                                    |
| `KAFKA_SASL_MECHANISM`         | si aplica                                                                 |
| `KAFKA_SASL_USERNAME`          | si aplica                                                                 |
| `KAFKA_SASL_PASSWORD_SECRET`   | **nombre del secreto** en Secret Manager (no el password en claro)        |
| `VPC_NETWORK`                  | Solo si Kafka vive en IP privada — nombre corto de la VPC                 |
| `VPC_SUBNET`                   | Solo si Kafka vive en IP privada — subnet en la misma región que Cloud Run|
| `VPC_CONNECTOR`                | Alternativa a `VPC_NETWORK`+`VPC_SUBNET` (Serverless VPC Access)          |
| `NEW_RELIC_APP_NAME`           | Nombre del servicio en NR (default: `miso-travelhub-service-payments`)    |
| `NEW_RELIC_ENVIRONMENT`        | `production`                                                              |
| `NEW_RELIC_LOG_LEVEL`          | `info` (raise a `debug` para troubleshoot)                                |

### GitHub Actions Secrets requeridos

| Secret                  | Propósito                                                                   |
|-------------------------|-----------------------------------------------------------------------------|
| `GCP_SA_KEY`            | JSON del SA con permisos para deploy a Cloud Run + push a Artifact Registry |
| `NEW_RELIC_LICENSE_KEY` | License key de New Relic. Vacío → agente desactivado                        |

### Networking

Si la VM de Kafka tiene **solo IP privada**, Cloud Run necesita salida a VPC. El workflow ya pasa `--network`/`--subnet` (o `--vpc-connector`) cuando las variables correspondientes están definidas, y agrega `--vpc-egress=private-ranges-only`. El runtime SA necesita `roles/compute.networkUser` sobre la subnet (Direct VPC egress).

Además, el firewall de la VPC debe permitir egress hacia la VM de Kafka en el puerto del broker (típicamente `tcp:9092`) desde el rango de la subnet/connector usada.

### Permisos del runtime SA

- `roles/compute.networkUser` sobre la subnet (si se usa Direct VPC egress)
- `roles/secretmanager.secretAccessor` sobre `KAFKA_SASL_PASSWORD` (si se usa SASL)

## Observabilidad — New Relic

El servicio inicializa el agente de New Relic en [`app/core/observability.py`](app/core/observability.py), llamado **antes** de cualquier import de framework en [`app/main.py`](app/main.py). Esto garantiza que los import hooks alcancen a instrumentar FastAPI, httpx, asyncpg, etc.

### Qué se captura

- **Auto-instrumentación**: cada request HTTP a `/api/v1/payment-webhook` es una transacción NR con su latencia, errores y trace distribuido.
- **`/health` se excluye** del tracing (`newrelic.agent.ignore_transaction()`) para no inflar la cuota con liveness probes.
- **Kafka publish** se envuelve con `@newrelic.agent.function_trace(name="kafka/publish_payment_webhook")` y se etiqueta con atributos custom (`kafka.topic`, `kafka.tx_id`, `kafka.invoice_id`, `payment.status`). Errores del publish llaman a `notice_error()` con contexto.
- **httpx** se auto-instrumenta (si llamas a otros servicios, aparecen como external spans).

### Activación

Es opcional: si `NEW_RELIC_LICENSE_KEY` está vacío, el agente queda inactivo y el servicio funciona normal. Para activarlo:

1. En GitHub `Settings → Secrets and variables → Actions → **Secrets**`, agrega:

   | Secret                  | Valor                          |
   |-------------------------|--------------------------------|
   | `NEW_RELIC_LICENSE_KEY` | `<tu-license-key-de-newrelic>` |

2. (Opcional) En `Variables`, ajusta nombre del servicio o entorno:

   | Variable                | Valor                                 |
   |-------------------------|---------------------------------------|
   | `NEW_RELIC_APP_NAME`    | `miso-travelhub-service-payments`     |
   | `NEW_RELIC_ENVIRONMENT` | `production`                          |
   | `NEW_RELIC_LOG_LEVEL`   | `info`                                |

3. Redeploy. Verás en logs:
   ```
   New Relic agent initialized: app_name=miso-travelhub-service-payments environment=production
   Observability: New Relic agent ACTIVE
   ```

> **Nota de seguridad**: la license key se pasa como env var al servicio de Cloud Run en texto plano. Para producción real con datos sensibles, considera mover la key a GCP Secret Manager (montarla vía el bloque `secrets:` del workflow en lugar de `env_vars:`).

### Tests

Los tests **no inicializan** el agente (no hay license key en CI), así que los decoradores son no-ops. No se hacen llamadas a NR durante `pytest`.

## Notas de operación

- El producer es **long-lived**: se inicia en el lifespan de FastAPI y se reutiliza en todas las requests. Cold starts pagan el costo del `start()` (handshake con el broker).
- Si Kafka no responde durante el startup, el contenedor levanta igual y `/payment-webhook` responde 503 hasta que el broker vuelva. Esto evita que un broker caído tumbe los health checks de Cloud Run.
- El endpoint **no persiste nada localmente**: si el publish falla con `KafkaPublishError`, devuelve 502 y el caller debe reintentar. No hay outbox.
