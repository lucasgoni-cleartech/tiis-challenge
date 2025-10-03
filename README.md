# TIIS Backend – ETL Pipeline (Redis, SQLite, SFTP)

End-to-end ETL pipeline for extracting, validating, transforming, persisting, and exposing user data.
Architecture built with Redis Pub/Sub, SQLite, SFTP, and FastAPI.

---

## 🔐 Secrets Setup

**Before running the stack**, you must generate an SSH key pair for the SFTP server.

From the project root (`TIIS-CHALLENGE/`):

```bash
mkdir -p tiis-backend/infra/secrets/sftp
ssh-keygen -t rsa -b 4096 -f tiis-backend/infra/secrets/sftp/id_rsa -N ""

```

This creates:

- **`secrets/sftp/id_rsa`** → private key (mounted as Docker secret for `backend-saver`)
- **`secrets/sftp/id_rsa.pub`** → public key (mounted into the SFTP container)

⚠️ **Important:** Do not commit the `secrets/` directory to version control.

---

## 🚀 Quick Start

### 1. Requirements

- Docker & Docker Compose
- (Optional) Python 3.12 + Poetry for local development

### 2. Setup

```bash
cd tiis-backend/infra/compose
cp .env.example .env
# Edit .env with real credentials (SFTP, Redis, etc.)
```

### 3. Deploy

```bash
docker compose up -d
```

This starts:

- **redis** (Pub/Sub messaging)
- **data-init** (one-shot volume initialization)
- **sftp-server** (file uploads)
- **backend-extractor** (data extraction)
- **backend-transformer** (validation & enrichment)
- **backend-saver** (SQLite + SFTP persistence)

Check all containers are running:

```bash
docker compose ps
```

---

## 🧪 End-to-End Testing

Run and inspect each component:

### 1. Extractor (one-shot run)

```bash
docker compose exec -e RUN_ONCE=true backend-extractor python -m apps.extractor
docker compose logs backend-extractor --tail=50
```

### 2. Transformer

```bash
docker compose logs backend-transformer --tail=50
```

### 3. Saver (DB + SFTP)

```bash
docker compose logs backend-saver --tail=50
```

### 4. SFTP server

List uploaded files:

```bash
docker compose exec sftp-server find /home/tiis_user/uploads -type f
```

### 5. Redis

Check Pub/Sub activity:

```bash
docker compose logs redis --tail=50
```

### 6. SQLite Database

Inspect persisted data:

```bash
docker compose exec backend-saver sqlite3 /data/db/app.db "SELECT COUNT(*) FROM raw_users;"
docker compose exec backend-saver sqlite3 /data/db/app.db "SELECT COUNT(*) FROM processed_users;"
docker compose exec backend-saver sqlite3 /data/db/app.db "SELECT COUNT(*) FROM dlq_users;"
```

---

## 📂 Data Flow

1. **Extractor** → `raw_users/records_*.jsonl` → Publishes to Redis `files.raw_users`
2. **Transformer** → `processed_users/etl_*.jsonl` + `dlq/invalid_users_*.jsonl` → Publishes to Redis `files.processed_users` and `files.dlq`
3. **Saver** → Inserts into SQLite (`/data/db/app.db`) + uploads to SFTP (`/uploads`)

### Database Tables

| Table              | Columns                                     | Purpose                          |
|--------------------|---------------------------------------------|----------------------------------|
| `raw_users`        | `id`, `inserted_at`, `data`                 | Raw extracted records            |
| `processed_users`  | `id`, `inserted_at`, `data`, `department_code` | Valid enriched records           |
| `dlq_users`        | `id`, `inserted_at`, `error_reason`, `data` | Invalid/failed records (DLQ)     |

---

## 🔑 Key Environment Variables

| Variable                  | Description                           | Default                        |
|---------------------------|---------------------------------------|--------------------------------|
| `USERS_API_BASE`          | Source API                            | `https://dummyjson.com/users`  |
| `REDIS_URL`               | Redis connection                      | `redis://redis:6379/0`         |
| `SQLITE_URL`              | SQLite DB path                        | `sqlite:////data/db/app.db`    |
| `SFTP_HOST`               | SFTP server host                      | `sftp.example.com`             |
| `SFTP_PORT`               | SFTP server port                      | `22`                           |
| `SFTP_USERNAME`           | SFTP username                         | `tiis_user`                    |
| `SFTP_KEY_PATH`           | SSH private key path                  | `/run/secrets/sftp_key`        |
| `SFTP_KEY_PASSPHRASE`     | SSH key passphrase                    | (empty)                        |
| `SFTP_REMOTE_BASE`        | SFTP remote base directory            | `/uploads/tiis`                |
| `DEPARTMENTS_CSV`         | Departments lookup CSV path           | `/data/departments.csv`        |
| `EXTRACT_SCHEDULE_CRON`   | Extraction cron schedule              | `0 3 * * *`                    |

See `infra/compose/.env.example` for the full list.

---

## 🧩 Components

### 1. **Extractor** (`apps/extractor/`)

- Scheduled extraction (daily cron at 03:00)
- Resumable pagination (`limit`/`skip` with persistent state)
- Retries with exponential backoff (3 attempts)
- Output: `raw_users/records_[YYYYMMDD_HHMMSS].jsonl`
- Publishes to Redis Pub/Sub: `files.raw_users`

### 2. **Transformer** (`apps/transformer/`)

- Redis Pub/Sub subscriber: `files.raw_users`
- Pydantic validation (id, firstName, email, age 18-65, company.department)
- Enrichment: `departments.csv` mapping → `department_code`
- Outputs:
  - Valid: `processed_users/etl_[YYYYMMDD_HHMMSS].jsonl`
  - Invalid (DLQ): `dlq/invalid_users_[YYYYMMDD_HHMMSS].jsonl` (with `error_reason`)
- Publishes to Redis Pub/Sub: `files.processed_users`, `files.dlq`
- **Auto-seeds** `departments.csv` from image if missing on startup

### 3. **Saver** (`apps/saver/`)

- Redis Pub/Sub subscriber: `files.raw_users`, `files.processed_users`, `files.dlq`
- SQLite persistence (3 tables: `raw_users`, `processed_users`, `dlq_users`)
- SFTP upload with SSH Key + passphrase (paramiko)

---

## 🛠️ Tech Stack

- **Python**: 3.12
- **Dependencies**:
  - ETL: `requests`, `tenacity`, `pydantic`, `apscheduler`
  - Messaging: `redis`
  - SFTP: `paramiko`
  - API: `fastapi`, `uvicorn[standard]`
  - DB: `sqlalchemy`
  - Utils: `python-dotenv`, `orjson`
- **Orchestration**: Docker Compose
- **DB**: SQLite
- **Message Queue**: Redis Pub/Sub

---

## 🧑‍💻 Local Development

```bash
# Install dependencies
poetry install

# Configure environment
cp infra/compose/.env.example .env

# Run components (examples)
poetry run python -m apps.extractor
poetry run python -m apps.transformer.consumer
poetry run python -m apps.saver.consumer
```

---

## 📦 Docker Compose Services

| Service                  | Description                              | Depends On           |
|--------------------------|------------------------------------------|----------------------|
| `redis`                  | Redis Pub/Sub messaging broker           | -                    |
| `data-init`              | One-shot volume initialization (uid 1000)| -                    |
| `sftp-server`            | SFTP server for file uploads             | -                    |
| `backend-extractor`      | Scheduled data extraction                | `redis`, `data-init` |
| `backend-transformer`    | Validation & enrichment                  | `redis`, `data-init` |
| `backend-saver`          | DB persistence + SFTP upload             | `redis`, `sftp-server`, `data-init` |

---

## 🔒 Security Best Practices

- All backend services run as **non-root user** (`uid 1000`)
- SSH keys stored as **Docker secrets**
- Sensitive config in `.env` (not committed)
- `init: true` for proper signal handling (PID 1)
- Volume permissions managed by `data-init` service

---

## 🧪 Testing

```bash
# Unit tests
poetry run pytest tests/unit -v

# Integration tests (requires Redis + SQLite)
poetry run pytest tests/integration -v

# Coverage
poetry run pytest --cov=apps --cov=services --cov=utils --cov-report=html
```

---

## 📄 License

MIT - See [LICENSE](./LICENSE)
