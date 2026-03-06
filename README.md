# Spark Validator

A framework for validating and analyzing Spark jobs migrated from DataStage.

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Git Poller     │────▶│  Redis Queue │────▶│  Executor        │
│  (Monitor GitHub)│     │  (Task Queue)│     │  (4-Stage Valid.)│
└─────────────────┘     └─────────────┘     └──────────────────┘
        │                                           │
        │                                           ▼
        │                                   ┌──────────────────┐
        │                                   │  PostgreSQL      │
        └──────────────────────────────────▶│  (Result Storage)│
                                            └──────────────────┘
```

## Project Structure

```
spark-validator/
├── src/
│   ├── poller_runner.py    # Git Poller entry point
│   ├── executor_runner.py  # Executor entry point
│   ├── validator/          # Core framework
│   │   ├── analyzer/       # Static code analysis
│   │   ├── executor/       # Spark executor
│   │   ├── queue/          # Redis queue management
│   │   ├── trigger/        # Triggers (Git Poller, Webhook)
│   │   └── ...
│   ├── jobs/               # Sample Spark jobs
│   ├── config/             # Configuration files
│   │   └── framework.yaml  # Main configuration
│   └── docker/             # Docker infrastructure
├── tests/
├── .venv/                  # Python virtual environment
└── requirements.txt
```

## Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install redis pyyaml structlog
```

### 2. Start Infrastructure

```bash
cd src/docker

# Start core services (without Spark cluster)
docker compose up -d redis postgres minio iceberg-rest

# Verify services
docker ps
```

| Service | Port | Description |
|---------|------|-------------|
| Redis | 6379 | Validation task queue |
| PostgreSQL | 5432 | Result storage |
| MinIO | 9008/9009 | S3-compatible storage |
| Iceberg REST | 8181 | Iceberg Catalog |

### 3. Configure Repositories

Edit `src/config/framework.yaml`:

```yaml
# Spark Job base directory
jobs:
  base_path: "/path/to/your/jobs"

# Git Poller configuration
triggers:
  git_poller:
    enabled: true
    interval_seconds: 300  # 5 minutes
    file_patterns:
      - "*.py"
      - "*.sql"
    repositories:
      - name: "domain-customer"
        url: "https://github.ibm.com/org/repo.git"
        branches:
          - "main"
        jobs_subdir: "src"
```

### 4. Run the Pipeline

**Terminal 1 - Git Poller:**
```bash
source .venv/bin/activate
python src/poller_runner.py
```

**Terminal 2 - Executor:**
```bash
source .venv/bin/activate
python src/executor_runner.py
```

## Validation Stages

Executor runs 4-stage validation:

| Stage | Description | Severity |
|-------|-------------|----------|
| `syntax` | Python AST syntax check | ERROR |
| `imports` | Star imports, deprecated modules (pyspark.mllib) | WARNING |
| `sql` | SQL statement validation in spark.sql() | ERROR/WARNING |
| `logic` | Bare except, .collect(), s3a://, hardcoded credentials | ERROR/WARNING |

### Logic Checks

- **Bare except**: `except:` should be `except Exception:`
- **Collect without limit**: `.collect()` should have `.limit(N)` before it
- **s3a:// protocol**: Prohibited, use `s3://` instead
- **Hardcoded credentials**: Detects `password=`, `secret=`, `api_key=`

## Components

### Git Poller (`poller_runner.py`)

- Periodically pulls updates from GitHub/GitHub Enterprise
- Detects changed .py files
- Adds tasks to Redis queue

```bash
# Single run (for testing)
python src/poller_runner.py --once

# Continuous run (production)
python src/poller_runner.py
```

### Executor (`executor_runner.py`)

- Gets tasks from Redis queue
- Executes 4-stage validation
- Writes results back to Redis

```bash
# Specify worker ID
python src/executor_runner.py --worker-id worker-1
```

### Queue Priority

| Priority | Value | Use Case |
|----------|-------|----------|
| CRITICAL | 0 | hotfix/* branches |
| MANUAL | 1 | Manual trigger |
| CI_CD | 2 | main/develop branches |
| BATCH | 3 | Scheduled scans |

## Configuration

### framework.yaml

```yaml
# Redis configuration
redis:
  url: "redis://localhost:6379/0"

# PostgreSQL configuration
postgres:
  host: "localhost"
  port: 5432
  database: "validation_results"

# Queue configuration
queue:
  max_concurrent: 5
  dedup_window_seconds: 60

# Validation stages
validation:
  stages:
    - name: "syntax"
      enabled: true
      timeout_seconds: 60
    - name: "logic"
      enabled: true
      timeout_seconds: 60
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | redis://localhost:6379/0 | Redis connection |
| `JOBS_BASE_PATH` | ./sbi | Job storage directory |
| `POSTGRES_HOST` | localhost | PostgreSQL host |

## Development

### Run Tests

```bash
pytest tests/ -v
```

### Manual Queue Job

```python
import asyncio
import redis.asyncio as redis

async def queue_job():
    r = await redis.from_url('redis://localhost:6379/0')
    # ... enqueue logic
    await r.aclose()

asyncio.run(queue_job())
```

## DataStage Migration Context

This project is used to validate ETL jobs migrated from IBM DataStage to PySpark:

- **DB2 Connector → spark.sql()**: SQL queries
- **Aggregator → groupBy().agg()**: Aggregation logic
- **Transformer → select/withColumn**: Column operations
- **Join → chained .join()**: Multi-table joins

### DataStage Stage IDs

Comment format `V###S#` in code is used to trace back to original DataStage design:
- `V138S0`: Primary stages (sources, lookups)
- Activity types: "DB2 Connector Activity", "Join function", "Aggregator function"
