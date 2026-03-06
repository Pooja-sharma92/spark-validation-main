#!/bin/bash
# Start the Validation Executor
# Usage: ./scripts/start-executor.sh [--worker-id ID] [--dry-run]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SRC_DIR="$PROJECT_ROOT/src"

echo "=== Starting Validation Executor ==="

# Activate virtual environment if exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
    echo "✓ Virtual environment activated"
fi

# Set environment variables
export PYTHONPATH="$SRC_DIR:$PYTHONPATH"
export JOBS_BASE_PATH="${JOBS_BASE_PATH:-/Users/xuym/Documents/GitHubLocal/sbi}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_PORT:-5433}"
export POSTGRES_DB="${POSTGRES_DB:-validation_results}"
export POSTGRES_USER="${POSTGRES_USER:-validator}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-validator_secret}"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"

echo "Redis URL: $REDIS_URL"
echo "Spark Master: $SPARK_MASTER"

cd "$SRC_DIR"

# Pass through arguments (e.g., --worker-id, --dry-run)
python executor_runner.py "$@"
