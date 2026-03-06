#!/bin/bash
# Start the Git Poller
# Usage: ./scripts/start-poller.sh [--once]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SRC_DIR="$PROJECT_ROOT/src"

echo "=== Starting Git Poller ==="

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
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_DB="${POSTGRES_DB:-validation_results}"
export POSTGRES_USER="${POSTGRES_USER:-validator}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-validator_secret}"

echo "Jobs base path: $JOBS_BASE_PATH"
echo "Redis URL: $REDIS_URL"

cd "$SRC_DIR"

# Pass through arguments (e.g., --once)
python poller_runner.py "$@"
