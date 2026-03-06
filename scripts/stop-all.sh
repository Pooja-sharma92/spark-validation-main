#!/bin/bash
# Stop all validation services
# Usage: ./scripts/stop-all.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/src/docker"

echo "=== Stopping Validation Services ==="

# Kill any running Python processes (poller/executor)
pkill -f "python.*poller_runner.py" 2>/dev/null && echo "✓ Poller stopped" || echo "  No poller running"
pkill -f "python.*executor_runner.py" 2>/dev/null && echo "✓ Executor stopped" || echo "  No executor running"

# Stop Docker infrastructure
cd "$DOCKER_DIR"
docker-compose stop redis postgres 2>/dev/null && echo "✓ Docker services stopped" || echo "  Docker services not running"

echo ""
echo "=== All services stopped ==="
