#!/bin/bash
# Start Docker infrastructure (Redis, PostgreSQL)
# Usage: ./scripts/start-infra.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/src/docker"

echo "=== Starting Validation Infrastructure ==="
echo "Docker dir: $DOCKER_DIR"

cd "$DOCKER_DIR"

# Start only infrastructure services
docker-compose up -d redis postgres

echo ""
echo "=== Waiting for services to be healthy ==="
sleep 3

# Check Redis
if docker-compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "✓ Redis is ready"
else
    echo "⚠ Redis may not be ready yet"
fi

# Check PostgreSQL
if docker-compose exec -T postgres pg_isready -U validator -d validation_results 2>/dev/null | grep -q "accepting connections"; then
    echo "✓ PostgreSQL is ready"
else
    echo "⚠ PostgreSQL may not be ready yet (waiting...)"
    sleep 5
fi

echo ""
echo "=== Infrastructure Ready ==="
echo "Redis: redis://localhost:6379"
echo "PostgreSQL: postgresql://validator@localhost:5432/validation_results"
