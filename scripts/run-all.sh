#!/bin/bash
# Start the complete validation pipeline
# Usage: ./scripts/run-all.sh
#
# This starts:
# 1. Docker infrastructure (Redis, PostgreSQL)
# 2. Git Poller (in background)
# 3. Executor (in foreground)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "  Spark Validation Pipeline"
echo "=========================================="
echo ""

# Step 1: Start infrastructure
echo "Step 1/3: Starting infrastructure..."
"$SCRIPT_DIR/start-infra.sh"
echo ""

# Step 2: Start poller in background
echo "Step 2/3: Starting Git Poller (background)..."
"$SCRIPT_DIR/start-poller.sh" &
POLLER_PID=$!
echo "Poller PID: $POLLER_PID"
sleep 2

# Step 3: Start executor in foreground
echo "Step 3/3: Starting Executor (foreground)..."
echo ""

# Trap to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down..."
    if kill -0 $POLLER_PID 2>/dev/null; then
        kill $POLLER_PID
        echo "Poller stopped"
    fi
}
trap cleanup EXIT

"$SCRIPT_DIR/start-executor.sh" "$@"
