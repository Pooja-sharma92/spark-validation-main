#!/bin/bash
# Show validation results
# Usage: ./scripts/show-results.sh [job_id]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "  Spark Validation Results"
echo "=========================================="
echo ""

# Queue Status
echo -e "${YELLOW}Queue Status:${NC}"
for p in 0 1 2 3; do
    count=$(redis-cli -n 0 zcard validation:queue:priority:$p 2>/dev/null || echo "0")
    echo "  P$p: $count pending"
done
echo ""

# If specific job_id provided
if [ -n "$1" ]; then
    echo -e "${YELLOW}Job Details: $1${NC}"
    redis-cli -n 0 hgetall "validation:job:$1" 2>/dev/null
    exit 0
fi

# Latest Results from PostgreSQL
echo -e "${YELLOW}Latest Validation Results:${NC}"
docker exec validation-postgres psql -U validator -d validation_results -t -c "
SELECT
    '  ' ||
    CASE WHEN vr.passed THEN '✓' ELSE '✗' END || ' ' ||
    substring(r.id::text from 1 for 8) || ' | ' ||
    r.status || ' | ' ||
    COALESCE(r.branch, '-') || ' | ' ||
    substring(r.job_path from '[^/]+$')
FROM validation_requests r
LEFT JOIN validation_results vr ON r.id = vr.request_id
ORDER BY r.created_at DESC
LIMIT 10;
" 2>/dev/null

echo ""
echo -e "${YELLOW}Recent Stage Results:${NC}"
docker exec validation-postgres psql -U validator -d validation_results -t -c "
SELECT
    '  ' || s.stage || ': ' || s.status || ' (' || round(s.duration_seconds::numeric, 4) || 's)'
FROM stage_results s
JOIN validation_results vr ON s.result_id = vr.id
ORDER BY s.id DESC
LIMIT 5;
" 2>/dev/null

echo ""
echo "=========================================="
echo "  Commands:"
echo "    ./scripts/show-results.sh           # Show all"
echo "    ./scripts/show-results.sh <job_id>  # Show specific job"
echo "=========================================="
