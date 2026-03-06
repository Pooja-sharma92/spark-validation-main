#!/usr/bin/env python3
"""
Submit a test validation job to the queue.

Usage:
    python test_submit_job.py <job_path> [--priority P0|P1|P2|P3] [--branch BRANCH]

Examples:
    python test_submit_job.py /path/to/your/job.py
    python test_submit_job.py /path/to/your/job.py --priority P1
    python test_submit_job.py /path/to/your/job.py --priority P0 --branch feature/test
"""

import argparse
import uuid
import redis
from datetime import datetime, timezone


def submit_job(job_path: str, priority: str = "P2", branch: str = "main", triggered_by: str = "test-user"):
    """Submit a validation job to the Redis queue."""
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # -----------------------------
    # IMPORTANT: normalize job_path
    # -----------------------------
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[1]  # .../spark-validation-main
    p = Path(job_path)

    # Convert to relative (repo-root) if possible
    try:
        if p.is_absolute():
            p = p.resolve()
            rel = p.relative_to(repo_root)
        else:
            rel = p
    except Exception:
        rel = p

    # Convert Windows "\" -> "/" so Linux container can open it
    normalized_job_path = str(rel).replace("\\", "/")

    # Parse priority (P0, P1, P2, P3 -> 0, 1, 2, 3)
    priority_num = int(priority[1]) if priority.startswith("P") else 2

    # Create job
    job_id = str(uuid.uuid4())
    job_data = {
        'id': job_id,
        'job_path': normalized_job_path,    # ✅ use normalized path
        'status': 'PENDING',
        'priority': str(priority_num),
        'trigger_source': 'MANUAL',
        'branch': branch,
        'commit_sha': 'test123',
        'triggered_by': triggered_by,
        'created_at': datetime.now(timezone.utc).isoformat(),
    }

    r.hset(f'validation:job:{job_id}', mapping=job_data)
    r.zadd(f'validation:queue:priority:{priority_num}', {job_id: datetime.now().timestamp()})

    print("✓ Job submitted successfully!")
    print(f"  ID: {job_id}")
    print(f"  Path: {normalized_job_path}")
    print(f"  Priority: P{priority_num}")
    print(f"  Branch: {branch}")

    total = sum(r.zcard(f'validation:queue:priority:{p}') for p in range(4))
    print(f"\nQueue status: {total} pending job(s)")
    for p in range(4):
        count = r.zcard(f'validation:queue:priority:{p}')
        if count > 0:
            print(f"  P{p}: {count} job(s)")

    return job_id



def main():
    parser = argparse.ArgumentParser(description="Submit a validation job to the queue")
    parser.add_argument("job_path", help="Path to the Python job file to validate")
    parser.add_argument("--priority", default="P2", choices=["P0", "P1", "P2", "P3"],
                       help="Job priority (default: P2)")
    parser.add_argument("--branch", default="main", help="Git branch (default: main)")
    parser.add_argument("--user", default="test-user", help="User who triggered the job")

    args = parser.parse_args()

    submit_job(args.job_path, args.priority, args.branch, args.user)


if __name__ == "__main__":
    main()
