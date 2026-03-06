#!/usr/bin/env python3
"""
Classify jobs from Redis queue and save to PostgreSQL.

This script:
1. Reads pending jobs from Redis queue
2. Classifies each job using AI (Azure OpenAI / Ollama)
3. Saves classification results to PostgreSQL
4. Skips already-classified jobs

Usage:
    python scripts/classify_queued_jobs.py [--force]  # --force to reclassify existing
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Load environment
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import redis
import psycopg2


def get_config():
    """Build AI config from environment variables."""
    return {
        "ai": {
            "enabled": True,
            "providers": {
                "primary": os.getenv("AI_PRIMARY_PROVIDER", "azure-openai"),
                "fallback": os.getenv("AI_FALLBACK_PROVIDER", "ollama"),
                "azure_openai": {
                    "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
                    "api_key": os.getenv("AZURE_OPENAI_KEY"),
                    "deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4"),
                    "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
                    "timeout_seconds": 120,
                },
                "ollama": {
                    "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
                    "model": os.getenv("OLLAMA_MODEL", "llama3.1"),
                    "timeout_seconds": 120,
                },
            },
            "retry": {
                "max_attempts": 2,
                "delay_seconds": 1.0,
                "backoff_multiplier": 2.0,
            },
        }
    }


def get_existing_categories(cursor):
    """Get existing categories from database."""
    categories = {"domains": [], "modules": [], "job_groups": []}

    cursor.execute("SELECT name, type FROM categories WHERE approved = true")
    for row in cursor.fetchall():
        name, cat_type = row
        if cat_type == "domain":
            categories["domains"].append(name)
        elif cat_type == "module":
            categories["modules"].append(name)
        elif cat_type == "job_group":
            categories["job_groups"].append(name)

    return categories


def get_or_create_category(cursor, name, cat_type, parent_id=None):
    """Get existing category or create new one."""
    import uuid

    if parent_id:
        cursor.execute(
            "SELECT id FROM categories WHERE name = %s AND type = %s AND parent_id = %s",
            (name, cat_type, parent_id)
        )
    else:
        cursor.execute(
            "SELECT id FROM categories WHERE name = %s AND type = %s AND parent_id IS NULL",
            (name, cat_type)
        )

    row = cursor.fetchone()
    if row:
        return row[0]

    # Create new category
    new_id = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO categories (id, name, type, parent_id, approved, ai_discovered)
        VALUES (%s, %s, %s, %s, true, true)
        RETURNING id
    """, (new_id, name, cat_type, parent_id))

    return cursor.fetchone()[0]


def is_already_classified(cursor, job_path):
    """Check if job is already classified."""
    cursor.execute(
        "SELECT id FROM job_classifications WHERE job_path = %s",
        (job_path,)
    )
    return cursor.fetchone() is not None


def save_classification(cursor, job_path, result, provider):
    """Save classification result to database."""
    import uuid

    # Get or create category hierarchy
    domain_id = get_or_create_category(cursor, result.domain, "domain")
    module_id = get_or_create_category(cursor, result.module, "module", domain_id)
    job_group_id = get_or_create_category(cursor, result.job_group, "job_group", module_id)

    job_name = os.path.basename(job_path)
    job_id = str(uuid.uuid4())

    cursor.execute("""
        INSERT INTO job_classifications (
            id, job_path, job_name, domain_id, module_id, job_group_id,
            complexity, complexity_score, complexity_reasoning,
            confidence_score, metrics, ai_provider
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_path) DO UPDATE SET
            domain_id = EXCLUDED.domain_id,
            module_id = EXCLUDED.module_id,
            job_group_id = EXCLUDED.job_group_id,
            complexity = EXCLUDED.complexity,
            complexity_score = EXCLUDED.complexity_score,
            complexity_reasoning = EXCLUDED.complexity_reasoning,
            confidence_score = EXCLUDED.confidence_score,
            metrics = EXCLUDED.metrics,
            ai_provider = EXCLUDED.ai_provider,
            updated_at = NOW()
    """, (
        job_id,
        job_path,
        job_name,
        domain_id,
        module_id,
        job_group_id,
        result.complexity.value,
        result.complexity_score,
        result.complexity_reasoning,
        result.confidence_score,
        json.dumps(result.metrics.to_dict() if result.metrics else {}),
        provider,
    ))


def get_jobs_from_queue(redis_client):
    """Get all pending jobs from Redis queue."""
    jobs = []

    # Check all priority queues
    for priority in range(4):
        queue_key = f"validation:queue:priority:{priority}"
        job_ids = redis_client.zrange(queue_key, 0, -1)

        for job_id in job_ids:
            job_key = f"validation:job:{job_id}"
            job_data = redis_client.hgetall(job_key)
            if job_data and job_data.get("job_path"):
                jobs.append({
                    "id": job_id,
                    "path": job_data["job_path"],
                    "priority": priority,
                })

    return jobs


def get_all_jobs_from_redis(redis_client):
    """Get all jobs from Redis history (not just queued)."""
    jobs = []
    seen_paths = set()

    # Scan for all validation:job:* keys
    for key in redis_client.scan_iter("validation:job:*"):
        job_data = redis_client.hgetall(key)
        if job_data and job_data.get("job_path"):
            job_path = job_data["job_path"]
            # Deduplicate by path (same file may have multiple validation runs)
            if job_path not in seen_paths:
                seen_paths.add(job_path)
                job_id = key.split(":")[-1]
                jobs.append({
                    "id": job_id,
                    "path": job_path,
                    "status": job_data.get("status", "UNKNOWN"),
                })

    return jobs


def main():
    parser = argparse.ArgumentParser(description="Classify queued jobs")
    parser.add_argument("--force", action="store_true", help="Force reclassify existing jobs")
    parser.add_argument("--all", action="store_true", help="Classify all jobs from Redis history (not just queued)")
    args = parser.parse_args()

    print("=" * 60)
    print("Job Classification from Redis")
    print("=" * 60)

    # Connect to Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    print(f"\n[Redis] Connected to {redis_url}")

    # Get jobs from queue or history
    if args.all:
        jobs = get_all_jobs_from_redis(redis_client)
        print(f"[Redis] Found {len(jobs)} unique jobs in history")
    else:
        jobs = get_jobs_from_queue(redis_client)
        print(f"[Redis] Found {len(jobs)} jobs in queue")

    if not jobs:
        print("\nNo jobs to classify.")
        if not args.all:
            print("Tip: Use --all to classify jobs from Redis history (already processed jobs).")
        return

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "validation_results"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    cursor = conn.cursor()
    print(f"[PostgreSQL] Connected")

    # Initialize classifier
    from ai.classifier import LLMClassifier
    config = get_config()
    classifier = LLMClassifier(config)
    print(f"[AI] Classifier initialized (primary: {config['ai']['providers']['primary']})")

    # Get existing categories
    existing_categories = get_existing_categories(cursor)
    print(f"[DB] Existing categories: {len(existing_categories['domains'])} domains, "
          f"{len(existing_categories['modules'])} modules, "
          f"{len(existing_categories['job_groups'])} job groups")

    # Classify each job
    print("\n" + "-" * 60)
    classified = 0
    skipped = 0
    errors = 0

    for job in jobs:
        job_path = job["path"]
        job_name = os.path.basename(job_path)

        # Check if already classified
        if not args.force and is_already_classified(cursor, job_path):
            print(f"[SKIP] {job_name} (already classified)")
            skipped += 1
            continue

        # Check if file exists
        if not os.path.exists(job_path):
            print(f"[SKIP] {job_name} (file not found)")
            skipped += 1
            continue

        print(f"\n[CLASSIFY] {job_name}")

        try:
            # Read code
            with open(job_path, 'r', encoding='utf-8') as f:
                code = f.read()

            # Classify
            result, provider = classifier.classify_job(
                code=code,
                file_path=job_path,
                existing_categories=existing_categories,
            )

            # Save to database
            save_classification(cursor, job_path, result, provider)
            conn.commit()

            print(f"    Domain: {result.domain}")
            print(f"    Module: {result.module}")
            print(f"    Job Group: {result.job_group}")
            print(f"    Complexity: {result.complexity.value} ({result.complexity_score})")
            print(f"    Provider: {provider}")

            # Update existing categories for next job
            if result.domain not in existing_categories["domains"]:
                existing_categories["domains"].append(result.domain)
            if result.module not in existing_categories["modules"]:
                existing_categories["modules"].append(result.module)
            if result.job_group not in existing_categories["job_groups"]:
                existing_categories["job_groups"].append(result.job_group)

            classified += 1

        except Exception as e:
            print(f"    ERROR: {e}")
            errors += 1

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total jobs in queue: {len(jobs)}")
    print(f"  Classified: {classified}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")

    cursor.close()
    conn.close()
    redis_client.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
