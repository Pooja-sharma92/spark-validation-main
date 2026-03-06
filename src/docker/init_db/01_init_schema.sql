-- Validation Framework Database Schema
-- PostgreSQL initialization script

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Validation Requests Table
CREATE TABLE IF NOT EXISTS validation_requests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_path VARCHAR(1024) NOT NULL,
    trigger_source VARCHAR(50) NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 2,
    batch_id UUID,
    commit_sha VARCHAR(64),
    branch VARCHAR(256),
    triggered_by VARCHAR(256),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    worker_id VARCHAR(256),
    metadata JSONB DEFAULT '{}',

    -- Indexes for common queries
    CONSTRAINT valid_status CHECK (status IN ('pending', 'running', 'completed', 'failed', 'error', 'timeout', 'skipped'))
);

-- Indexes for validation_requests
CREATE INDEX IF NOT EXISTS idx_requests_status ON validation_requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_job_path ON validation_requests(job_path);
CREATE INDEX IF NOT EXISTS idx_requests_created_at ON validation_requests(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_requests_batch_id ON validation_requests(batch_id) WHERE batch_id IS NOT NULL;

-- Validation Results Table
CREATE TABLE IF NOT EXISTS validation_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    request_id UUID NOT NULL REFERENCES validation_requests(id) ON DELETE CASCADE,

    passed BOOLEAN NOT NULL,
    summary JSONB NOT NULL DEFAULT '{}',

    -- Timing
    duration_seconds FLOAT,

    -- Error details
    error_message TEXT,
    error_traceback TEXT,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for results by request
CREATE INDEX IF NOT EXISTS idx_results_request_id ON validation_results(request_id);

-- Stage Results Table
CREATE TABLE IF NOT EXISTS stage_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    result_id UUID NOT NULL REFERENCES validation_results(id) ON DELETE CASCADE,

    stage VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    duration_seconds FLOAT,
    metrics JSONB DEFAULT '{}',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for stage results
CREATE INDEX IF NOT EXISTS idx_stage_results_result_id ON stage_results(result_id);

-- Validation Issues Table
CREATE TABLE IF NOT EXISTS validation_issues (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    stage_result_id UUID NOT NULL REFERENCES stage_results(id) ON DELETE CASCADE,

    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    file_path VARCHAR(1024),
    line_number INTEGER,
    rule_id VARCHAR(100),
    suggestion TEXT,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_severity CHECK (severity IN ('critical', 'error', 'warning', 'info'))
);

-- Index for issues by severity
CREATE INDEX IF NOT EXISTS idx_issues_stage_result_id ON validation_issues(stage_result_id);
CREATE INDEX IF NOT EXISTS idx_issues_severity ON validation_issues(severity);

-- Batch Summaries Table
CREATE TABLE IF NOT EXISTS batch_summaries (
    id UUID PRIMARY KEY,
    total_jobs INTEGER NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,

    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,

    success_rate FLOAT GENERATED ALWAYS AS (
        CASE WHEN total_jobs = 0 THEN 0.0
        ELSE (completed - failed)::FLOAT / total_jobs * 100
        END
    ) STORED
);

-- Queue Statistics Table (for monitoring)
CREATE TABLE IF NOT EXISTS queue_stats_history (
    id SERIAL PRIMARY KEY,
    total_pending INTEGER NOT NULL,
    active_validations INTEGER NOT NULL,
    by_priority JSONB NOT NULL,

    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for stats history (time-series queries)
CREATE INDEX IF NOT EXISTS idx_queue_stats_recorded_at ON queue_stats_history(recorded_at DESC);

-- Partitioning for validation_requests (by month)
-- Note: Uncomment and modify for production use
-- CREATE TABLE validation_requests_y2024m01 PARTITION OF validation_requests
--     FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Function to clean up old stats
CREATE OR REPLACE FUNCTION cleanup_old_stats(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM queue_stats_history
    WHERE recorded_at < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO validator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO validator;

-- Initial log
INSERT INTO queue_stats_history (total_pending, active_validations, by_priority)
VALUES (0, 0, '{"CRITICAL": 0, "MANUAL": 0, "CI_CD": 0, "BATCH": 0}');

COMMENT ON TABLE validation_requests IS 'All validation job requests';
COMMENT ON TABLE validation_results IS 'Validation results with pass/fail status';
COMMENT ON TABLE stage_results IS 'Results from individual validation stages';
COMMENT ON TABLE validation_issues IS 'Issues found during validation';
COMMENT ON TABLE batch_summaries IS 'Summary of batch validation runs';
COMMENT ON TABLE queue_stats_history IS 'Historical queue statistics for monitoring';
