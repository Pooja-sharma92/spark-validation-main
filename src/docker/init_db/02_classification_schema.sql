-- Migration: 003_classification_tables
-- Description: AI Job Classification Module tables
-- Created: 2024-01-02

-- ============================================================================
-- Categories Table
-- Stores domain, module, and job_group categories in a hierarchical structure
-- ============================================================================
CREATE TABLE IF NOT EXISTS categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(50) NOT NULL CHECK (type IN ('domain', 'module', 'job_group')),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_id UUID REFERENCES categories(id) ON DELETE CASCADE,
    ai_discovered BOOLEAN DEFAULT FALSE,
    approved BOOLEAN DEFAULT TRUE,
    approved_by VARCHAR(255),
    approved_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (type, name, parent_id)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_id);
CREATE INDEX IF NOT EXISTS idx_categories_type ON categories(type);
CREATE INDEX IF NOT EXISTS idx_categories_approved ON categories(approved) WHERE approved = FALSE;
CREATE INDEX IF NOT EXISTS idx_categories_type_name ON categories(type, name);

-- ============================================================================
-- Job Classifications Table
-- Stores classification results for each Spark job
-- ============================================================================
CREATE TABLE IF NOT EXISTS job_classifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_path VARCHAR(1024) NOT NULL,
    job_name VARCHAR(255) NOT NULL,

    -- Classification results (linked to categories)
    domain_id UUID REFERENCES categories(id) ON DELETE SET NULL,
    module_id UUID REFERENCES categories(id) ON DELETE SET NULL,
    job_group_id UUID REFERENCES categories(id) ON DELETE SET NULL,

    -- Complexity assessment
    complexity VARCHAR(20) CHECK (complexity IN ('low', 'medium', 'high')),
    complexity_score INTEGER CHECK (complexity_score >= 0 AND complexity_score <= 100),
    complexity_reasoning TEXT,

    -- Complexity metrics (for detailed analysis)
    metrics JSONB DEFAULT '{}',
    -- Example: {"joins": 5, "aggregations": 2, "sql_queries": 6, "lines_of_code": 350, "dataframes": 8}

    -- AI confidence
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),

    -- AI suggestions for new categories (when no existing category matches)
    suggested_domain VARCHAR(255),
    suggested_module VARCHAR(255),
    suggested_job_group VARCHAR(255),

    -- AI provider info
    ai_provider VARCHAR(50),
    ai_model VARCHAR(100),

    -- Batch reference
    batch_id UUID,

    -- Timestamps
    classified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Raw AI response for debugging/auditing
    raw_response JSONB,

    -- Unique constraint on job path
    UNIQUE (job_path)
);

-- Indexes for efficient filtering
CREATE INDEX IF NOT EXISTS idx_job_classifications_domain ON job_classifications(domain_id);
CREATE INDEX IF NOT EXISTS idx_job_classifications_module ON job_classifications(module_id);
CREATE INDEX IF NOT EXISTS idx_job_classifications_job_group ON job_classifications(job_group_id);
CREATE INDEX IF NOT EXISTS idx_job_classifications_complexity ON job_classifications(complexity);
CREATE INDEX IF NOT EXISTS idx_job_classifications_batch ON job_classifications(batch_id);
CREATE INDEX IF NOT EXISTS idx_job_classifications_confidence ON job_classifications(confidence_score);
CREATE INDEX IF NOT EXISTS idx_job_classifications_classified_at ON job_classifications(classified_at DESC);

-- ============================================================================
-- Classification Batches Table
-- Tracks batch classification runs
-- ============================================================================
CREATE TABLE IF NOT EXISTS classification_batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255),
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),

    -- Scope definition
    directories JSONB NOT NULL,  -- Array of directories to scan
    file_patterns JSONB DEFAULT '["*.py"]',
    exclude_patterns JSONB DEFAULT '[]',

    -- Progress tracking
    total_jobs INTEGER DEFAULT 0,
    processed_jobs INTEGER DEFAULT 0,
    successful_jobs INTEGER DEFAULT 0,
    failed_jobs INTEGER DEFAULT 0,
    skipped_jobs INTEGER DEFAULT 0,

    -- AI provider used
    ai_provider VARCHAR(50),
    ai_model VARCHAR(100),

    -- Timing
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Error tracking
    error_message TEXT,
    error_details JSONB,

    -- Audit
    triggered_by VARCHAR(255),

    -- Options
    options JSONB DEFAULT '{}'
    -- Example: {"force_reclassify": false, "include_tests": false}
);

CREATE INDEX IF NOT EXISTS idx_classification_batches_status ON classification_batches(status);
CREATE INDEX IF NOT EXISTS idx_classification_batches_created_at ON classification_batches(created_at DESC);

-- ============================================================================
-- Suggested Categories Table
-- Stores AI-suggested categories pending human approval
-- ============================================================================
CREATE TABLE IF NOT EXISTS suggested_categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(50) NOT NULL CHECK (type IN ('domain', 'module', 'job_group')),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_name VARCHAR(255),  -- For context (e.g., parent domain for a module suggestion)

    -- Jobs that suggested this category
    suggested_by_jobs JSONB DEFAULT '[]',  -- Array of job paths
    occurrence_count INTEGER DEFAULT 1,

    -- Timestamps
    first_suggested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_suggested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Review status
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'merged')),
    merged_into_id UUID REFERENCES categories(id) ON DELETE SET NULL,

    -- Reviewer info
    reviewed_by VARCHAR(255),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    review_notes TEXT,

    -- Unique on type + name + parent to prevent duplicates
    UNIQUE (type, name, parent_name)
);

CREATE INDEX IF NOT EXISTS idx_suggested_categories_status ON suggested_categories(status);
CREATE INDEX IF NOT EXISTS idx_suggested_categories_type ON suggested_categories(type);
CREATE INDEX IF NOT EXISTS idx_suggested_categories_occurrence ON suggested_categories(occurrence_count DESC);

-- ============================================================================
-- Classification History Table (Optional - for tracking changes)
-- ============================================================================
CREATE TABLE IF NOT EXISTS classification_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_classification_id UUID REFERENCES job_classifications(id) ON DELETE CASCADE,

    -- Previous values
    previous_domain_id UUID,
    previous_module_id UUID,
    previous_job_group_id UUID,
    previous_complexity VARCHAR(20),

    -- New values
    new_domain_id UUID,
    new_module_id UUID,
    new_job_group_id UUID,
    new_complexity VARCHAR(20),

    -- Change metadata
    change_reason VARCHAR(50) CHECK (change_reason IN ('reclassification', 'manual_override', 'category_merge')),
    changed_by VARCHAR(255),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_classification_history_job ON classification_history(job_classification_id);
CREATE INDEX IF NOT EXISTS idx_classification_history_changed_at ON classification_history(changed_at DESC);

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to categories
DROP TRIGGER IF EXISTS update_categories_updated_at ON categories;
CREATE TRIGGER update_categories_updated_at
    BEFORE UPDATE ON categories
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to job_classifications
DROP TRIGGER IF EXISTS update_job_classifications_updated_at ON job_classifications;
CREATE TRIGGER update_job_classifications_updated_at
    BEFORE UPDATE ON job_classifications
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Initial Seed Data (Common Domains)
-- ============================================================================
INSERT INTO categories (type, name, description, ai_discovered, approved) VALUES
    ('domain', 'Finance', 'Financial data processing, loans, accounts, transactions', FALSE, TRUE),
    ('domain', 'Customer', 'Customer data management, profiles, segmentation', FALSE, TRUE),
    ('domain', 'Risk', 'Risk assessment, compliance, fraud detection', FALSE, TRUE),
    ('domain', 'Marketing', 'Marketing analytics, campaigns, targeting', FALSE, TRUE),
    ('domain', 'Operations', 'Operational data, logistics, inventory', FALSE, TRUE)
ON CONFLICT (type, name, parent_id) DO NOTHING;

-- Insert common job groups
INSERT INTO categories (type, name, description, ai_discovered, approved) VALUES
    ('job_group', 'ETL', 'Extract-Transform-Load jobs', FALSE, TRUE),
    ('job_group', 'Analytics', 'Analytical processing and aggregations', FALSE, TRUE),
    ('job_group', 'Reporting', 'Report generation and data exports', FALSE, TRUE),
    ('job_group', 'Data Quality', 'Data validation and cleansing', FALSE, TRUE),
    ('job_group', 'Dimension Load', 'Dimension table loading', FALSE, TRUE),
    ('job_group', 'Fact Load', 'Fact table loading', FALSE, TRUE)
ON CONFLICT (type, name, parent_id) DO NOTHING;

-- ============================================================================
-- Views for easier querying
-- ============================================================================

-- View: Job classifications with category names
CREATE OR REPLACE VIEW v_job_classifications AS
SELECT
    jc.id,
    jc.job_path,
    jc.job_name,
    d.name AS domain_name,
    m.name AS module_name,
    jg.name AS job_group_name,
    jc.complexity,
    jc.complexity_score,
    jc.confidence_score,
    jc.suggested_domain,
    jc.suggested_module,
    jc.suggested_job_group,
    jc.ai_provider,
    jc.classified_at,
    jc.batch_id
FROM job_classifications jc
LEFT JOIN categories d ON jc.domain_id = d.id
LEFT JOIN categories m ON jc.module_id = m.id
LEFT JOIN categories jg ON jc.job_group_id = jg.id;

-- View: Category hierarchy
CREATE OR REPLACE VIEW v_category_tree AS
WITH RECURSIVE category_tree AS (
    -- Root categories (no parent)
    SELECT
        id,
        type,
        name,
        description,
        parent_id,
        approved,
        1 AS level,
        name::TEXT AS path
    FROM categories
    WHERE parent_id IS NULL

    UNION ALL

    -- Child categories
    SELECT
        c.id,
        c.type,
        c.name,
        c.description,
        c.parent_id,
        c.approved,
        ct.level + 1,
        ct.path || ' > ' || c.name
    FROM categories c
    INNER JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT * FROM category_tree
ORDER BY path;

-- View: Classification summary stats
CREATE OR REPLACE VIEW v_classification_summary AS
SELECT
    d.name AS domain,
    COUNT(DISTINCT jc.id) AS job_count,
    COUNT(DISTINCT CASE WHEN jc.complexity = 'low' THEN jc.id END) AS low_complexity,
    COUNT(DISTINCT CASE WHEN jc.complexity = 'medium' THEN jc.id END) AS medium_complexity,
    COUNT(DISTINCT CASE WHEN jc.complexity = 'high' THEN jc.id END) AS high_complexity,
    AVG(jc.confidence_score) AS avg_confidence
FROM job_classifications jc
LEFT JOIN categories d ON jc.domain_id = d.id
GROUP BY d.name
ORDER BY job_count DESC;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO validator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO validator;
