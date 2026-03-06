/**
 * Classification Service
 *
 * Handles job classification operations including batch processing.
 * Communicates with Python backend via Redis for batch operations.
 */

import pg from 'pg';
import { createClient, RedisClientType } from 'redis';
import {
  JobClassification,
  ClassificationBatch,
  BatchStatus,
  StartBatchRequest,
  BatchStatusResponse,
  ClassificationStats,
  ClassificationFilters,
  Complexity,
  ComplexityMetrics,
  TreeNode,
} from '../types/classification.js';

const { Pool } = pg;

// Redis key patterns for classification
const BATCH_QUEUE_KEY = 'classification:batch:queue';
const BATCH_STATUS_PREFIX = 'classification:batch:status:';

class ClassifyService {
  private pool: pg.Pool | null = null;
  private redis: RedisClientType | null = null;
  private redisUrl: string;

  constructor() {
    this.redisUrl = process.env.REDIS_URL || 'redis://localhost:6379/0';
    this.initPool();
  }

  private initPool(): void {
    this.pool = new Pool({
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
      database: process.env.POSTGRES_DB || 'validation_results',
      user: process.env.POSTGRES_USER || 'postgres',
      password: process.env.POSTGRES_PASSWORD || '',
      max: 10,
    });
  }

  private getPool(): pg.Pool {
    if (!this.pool) {
      this.initPool();
    }
    return this.pool!;
  }

  private async getRedis(): Promise<RedisClientType> {
    if (!this.redis || !this.redis.isOpen) {
      this.redis = createClient({ url: this.redisUrl });
      this.redis.on('error', (err) => console.error('Redis Client Error', err));
      await this.redis.connect();
    }
    return this.redis;
  }

  // =========================================================================
  // Classification Queries
  // =========================================================================

  async getClassifications(
    filters: ClassificationFilters,
    page = 1,
    limit = 20
  ): Promise<{ data: JobClassification[]; total: number }> {
    const offset = (page - 1) * limit;
    let query = `
      SELECT jc.*, d.name as domain_name, m.name as module_name, jg.name as job_group_name
      FROM job_classifications jc
      LEFT JOIN categories d ON jc.domain_id = d.id
      LEFT JOIN categories m ON jc.module_id = m.id
      LEFT JOIN categories jg ON jc.job_group_id = jg.id
      WHERE 1=1
    `;
    let countQuery = 'SELECT COUNT(*) FROM job_classifications jc WHERE 1=1';
    const params: (string | boolean)[] = [];
    let paramCount = 0;

    if (filters.domainId) {
      paramCount++;
      query += ` AND jc.domain_id = $${paramCount}`;
      countQuery += ` AND jc.domain_id = $${paramCount}`;
      params.push(filters.domainId);
    }

    if (filters.moduleId) {
      paramCount++;
      query += ` AND jc.module_id = $${paramCount}`;
      countQuery += ` AND jc.module_id = $${paramCount}`;
      params.push(filters.moduleId);
    }

    if (filters.jobGroupId) {
      paramCount++;
      query += ` AND jc.job_group_id = $${paramCount}`;
      countQuery += ` AND jc.job_group_id = $${paramCount}`;
      params.push(filters.jobGroupId);
    }

    if (filters.complexity) {
      paramCount++;
      query += ` AND jc.complexity = $${paramCount}`;
      countQuery += ` AND jc.complexity = $${paramCount}`;
      params.push(filters.complexity);
    }

    if (filters.batchId) {
      paramCount++;
      query += ` AND jc.batch_id = $${paramCount}`;
      countQuery += ` AND jc.batch_id = $${paramCount}`;
      params.push(filters.batchId);
    }

    if (filters.hasSuggestions !== undefined) {
      if (filters.hasSuggestions) {
        query += ' AND (jc.suggested_domain IS NOT NULL OR jc.suggested_module IS NOT NULL OR jc.suggested_job_group IS NOT NULL)';
        countQuery += ' AND (jc.suggested_domain IS NOT NULL OR jc.suggested_module IS NOT NULL OR jc.suggested_job_group IS NOT NULL)';
      } else {
        query += ' AND jc.suggested_domain IS NULL AND jc.suggested_module IS NULL AND jc.suggested_job_group IS NULL';
        countQuery += ' AND jc.suggested_domain IS NULL AND jc.suggested_module IS NULL AND jc.suggested_job_group IS NULL';
      }
    }

    if (filters.search) {
      paramCount++;
      query += ` AND (jc.job_path ILIKE $${paramCount} OR jc.job_name ILIKE $${paramCount})`;
      countQuery += ` AND (jc.job_path ILIKE $${paramCount} OR jc.job_name ILIKE $${paramCount})`;
      params.push(`%${filters.search}%`);
    }

    // Add pagination
    const countParams = [...params];
    query += ` ORDER BY jc.classified_at DESC LIMIT $${paramCount + 1} OFFSET $${paramCount + 2}`;
    params.push(limit.toString(), offset.toString());

    const pool = this.getPool();
    const [dataResult, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, countParams),
    ]);

    return {
      data: dataResult.rows.map(this.rowToClassification),
      total: parseInt(countResult.rows[0].count, 10),
    };
  }

  async getClassificationByPath(jobPath: string): Promise<JobClassification | null> {
    const result = await this.getPool().query(
      `SELECT jc.*, d.name as domain_name, m.name as module_name, jg.name as job_group_name
       FROM job_classifications jc
       LEFT JOIN categories d ON jc.domain_id = d.id
       LEFT JOIN categories m ON jc.module_id = m.id
       LEFT JOIN categories jg ON jc.job_group_id = jg.id
       WHERE jc.job_path = $1`,
      [jobPath]
    );
    return result.rows.length > 0 ? this.rowToClassification(result.rows[0]) : null;
  }

  // =========================================================================
  // Batch Operations
  // =========================================================================

  async startBatch(request: StartBatchRequest, triggeredBy?: string): Promise<ClassificationBatch> {
    const pool = this.getPool();
    const redis = await this.getRedis();

    // Create batch record
    const result = await pool.query(
      `INSERT INTO classification_batches
       (name, status, directories, file_patterns, exclude_patterns, ai_provider, triggered_by, options)
       VALUES ($1, 'pending', $2, $3, $4, $5, $6, $7)
       RETURNING *`,
      [
        request.name || `Batch ${new Date().toISOString()}`,
        JSON.stringify(request.directories),
        JSON.stringify(request.filePatterns || ['*.py']),
        JSON.stringify(request.excludePatterns || []),
        request.aiProvider || 'ollama',
        triggeredBy || 'api',
        JSON.stringify({ forceReclassify: request.forceReclassify || false }),
      ]
    );

    const batch = this.rowToBatch(result.rows[0]);

    // Queue batch for Python processor
    await redis.lPush(BATCH_QUEUE_KEY, JSON.stringify({
      batchId: batch.id,
      directories: request.directories,
      filePatterns: request.filePatterns || ['*.py'],
      excludePatterns: request.excludePatterns || [],
      aiProvider: request.aiProvider || 'ollama',
      forceReclassify: request.forceReclassify || false,
    }));

    return batch;
  }

  async getBatchStatus(batchId: string): Promise<BatchStatusResponse | null> {
    const pool = this.getPool();

    const result = await pool.query(
      'SELECT * FROM classification_batches WHERE id = $1',
      [batchId]
    );

    if (result.rows.length === 0) {
      return null;
    }

    const batch = this.rowToBatch(result.rows[0]);

    // Calculate estimated time remaining
    let estimatedTimeRemaining: number | undefined;
    if (batch.status === 'running' && batch.startedAt && batch.processedJobs > 0) {
      const elapsed = Date.now() - new Date(batch.startedAt).getTime();
      const avgTimePerJob = elapsed / batch.processedJobs;
      const remainingJobs = batch.totalJobs - batch.processedJobs;
      estimatedTimeRemaining = Math.round((avgTimePerJob * remainingJobs) / 1000);
    }

    return {
      id: batch.id,
      name: batch.name,
      status: batch.status,
      totalJobs: batch.totalJobs,
      processedJobs: batch.processedJobs,
      successfulJobs: batch.successfulJobs,
      failedJobs: batch.failedJobs,
      skippedJobs: batch.skippedJobs,
      progressPercent: batch.progressPercent,
      startedAt: batch.startedAt,
      estimatedTimeRemaining,
      isRunning: batch.status === 'running',
      isCancelled: batch.status === 'cancelled',
    };
  }

  async cancelBatch(batchId: string): Promise<boolean> {
    const redis = await this.getRedis();

    // Signal cancellation via Redis
    await redis.set(`${BATCH_STATUS_PREFIX}${batchId}:cancel`, '1', { EX: 3600 });

    // Update status in database
    const result = await this.getPool().query(
      `UPDATE classification_batches
       SET status = 'cancelled', completed_at = NOW()
       WHERE id = $1 AND status = 'running'`,
      [batchId]
    );

    return (result.rowCount ?? 0) > 0;
  }

  async getRecentBatches(limit = 20): Promise<ClassificationBatch[]> {
    const result = await this.getPool().query(
      'SELECT * FROM classification_batches ORDER BY created_at DESC LIMIT $1',
      [limit]
    );
    return result.rows.map(this.rowToBatch);
  }

  async deleteBatch(batchId: string): Promise<boolean> {
    const pool = this.getPool();

    // Delete associated classifications first
    await pool.query('DELETE FROM job_classifications WHERE batch_id = $1', [batchId]);

    // Delete batch
    const result = await pool.query('DELETE FROM classification_batches WHERE id = $1', [batchId]);
    return (result.rowCount ?? 0) > 0;
  }

  // =========================================================================
  // Tree Structure for UI
  // =========================================================================

  /**
   * Get jobs organized in a tree hierarchy: Domain > Module > Job Group > Job
   */
  async getJobTree(): Promise<TreeNode[]> {
    const pool = this.getPool();

    // Get all domains with their modules, job groups, and jobs
    const result = await pool.query(`
      WITH job_data AS (
        SELECT
          jc.id as job_id,
          jc.job_path,
          jc.job_name,
          jc.complexity,
          jc.complexity_score,
          jc.confidence_score,
          d.id as domain_id,
          d.name as domain_name,
          d.description as domain_desc,
          m.id as module_id,
          m.name as module_name,
          m.description as module_desc,
          jg.id as job_group_id,
          jg.name as job_group_name,
          jg.description as job_group_desc
        FROM job_classifications jc
        LEFT JOIN categories d ON jc.domain_id = d.id
        LEFT JOIN categories m ON jc.module_id = m.id
        LEFT JOIN categories jg ON jc.job_group_id = jg.id
      )
      SELECT * FROM job_data ORDER BY domain_name, module_name, job_group_name, job_name
    `);

    // Build tree structure
    const domainMap = new Map<string, TreeNode>();

    for (const row of result.rows) {
      const domainId = row.domain_id || 'unclassified';
      const domainName = row.domain_name || 'Unclassified';
      const moduleId = row.module_id || 'unknown-module';
      const moduleName = row.module_name || 'Unknown Module';
      const jobGroupId = row.job_group_id || 'unknown-group';
      const jobGroupName = row.job_group_name || 'Unknown Group';

      // Get or create domain node
      if (!domainMap.has(domainId)) {
        domainMap.set(domainId, {
          id: domainId,
          name: domainName,
          type: 'domain',
          description: row.domain_desc,
          children: [],
          metadata: { totalJobs: 0, completedJobs: 0, runningJobs: 0, failedJobs: 0 },
        });
      }
      const domainNode = domainMap.get(domainId)!;

      // Get or create module node
      let moduleNode = domainNode.children!.find(c => c.id === moduleId);
      if (!moduleNode) {
        moduleNode = {
          id: moduleId,
          name: moduleName,
          type: 'module',
          description: row.module_desc,
          children: [],
          metadata: { totalJobs: 0, completedJobs: 0, runningJobs: 0, failedJobs: 0 },
        };
        domainNode.children!.push(moduleNode);
      }

      // Get or create job group node
      let jobGroupNode = moduleNode.children!.find(c => c.id === jobGroupId);
      if (!jobGroupNode) {
        jobGroupNode = {
          id: jobGroupId,
          name: jobGroupName,
          type: 'job-group',
          description: row.job_group_desc,
          children: [],
          metadata: { totalJobs: 0, completedJobs: 0, runningJobs: 0, failedJobs: 0 },
        };
        moduleNode.children!.push(jobGroupNode);
      }

      // Add job node
      const jobNode: TreeNode = {
        id: row.job_id,
        name: row.job_name,
        type: 'job',
        jobId: row.job_id,
        jobPath: row.job_path,
        complexity: row.complexity,
        complexityScore: row.complexity_score,
        confidenceScore: row.confidence_score,
      };
      jobGroupNode.children!.push(jobNode);

      // Update metadata counts
      domainNode.metadata!.totalJobs++;
      moduleNode.metadata!.totalJobs++;
      jobGroupNode.metadata!.totalJobs++;
    }

    return Array.from(domainMap.values());
  }

  // =========================================================================
  // Statistics
  // =========================================================================

  async getStats(): Promise<ClassificationStats> {
    const pool = this.getPool();

    const [totalResult, complexityResult, domainResult, suggestionsResult, batchResult] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM job_classifications'),
      pool.query("SELECT complexity, COUNT(*) as count FROM job_classifications WHERE complexity IS NOT NULL GROUP BY complexity"),
      pool.query(`
        SELECT c.name, COUNT(*) as count
        FROM job_classifications jc
        LEFT JOIN categories c ON jc.domain_id = c.id
        GROUP BY c.name
        ORDER BY count DESC
        LIMIT 10
      `),
      pool.query("SELECT COUNT(*) FROM suggested_categories WHERE status = 'pending'"),
      pool.query('SELECT * FROM classification_batches ORDER BY created_at DESC LIMIT 1'),
    ]);

    const byComplexity: Record<Complexity, number> = {
      low: 0,
      medium: 0,
      high: 0,
    };

    for (const row of complexityResult.rows) {
      if (row.complexity) {
        byComplexity[row.complexity as Complexity] = parseInt(row.count, 10);
      }
    }

    const byDomain: Record<string, number> = {};
    for (const row of domainResult.rows) {
      byDomain[row.name || 'Unclassified'] = parseInt(row.count, 10);
    }

    return {
      totalClassifications: parseInt(totalResult.rows[0].count, 10),
      byComplexity,
      byDomain,
      pendingSuggestions: parseInt(suggestionsResult.rows[0].count, 10),
      recentBatch: batchResult.rows.length > 0 ? this.rowToBatch(batchResult.rows[0]) : undefined,
    };
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private rowToClassification(row: Record<string, unknown>): JobClassification {
    const metrics = row.metrics as Record<string, number> | null;

    return {
      id: row.id as string,
      jobPath: row.job_path as string,
      jobName: row.job_name as string,
      domainId: row.domain_id as string | undefined,
      moduleId: row.module_id as string | undefined,
      jobGroupId: row.job_group_id as string | undefined,
      domainName: row.domain_name as string | undefined,
      moduleName: row.module_name as string | undefined,
      jobGroupName: row.job_group_name as string | undefined,
      complexity: row.complexity as Complexity | undefined,
      complexityScore: row.complexity_score as number | undefined,
      complexityReasoning: row.complexity_reasoning as string | undefined,
      metrics: metrics ? {
        linesOfCode: metrics.lines_of_code || 0,
        sqlQueries: metrics.sql_queries || 0,
        joins: metrics.joins || 0,
        aggregations: metrics.aggregations || 0,
        transformations: metrics.transformations || 0,
        dataframes: metrics.dataframes || 0,
        sourceTables: metrics.source_tables || 0,
        targetTables: metrics.target_tables || 0,
        udfs: metrics.udfs || 0,
      } : undefined,
      confidenceScore: row.confidence_score as number | undefined,
      suggestedDomain: row.suggested_domain as string | undefined,
      suggestedModule: row.suggested_module as string | undefined,
      suggestedJobGroup: row.suggested_job_group as string | undefined,
      aiProvider: row.ai_provider as string | undefined,
      aiModel: row.ai_model as string | undefined,
      batchId: row.batch_id as string | undefined,
      classifiedAt: (row.classified_at as Date).toISOString(),
      updatedAt: (row.updated_at as Date).toISOString(),
      hasSuggestions: !!(row.suggested_domain || row.suggested_module || row.suggested_job_group),
    };
  }

  private rowToBatch(row: Record<string, unknown>): ClassificationBatch {
    const directories = typeof row.directories === 'string'
      ? JSON.parse(row.directories)
      : row.directories as string[];

    const filePatterns = typeof row.file_patterns === 'string'
      ? JSON.parse(row.file_patterns)
      : (row.file_patterns as string[]) || ['*.py'];

    const excludePatterns = typeof row.exclude_patterns === 'string'
      ? JSON.parse(row.exclude_patterns)
      : (row.exclude_patterns as string[]) || [];

    const totalJobs = (row.total_jobs as number) || 0;
    const processedJobs = (row.processed_jobs as number) || 0;

    return {
      id: row.id as string,
      name: row.name as string | undefined,
      status: row.status as BatchStatus,
      directories,
      filePatterns,
      excludePatterns,
      totalJobs,
      processedJobs,
      successfulJobs: (row.successful_jobs as number) || 0,
      failedJobs: (row.failed_jobs as number) || 0,
      skippedJobs: (row.skipped_jobs as number) || 0,
      progressPercent: totalJobs > 0 ? Math.round((processedJobs / totalJobs) * 100) : 0,
      aiProvider: row.ai_provider as string | undefined,
      aiModel: row.ai_model as string | undefined,
      createdAt: (row.created_at as Date).toISOString(),
      startedAt: row.started_at ? (row.started_at as Date).toISOString() : undefined,
      completedAt: row.completed_at ? (row.completed_at as Date).toISOString() : undefined,
      errorMessage: row.error_message as string | undefined,
      triggeredBy: row.triggered_by as string | undefined,
    };
  }

  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
    if (this.redis) {
      await this.redis.quit();
      this.redis = null;
    }
  }
}

export const classifyService = new ClassifyService();
