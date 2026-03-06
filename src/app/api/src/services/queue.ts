import { createClient, RedisClientType } from 'redis';

// Redis key patterns (must match Python QueueManager)
const KEY_QUEUE_PREFIX = 'validation:queue:priority:';
const KEY_JOB_PREFIX = 'validation:job:';
const KEY_DEAD_LETTER = 'validation:queue:dead_letter';

// Priority mapping
const PRIORITY_MAP: Record<number, { name: string; description: string }> = {
  0: { name: 'CRITICAL', description: 'Hotfix, Production' },
  1: { name: 'MANUAL', description: 'PR Reviews, Manual' },
  2: { name: 'CI_CD', description: 'Pipelines, Feature branches' },
  3: { name: 'BATCH', description: 'Scheduled, Bulk scans' },
};

export interface ValidationIssue {
  stage: string;
  severity: string;
  message: string;
  line?: number;
  column?: number;
  suggestion?: string;
}

export interface ValidationStage {
  stage: string;
  passed: boolean;
  duration_seconds: number;
  issues: ValidationIssue[];
  details: Record<string, unknown>;
}

export interface ValidationResult {
  job_path: string;
  passed: boolean;
  stages: ValidationStage[];
  started_at: string;
  completed_at: string;
  error?: string;
}

export interface QueueJob {
  id: string;
  jobPath: string;
  status: string;
  priority: string;
  triggerSource: string;
  branch?: string;
  commitSha?: string;
  triggeredBy?: string;
  createdAt: string;
  queuedAt?: string;
  startedAt?: string;
  completedAt?: string;
  workerId?: string;
  errorMessage?: string;
  age?: number;
  result?: ValidationResult;
}

export interface QueueMetrics {
  totalPending: number;
  pendingByPriority: Record<string, number>;
  backpressureLevel: string;
  activeWorkers: number;
  totalWorkers: number;
  oldestPendingAge: number;
  oldestPendingPriority?: string;
}

export interface PriorityQueueInfo {
  priority: string;
  name: string;
  description: string;
  pendingCount: number;
  jobs: QueueJob[];
}

class QueueService {
  private redis: RedisClientType | null = null;
  private redisUrl: string;
  private maxWorkers: number;

  constructor() {
    this.redisUrl = process.env.REDIS_URL || 'redis://localhost:6379/0';
    this.maxWorkers = parseInt(process.env.MAX_WORKERS || '5', 10);
  }

  private async getRedis(): Promise<RedisClientType> {
    if (!this.redis || !this.redis.isOpen) {
      this.redis = createClient({ url: this.redisUrl });
      this.redis.on('error', (err) => console.error('Redis Client Error', err));
      await this.redis.connect();
    }
    return this.redis;
  }

  private formatJob(jobId: string, data: Record<string, string>, includeResult = false): QueueJob {
    const createdAt = data.created_at || new Date().toISOString();
    const queuedAt = data.queued_at;
    const age = queuedAt ? Math.floor((Date.now() - new Date(queuedAt).getTime()) / 1000) : 0;

    let result: ValidationResult | undefined;
    if (includeResult && data.result) {
      try {
        result = JSON.parse(data.result) as ValidationResult;
      } catch {
        // Ignore parse errors
      }
    }

    return {
      id: jobId,
      jobPath: data.job_path || '',
      status: (data.status || 'pending').toLowerCase(),
      priority: `P${data.priority || '2'}`,
      triggerSource: this.mapTriggerSource(data.trigger_source),
      branch: data.branch || undefined,
      commitSha: data.commit_sha ? data.commit_sha.substring(0, 7) : undefined,
      triggeredBy: data.triggered_by || undefined,
      createdAt,
      queuedAt,
      startedAt: data.started_at || undefined,
      completedAt: data.completed_at || undefined,
      workerId: data.worker_id || undefined,
      errorMessage: data.error_message || data.error || undefined,
      age,
      result,
    };
  }

  private mapTriggerSource(source: string): string {
    // Normalize to lowercase first (Redis stores uppercase values)
    const normalizedSource = (source || '').toLowerCase();
    const mapping: Record<string, string> = {
      'git_webhook': 'webhook',
      'file_watcher': 'file',
      'ci_cd': 'ci_cd',
      'manual': 'manual',
      'scheduled': 'scheduled',
    };
    return mapping[normalizedSource] || normalizedSource || 'manual';
  }

  async getQueueMetrics(): Promise<QueueMetrics> {
    const redis = await this.getRedis();
    const pendingByPriority: Record<string, number> = { P0: 0, P1: 0, P2: 0, P3: 0 };
    let totalPending = 0;
    let oldestPendingAge = 0;
    let oldestPendingPriority: string | undefined;

    // Get pending counts and oldest job per priority
    for (let p = 0; p <= 3; p++) {
      const queueKey = `${KEY_QUEUE_PREFIX}${p}`;
      const count = await redis.zCard(queueKey);
      pendingByPriority[`P${p}`] = count;
      totalPending += count;

      // Check oldest in this queue
      const oldest = await redis.zRange(queueKey, 0, 0);
      if (oldest.length > 0) {
        const score = await redis.zScore(queueKey, oldest[0]);
        if (score) {
          const age = Math.floor(Date.now() / 1000 - score);
          if (age > oldestPendingAge) {
            oldestPendingAge = age;
            oldestPendingPriority = `P${p}`;
          }
        }
      }
    }

    // Count active workers (jobs with status=running)
    let activeWorkers = 0;
    const jobKeys = await redis.keys(`${KEY_JOB_PREFIX}*`);
    for (const key of jobKeys) {
      const status = await redis.hGet(key, 'status');
      if (status === 'running') {
        activeWorkers++;
      }
    }

    // Determine backpressure level
    let backpressureLevel = 'normal';
    if (totalPending >= 200) backpressureLevel = 'rejecting';
    else if (totalPending >= 100) backpressureLevel = 'critical';
    else if (totalPending >= 50) backpressureLevel = 'warning';

    return {
      totalPending,
      pendingByPriority,
      backpressureLevel,
      activeWorkers,
      totalWorkers: this.maxWorkers,
      oldestPendingAge,
      oldestPendingPriority,
    };
  }

  async getPriorityQueues(): Promise<PriorityQueueInfo[]> {
    const redis = await this.getRedis();
    const queues: PriorityQueueInfo[] = [];

    for (let p = 0; p <= 3; p++) {
      const queueKey = `${KEY_QUEUE_PREFIX}${p}`;
      const jobIds = await redis.zRange(queueKey, 0, 19); // Get first 20 jobs
      const jobs: QueueJob[] = [];

      for (const jobId of jobIds) {
        const jobKey = `${KEY_JOB_PREFIX}${jobId}`;
        const jobData = await redis.hGetAll(jobKey);
        if (jobData && Object.keys(jobData).length > 0) {
          jobs.push(this.formatJob(jobId, jobData));
        }
      }

      const count = await redis.zCard(queueKey);
      const info = PRIORITY_MAP[p];

      queues.push({
        priority: `P${p}`,
        name: info.name,
        description: info.description,
        pendingCount: count,
        jobs,
      });
    }

    return queues;
  }

  async getAllJobs(limit: number = 100): Promise<QueueJob[]> {
    const redis = await this.getRedis();
    const jobs: QueueJob[] = [];

    // Get all job keys
    const jobKeys = await redis.keys(`${KEY_JOB_PREFIX}*`);

    for (const key of jobKeys.slice(0, limit)) {
      const jobId = key.replace(KEY_JOB_PREFIX, '');
      const jobData = await redis.hGetAll(key);
      if (jobData && Object.keys(jobData).length > 0) {
        jobs.push(this.formatJob(jobId, jobData));
      }
    }

    // Sort by created_at descending
    jobs.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

    return jobs;
  }

  async getDeadLetterJobs(limit: number = 50): Promise<QueueJob[]> {
    const redis = await this.getRedis();
    const jobIds = await redis.zRange(KEY_DEAD_LETTER, 0, limit - 1, { REV: true });
    const jobs: QueueJob[] = [];

    for (const jobId of jobIds) {
      const jobKey = `${KEY_JOB_PREFIX}${jobId}`;
      const jobData = await redis.hGetAll(jobKey);
      if (jobData && Object.keys(jobData).length > 0) {
        jobs.push(this.formatJob(jobId, jobData));
      }
    }

    return jobs;
  }

  async getJobById(jobId: string, includeResult = true): Promise<QueueJob | null> {
    const redis = await this.getRedis();
    const jobKey = `${KEY_JOB_PREFIX}${jobId}`;
    const jobData = await redis.hGetAll(jobKey);

    if (!jobData || Object.keys(jobData).length === 0) {
      return null;
    }

    return this.formatJob(jobId, jobData, includeResult);
  }

  async getValidationHistory(limit: number = 100): Promise<QueueJob[]> {
    const redis = await this.getRedis();
    const jobs: QueueJob[] = [];

    // Get all job keys
    const jobKeys = await redis.keys(`${KEY_JOB_PREFIX}*`);

    for (const key of jobKeys) {
      const jobId = key.replace(KEY_JOB_PREFIX, '');
      const jobData = await redis.hGetAll(key);
      if (jobData && Object.keys(jobData).length > 0) {
        // Only include completed/failed jobs with results
        const status = jobData.status?.toUpperCase();
        if (status === 'COMPLETED' || status === 'FAILED' || status === 'ERROR') {
          jobs.push(this.formatJob(jobId, jobData, true));
        }
      }
    }

    // Sort by completed_at or created_at descending
    jobs.sort((a, b) => {
      const dateA = new Date(a.completedAt || a.createdAt).getTime();
      const dateB = new Date(b.completedAt || b.createdAt).getTime();
      return dateB - dateA;
    });

    return jobs.slice(0, limit);
  }

  async retryJob(jobId: string): Promise<boolean> {
    const redis = await this.getRedis();

    // Remove from dead letter
    const removed = await redis.zRem(KEY_DEAD_LETTER, jobId);
    if (!removed) {
      return false;
    }

    // Get job data
    const jobKey = `${KEY_JOB_PREFIX}${jobId}`;
    const jobData = await redis.hGetAll(jobKey);
    if (!jobData) {
      return false;
    }

    // Reset status
    await redis.hSet(jobKey, {
      status: 'pending',
    });
    await redis.hDel(jobKey, ['error_message', 'started_at', 'completed_at', 'worker_id']);

    // Add back to queue
    const priority = parseInt(jobData.priority || '2', 10);
    const queueKey = `${KEY_QUEUE_PREFIX}${priority}`;
    await redis.zAdd(queueKey, { score: Date.now() / 1000, value: jobId });

    return true;
  }

  async deleteJob(jobId: string): Promise<boolean> {
    const redis = await this.getRedis();

    // Remove from dead letter queue
    await redis.zRem(KEY_DEAD_LETTER, jobId);

    // Remove from all priority queues
    for (let p = 0; p <= 3; p++) {
      await redis.zRem(`${KEY_QUEUE_PREFIX}${p}`, jobId);
    }

    // Delete job data
    const jobKey = `${KEY_JOB_PREFIX}${jobId}`;
    const deleted = await redis.del(jobKey);

    return deleted > 0;
  }

  async close(): Promise<void> {
    if (this.redis) {
      await this.redis.quit();
      this.redis = null;
    }
  }
}

export const queueService = new QueueService();
