// Queue Monitoring Types

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'error';

export type Priority = 'P0' | 'P1' | 'P2' | 'P3';

export type TriggerSource = 'webhook' | 'file' | 'manual' | 'scheduled' | 'ci_cd';

export type BackpressureLevel = 'normal' | 'warning' | 'critical' | 'rejecting';

export interface QueueJob {
  id: string;
  jobPath: string;
  status: JobStatus;
  priority: Priority;
  triggerSource: TriggerSource;
  branch?: string;
  commitSha?: string;
  triggeredBy?: string;
  createdAt: string;
  queuedAt?: string;
  startedAt?: string;
  completedAt?: string;
  workerId?: string;
  duration?: number; // milliseconds
  age?: number; // seconds
  errorMessage?: string;
  metadata?: Record<string, unknown>;
  validationResults?: ValidationStageResult[];
  logs?: LogEntry[];
}

export interface ValidationStageResult {
  stage: 'syntax' | 'logic' | 'data' | 'ai_analysis';
  status: 'passed' | 'failed' | 'warning' | 'skipped';
  duration: number; // milliseconds
  issueCount: number;
  issues: ValidationIssue[];
}

export interface ValidationIssue {
  id: string;
  severity: 'critical' | 'error' | 'warning' | 'info';
  message: string;
  fileLocation?: string;
  lineNumber?: number;
  suggestedFix?: string;
}

// Backend validation result types (from executor_runner)
export interface ExecutorValidationIssue {
  stage: string;
  severity: 'ERROR' | 'WARNING' | 'INFO';
  message: string;
  line?: number;
  column?: number;
  suggestion?: string;
}

export interface ExecutorValidationStage {
  stage: string;
  passed: boolean;
  duration_seconds: number;
  issues: ExecutorValidationIssue[];
  details: Record<string, unknown>;
}

export interface ExecutorValidationResult {
  job_path: string;
  passed: boolean;
  stages: ExecutorValidationStage[];
  started_at: string;
  completed_at: string;
  error?: string;
}

export interface ValidationJob extends QueueJob {
  result?: ExecutorValidationResult;
}

export interface LogEntry {
  timestamp: string;
  level: 'debug' | 'info' | 'warn' | 'error';
  message: string;
}

export interface QueueMetrics {
  totalPending: number;
  pendingByPriority: Record<Priority, number>;
  backpressureLevel: BackpressureLevel;
  activeWorkers: number;
  totalWorkers: number;
  throughput: number; // jobs per minute
  throughputHistory: number[]; // last 30 data points
  throughputVsAverage: number; // percentage
  oldestPendingAge: number; // seconds
  oldestPendingPriority?: Priority;
}

export interface RateLimit {
  priority: Priority;
  currentRate: number;
  limit: number;
  window: number; // seconds
  remainingTokens: number;
  resetIn: number; // seconds
  enabled: boolean;
}

export interface PriorityQueueInfo {
  priority: Priority;
  name: string;
  description: string;
  color: string;
  badgeColor: string;
  pendingCount: number;
  rateLimit: RateLimit;
  jobs: QueueJob[];
}
