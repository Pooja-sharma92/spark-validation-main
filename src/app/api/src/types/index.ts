export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'validating';

export interface MigrationJob {
  id: string;
  name: string;
  sourceTable: string;
  targetTable: string;
  status: JobStatus;
  progress: number;
  sourceRowCount: number;
  targetRowCount: number;
  createdAt: string;
  updatedAt: string;
  domain?: string;
  module?: string;
  priority?: 'low' | 'medium' | 'high' | 'critical';
}

export interface ValidationResult {
  id: string;
  jobId: string;
  stepName: string;
  status: 'passed' | 'failed' | 'warning' | 'skipped';
  message?: string;
  timestamp: string;
}

export interface BatchJob {
  id: string;
  name: string;
  jobIds: string[];
  status: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
}

export interface DashboardStats {
  totalJobs: number;
  completedJobs: number;
  runningJobs: number;
  failedJobs: number;
  pendingJobs: number;
  validationPassRate: number;
  averageExecutionTime: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

export interface ApiError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}
