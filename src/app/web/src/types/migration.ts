export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'validating';

export type ValidationStepStatus = 'pending' | 'running' | 'passed' | 'failed' | 'skipped' | 'warning';

export interface ValidationStep {
  id: string;
  name: string;
  description: string;
  status: ValidationStepStatus;
  startTime?: string;
  endTime?: string;
  duration?: number;
  errors?: ValidationError[];
  warnings?: ValidationWarning[];
  progress?: number;
  metadata?: {
    recordsChecked?: number;
    issuesFound?: number;
    [key: string]: unknown;
  };
}

export interface ValidationError {
  id: string;
  severity: 'critical' | 'error' | 'warning';
  code: string;
  message: string;
  details?: string;
  location?: string;
  suggestion?: string;
  timestamp: string;
}

export interface ValidationWarning {
  id: string;
  code: string;
  message: string;
  details?: string;
  suggestion?: string;
}

export interface MigrationJob {
  id: string;
  name: string;
  sourceTable: string;
  targetTable: string;
  status: JobStatus;
  startTime?: string;
  endTime?: string;
  progress: number;
  sourceRowCount: number;
  targetRowCount: number;
  validatedRowCount: number;
  failedRowCount: number;
  mismatchCount: number;
  priority: 'high' | 'medium' | 'low';
  error?: string;
  domain: string;
  complexity: 'simple' | 'medium' | 'complex' | 'critical';
  estimatedDuration?: number;
  actualDuration?: number;
  owner?: string;
  tags?: string[];
  dependencies?: string[];
  sparkJobId?: string;
  validationSteps: ValidationStep[];
  performance?: {
    rowsPerSecond?: number;
    cpuUsage?: number;
    memoryUsage?: number;
    ioWaitTime?: number;
  };
  errors?: ValidationError[];
  warnings?: ValidationWarning[];
  metadata?: {
    lastModified?: string;
    retryCount?: number;
    executionNode?: string;
    configHash?: string;
    [key: string]: unknown;
  };
  executionHistory?: JobExecutionRecord[];
  sourceCode?: JobSourceCode;
}

export interface JobExecutionRecord {
  id: string;
  executionNumber: number;
  startTime: string;
  endTime?: string;
  status: JobStatus;
  duration?: number;
  triggeredBy: string;
  triggerReason: 'manual' | 'scheduled' | 'retry' | 'dependency';
  validationSteps: ValidationStep[];
  performance?: {
    rowsPerSecond?: number;
    cpuUsage?: number;
    memoryUsage?: number;
    ioWaitTime?: number;
  };
  // Additional fields for validation results
  rowsProcessed?: number;
  rowsRejected?: number;
  version?: string;
  errorMessage?: string;
  performanceMetrics?: {
    cpuUsage: number;
    memoryUsage: number;
    diskIO: number;
    networkIO: number;
  };
  dataStats: {
    sourceRowCount: number;
    targetRowCount: number;
    validatedRowCount: number;
    failedRowCount: number;
    mismatchCount: number;
  };
  errors?: ValidationError[];
  warnings?: ValidationWarning[];
  environment: {
    executionNode: string;
    sparkJobId?: string;
    configHash: string;
    sparkVersion?: string;
  };
  changesSinceLastRun?: string[];
}

export interface JobSourceCode {
  sql?: string;
  python?: string;
  scala?: string;
  config?: string;
  language: 'sql' | 'python' | 'scala' | 'mixed';
  lastModified: string;
  modifiedBy: string;
  version: string;
  repositoryUrl?: string;
  commitHash?: string;
}

export interface DataValidation {
  jobId: string;
  tableName: string;
  totalRows: number;
  validatedRows: number;
  matchedRows: number;
  mismatchedRows: number;
  missingRows: number;
  extraRows: number;
  fieldMismatches: FieldMismatch[];
}

export interface FieldMismatch {
  rowId: string;
  fieldName: string;
  sourceValue: unknown;
  targetValue: unknown;
  issue: string;
}

export interface MigrationBatch {
  id: string;
  name: string;
  description: string;
  createdAt: string;
  totalJobs: number;
  completedJobs: number;
  failedJobs: number;
  status: 'active' | 'paused' | 'completed' | 'cancelled';
}
