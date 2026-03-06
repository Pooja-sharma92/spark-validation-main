// Transform API data (QueueJob) to UI data (MigrationJob)

import { MigrationJob, ValidationStep, JobExecutionRecord, JobStatus, ValidationStepStatus } from '../types/migration';
import { QueueJob, ValidationStage } from './api';

/**
 * Map API validation stage to UI ValidationStep format
 */
function transformValidationStage(stage: ValidationStage, index: number): ValidationStep {
  // Map stage status
  let status: ValidationStepStatus = 'pending';
  if (stage.passed) {
    status = stage.issues.length > 0 ? 'warning' : 'passed';
  } else {
    status = 'failed';
  }

  // Extract errors and warnings from issues
  const errors = stage.issues
    .filter(i => i.severity === 'error' || i.severity === 'critical')
    .map((issue, idx) => ({
      id: `${stage.stage}-error-${idx}`,
      severity: (issue.severity === 'critical' ? 'critical' : 'error') as 'critical' | 'error' | 'warning',
      code: `${stage.stage.toUpperCase()}_ERR`,
      message: issue.message,
      details: issue.suggestion,
      location: issue.line ? `Line ${issue.line}${issue.column ? `:${issue.column}` : ''}` : undefined,
      suggestion: issue.suggestion,
      timestamp: new Date().toISOString(),
    }));

  const warnings = stage.issues
    .filter(i => i.severity === 'warning')
    .map((issue, idx) => ({
      id: `${stage.stage}-warn-${idx}`,
      code: `${stage.stage.toUpperCase()}_WARN`,
      message: issue.message,
      details: issue.suggestion,
      suggestion: issue.suggestion,
    }));

  return {
    id: `step-${index}`,
    name: formatStageName(stage.stage),
    description: getStageDescription(stage.stage),
    status,
    duration: Math.round(stage.duration_seconds * 1000), // Convert to ms
    errors: errors.length > 0 ? errors : undefined,
    warnings: warnings.length > 0 ? warnings : undefined,
    metadata: {
      issuesFound: stage.issues.length,
      ...stage.details,
    },
  };
}

/**
 * Format stage name for display
 */
function formatStageName(stage: string): string {
  const names: Record<string, string> = {
    pre_execution: 'Pre-Execution Validation',
    syntax: 'Syntax Check',
    imports: 'Import Analysis',
    sql: 'SQL Validation',
    logic: 'Logic Check',
    execution: 'Execution Test',
  };
  return names[stage] || stage.charAt(0).toUpperCase() + stage.slice(1);
}

/**
 * Get stage description
 */
function getStageDescription(stage: string): string {
  const descriptions: Record<string, string> = {
    pre_execution: 'Static metadata/YAML/asset checks before execution',
    syntax: 'Validates Python syntax and AST parsing',
    imports: 'Checks import statements and dependencies',
    sql: 'Validates SQL queries in spark.sql() calls',
    logic: 'Detects anti-patterns and code quality issues',
    execution: 'Runs job in dry-run mode',
  };
  return descriptions[stage] || `Validation stage: ${stage}`;
}

/**
 * Map API status to UI JobStatus
 */
function mapJobStatus(status: string): JobStatus {
  const normalizedStatus = status.toLowerCase();
  switch (normalizedStatus) {
    case 'completed':
      return 'completed';
    case 'failed':
    case 'error':
      return 'failed';
    case 'running':
    case 'processing':
      return 'running';
    case 'validating':
      return 'validating';
    default:
      return 'pending';
  }
}

/**
 * Map priority string (P0-P3) to UI priority
 */
function mapPriority(priority: string): 'high' | 'medium' | 'low' {
  if (priority === 'P0' || priority === 'P1') return 'high';
  if (priority === 'P2') return 'medium';
  return 'low';
}

/**
 * Extract job name from job path
 */
function extractJobName(jobPath: string): string {
  const parts = jobPath.split('/');
  const fileName = parts[parts.length - 1] || 'Unknown Job';
  return fileName.replace('.py', '');
}

/**
 * Calculate duration in milliseconds
 */
function calculateDuration(startTime?: string, endTime?: string): number | undefined {
  if (!startTime || !endTime) return undefined;
  return new Date(endTime).getTime() - new Date(startTime).getTime();
}

/**
 * Transform QueueJob to MigrationJob format
 */
export function transformQueueJobToMigrationJob(queueJob: QueueJob): MigrationJob {
  const result = queueJob.result;
  const validationSteps = result?.stages?.map(transformValidationStage) || [];

  // Calculate progress based on validation stages
  const passedStages = validationSteps.filter(s => s.status === 'passed' || s.status === 'warning').length;
  const totalStages = validationSteps.length || 5;
  const progress = queueJob.status.toLowerCase() === 'completed'
    ? 100
    : Math.round((passedStages / totalStages) * 100);

  // Calculate job complexity from path or default
  const jobName = extractJobName(queueJob.jobPath);
  let complexity: 'simple' | 'medium' | 'complex' | 'critical' = 'medium';
  if (validationSteps.some(s => s.errors && s.errors.length > 2)) {
    complexity = 'complex';
  }

  // Build execution history from current result (single entry)
  const executionHistory: JobExecutionRecord[] = result ? [{
    id: `exec-${queueJob.id}`,
    executionNumber: 1,
    startTime: queueJob.startedAt || queueJob.createdAt,
    endTime: queueJob.completedAt,
    status: mapJobStatus(queueJob.status),
    duration: calculateDuration(queueJob.startedAt, queueJob.completedAt),
    triggeredBy: queueJob.triggeredBy || 'system',
    triggerReason: queueJob.triggerSource === 'manual' ? 'manual' : 'scheduled',
    validationSteps,
    dataStats: {
      sourceRowCount: 0,
      targetRowCount: 0,
      validatedRowCount: validationSteps.filter(s => s.status === 'passed').length,
      failedRowCount: validationSteps.filter(s => s.status === 'failed').length,
      mismatchCount: 0,
    },
    errors: validationSteps.flatMap(s => s.errors || []),
    warnings: validationSteps.flatMap(s => s.warnings || []),
    environment: {
      executionNode: queueJob.workerId || 'local',
      configHash: queueJob.commitSha || 'N/A',
    },
  }] : [];

  return {
    id: queueJob.id,
    name: jobName,
    sourceTable: 'N/A',
    targetTable: 'N/A',
    status: mapJobStatus(queueJob.status),
    startTime: queueJob.startedAt,
    endTime: queueJob.completedAt,
    progress,
    sourceRowCount: 0,
    targetRowCount: 0,
    validatedRowCount: passedStages,
    failedRowCount: validationSteps.filter(s => s.status === 'failed').length,
    mismatchCount: 0,
    priority: mapPriority(queueJob.priority),
    error: queueJob.errorMessage || result?.error,
    domain: extractDomain(queueJob.jobPath),
    complexity,
    validationSteps,
    metadata: {
      jobPath: queueJob.jobPath,
      branch: queueJob.branch,
      commitSha: queueJob.commitSha,
      triggerSource: queueJob.triggerSource,
      createdAt: queueJob.createdAt,
    },
    executionHistory,
  };
}

/**
 * Extract domain from job path
 */
function extractDomain(jobPath: string): string {
  // Try to extract domain from path like "domain-customer/jobs/..."
  const match = jobPath.match(/domain[-_]?(\w+)/i);
  if (match) {
    return match[1].charAt(0).toUpperCase() + match[1].slice(1);
  }

  // Try to extract from directory structure
  const parts = jobPath.split('/');
  for (const part of parts) {
    if (part.startsWith('domain') || part.includes('_')) {
      return part.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
  }

  return 'Unknown';
}

/**
 * Transform multiple QueueJobs to MigrationJobs
 */
export function transformQueueJobsToMigrationJobs(queueJobs: QueueJob[]): MigrationJob[] {
  return queueJobs.map(transformQueueJobToMigrationJob);
}