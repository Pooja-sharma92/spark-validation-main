export interface JobFilter {
  search: string;
  status: string[];
  priority: string[];
  tags: string[];
  dateRange: {
    start?: string;
    end?: string;
  };
  hasIssues: boolean | null;
  sourceTable?: string;
  targetTable?: string;
}

export interface JobGroup {
  id: string;
  name: string;
  description: string;
  jobIds: string[];
  color: string;
  createdAt: string;
}

export interface JobDependency {
  jobId: string;
  dependsOn: string[];
  blockingJobs: string[];
}

export interface JobTag {
  id: string;
  name: string;
  color: string;
  description: string;
}

export interface BulkOperation {
  type: 'start' | 'pause' | 'retry' | 'cancel' | 'delete' | 'assign-tag';
  jobIds: string[];
  params?: unknown;
}

export interface JobSchedule {
  jobId: string;
  cronExpression?: string;
  scheduledTime?: string;
  repeatInterval?: number;
  enabled: boolean;
}
