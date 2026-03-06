// API client for connecting to the validation backend

// Use window variable for runtime config, fallback to default
declare global {
  interface Window {
    __VITE_API_URL__?: string;
  }
}

const API_BASE_URL = (typeof window !== 'undefined' && window.__VITE_API_URL__)
  ? window.__VITE_API_URL__
  : 'http://localhost:3801/api';

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

class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async fetch<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status} ${response.statusText}`);
    }

    return response.json();
  }

  // Queue endpoints
  async getQueueMetrics(): Promise<QueueMetrics> {
    return this.fetch<QueueMetrics>('/queue/metrics');
  }

  async getAllJobs(limit: number = 100): Promise<QueueJob[]> {
    return this.fetch<QueueJob[]>(`/queue/jobs?limit=${limit}`);
  }

  async getJobById(jobId: string): Promise<QueueJob | null> {
    try {
      return await this.fetch<QueueJob>(`/queue/jobs/${jobId}`);
    } catch {
      return null;
    }
  }

  async getValidationHistory(limit: number = 100): Promise<QueueJob[]> {
    return this.fetch<QueueJob[]>(`/queue/history?limit=${limit}`);
  }

  async getDeadLetterJobs(limit: number = 50): Promise<QueueJob[]> {
    return this.fetch<QueueJob[]>(`/queue/dead-letter?limit=${limit}`);
  }

  async retryJob(jobId: string): Promise<boolean> {
    try {
      await this.fetch(`/queue/jobs/${jobId}/retry`, { method: 'POST' });
      return true;
    } catch {
      return false;
    }
  }

  async deleteJob(jobId: string): Promise<boolean> {
    try {
      await this.fetch(`/queue/jobs/${jobId}`, { method: 'DELETE' });
      return true;
    } catch {
      return false;
    }
  }
}

export const apiClient = new ApiClient();
