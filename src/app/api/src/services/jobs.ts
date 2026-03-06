import { MigrationJob, PaginatedResponse } from '../types/index.js';

interface ListJobsParams {
  page: number;
  limit: number;
  status?: string;
  domain?: string;
  search?: string;
  sortBy: string;
  sortOrder: 'asc' | 'desc';
}

// Mock data for development - replace with actual database queries
const mockJobs: MigrationJob[] = [
  {
    id: 'job-001',
    name: 'Customer Data Migration',
    sourceTable: 'IBM_TEST.CUSTOMER_MASTER',
    targetTable: 'VIBMAVPNE0.DIM_CUSTOMER',
    status: 'completed',
    progress: 100,
    sourceRowCount: 1500000,
    targetRowCount: 1500000,
    domain: 'Customer',
    module: 'Core',
    priority: 'high',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  },
  {
    id: 'job-002',
    name: 'Loan Data Migration',
    sourceTable: 'IBM_TEST.LOAN_MASTER',
    targetTable: 'VIBMAVPNE0.FACT_LOAN',
    status: 'running',
    progress: 65,
    sourceRowCount: 800000,
    targetRowCount: 520000,
    domain: 'Loan',
    module: 'Core',
    priority: 'critical',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  },
];

export class JobsService {
  async listJobs(params: ListJobsParams): Promise<PaginatedResponse<MigrationJob>> {
    let filtered = [...mockJobs];

    // Apply filters
    if (params.status) {
      filtered = filtered.filter(j => j.status === params.status);
    }
    if (params.domain) {
      filtered = filtered.filter(j => j.domain === params.domain);
    }
    if (params.search) {
      const search = params.search.toLowerCase();
      filtered = filtered.filter(
        j => j.name.toLowerCase().includes(search) || j.sourceTable.toLowerCase().includes(search)
      );
    }

    // Apply sorting
    filtered.sort((a, b) => {
      const aVal = a[params.sortBy as keyof MigrationJob] as string;
      const bVal = b[params.sortBy as keyof MigrationJob] as string;
      return params.sortOrder === 'asc'
        ? aVal.localeCompare(bVal)
        : bVal.localeCompare(aVal);
    });

    // Apply pagination
    const total = filtered.length;
    const start = (params.page - 1) * params.limit;
    const data = filtered.slice(start, start + params.limit);

    return {
      data,
      pagination: {
        page: params.page,
        limit: params.limit,
        total,
        totalPages: Math.ceil(total / params.limit),
      },
    };
  }

  async getJob(id: string): Promise<MigrationJob | null> {
    return mockJobs.find(j => j.id === id) || null;
  }

  async getJobHistory(id: string): Promise<unknown[]> {
    // TODO: Implement with actual data source
    return [
      {
        id: `${id}-run-1`,
        startTime: new Date(Date.now() - 3600000).toISOString(),
        endTime: new Date().toISOString(),
        status: 'completed',
        rowsProcessed: 1500000,
      },
    ];
  }

  async getJobValidation(id: string): Promise<unknown[]> {
    // TODO: Implement with actual data source
    return [
      { stepName: 'Syntax Check', status: 'passed' },
      { stepName: 'Row Count Validation', status: 'passed' },
      { stepName: 'Data Quality Check', status: 'warning', message: 'Minor issues found' },
    ];
  }

  async triggerJob(id: string): Promise<{ success: boolean; message: string }> {
    // TODO: Implement job triggering via queue
    console.log(`Triggering job ${id}`);
    return { success: true, message: `Job ${id} queued for execution` };
  }

  async cancelJob(id: string): Promise<{ success: boolean; message: string }> {
    // TODO: Implement job cancellation
    console.log(`Cancelling job ${id}`);
    return { success: true, message: `Job ${id} cancelled` };
  }

  async getJobTree(): Promise<unknown> {
    // TODO: Implement tree structure
    return {
      domains: [
        {
          id: 'customer',
          name: 'Customer',
          modules: [
            { id: 'core', name: 'Core', jobCount: 5 },
            { id: 'analytics', name: 'Analytics', jobCount: 3 },
          ],
        },
      ],
    };
  }

  async getTags(): Promise<unknown[]> {
    return [
      { id: 'priority-high', name: 'High Priority', color: 'red' },
      { id: 'priority-low', name: 'Low Priority', color: 'green' },
    ];
  }

  async getDependencies(): Promise<unknown[]> {
    return [
      { jobId: 'job-001', dependsOn: [], blockedBy: [] },
      { jobId: 'job-002', dependsOn: ['job-001'], blockedBy: [] },
    ];
  }
}
