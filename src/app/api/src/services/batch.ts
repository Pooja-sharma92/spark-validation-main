import { BatchJob, PaginatedResponse } from '../types/index.js';

interface ListBatchesParams {
  page: number;
  limit: number;
  status?: string;
}

const mockBatches: BatchJob[] = [
  {
    id: 'batch-001',
    name: 'Daily Customer Jobs',
    jobIds: ['job-001', 'job-002'],
    status: 'completed',
    createdAt: new Date(Date.now() - 86400000).toISOString(),
    startedAt: new Date(Date.now() - 82800000).toISOString(),
    completedAt: new Date(Date.now() - 79200000).toISOString(),
  },
  {
    id: 'batch-002',
    name: 'Weekly Analytics Jobs',
    jobIds: ['job-003', 'job-004', 'job-005'],
    status: 'pending',
    createdAt: new Date().toISOString(),
  },
];

export class BatchService {
  async listBatches(params: ListBatchesParams): Promise<PaginatedResponse<BatchJob>> {
    let filtered = [...mockBatches];

    if (params.status) {
      filtered = filtered.filter(b => b.status === params.status);
    }

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

  async getBatch(id: string): Promise<BatchJob | null> {
    return mockBatches.find(b => b.id === id) || null;
  }

  async createBatch(name: string, jobIds: string[]): Promise<BatchJob> {
    const batch: BatchJob = {
      id: `batch-${Date.now()}`,
      name,
      jobIds,
      status: 'pending',
      createdAt: new Date().toISOString(),
    };
    mockBatches.push(batch);
    return batch;
  }

  async runBatch(id: string): Promise<{ success: boolean; message: string }> {
    const batch = mockBatches.find(b => b.id === id);
    if (!batch) {
      return { success: false, message: 'Batch not found' };
    }
    // TODO: Implement batch execution via queue
    console.log(`Running batch ${id}`);
    return { success: true, message: `Batch ${id} started` };
  }

  async cancelBatch(id: string): Promise<{ success: boolean; message: string }> {
    // TODO: Implement batch cancellation
    console.log(`Cancelling batch ${id}`);
    return { success: true, message: `Batch ${id} cancelled` };
  }

  async deleteBatch(id: string): Promise<void> {
    const index = mockBatches.findIndex(b => b.id === id);
    if (index !== -1) {
      mockBatches.splice(index, 1);
    }
  }
}
