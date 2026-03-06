import { Router, Request, Response, NextFunction } from 'express';
import { queueService } from '../services/queue.js';

export const queueRouter = Router();

// GET /api/queue/metrics - Get queue health metrics
queueRouter.get('/metrics', async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const metrics = await queueService.getQueueMetrics();
    res.json(metrics);
  } catch (error) {
    next(error);
  }
});

// GET /api/queue/priorities - Get all priority queues with jobs
queueRouter.get('/priorities', async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const queues = await queueService.getPriorityQueues();
    res.json(queues);
  } catch (error) {
    next(error);
  }
});

// GET /api/queue/jobs - Get all jobs
queueRouter.get('/jobs', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const limit = parseInt(req.query.limit as string) || 100;
    const jobs = await queueService.getAllJobs(limit);
    res.json(jobs);
  } catch (error) {
    next(error);
  }
});

// GET /api/queue/dead-letter - Get dead letter queue jobs
queueRouter.get('/dead-letter', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50;
    const jobs = await queueService.getDeadLetterJobs(limit);
    res.json(jobs);
  } catch (error) {
    next(error);
  }
});

// GET /api/queue/history - Get validation history (completed/failed jobs with results)
queueRouter.get('/history', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const limit = parseInt(req.query.limit as string) || 100;
    const jobs = await queueService.getValidationHistory(limit);
    res.json(jobs);
  } catch (error) {
    next(error);
  }
});

// GET /api/queue/jobs/:id - Get single job details
queueRouter.get('/jobs/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const job = await queueService.getJobById(req.params.id, true);
    if (!job) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }
    res.json(job);
  } catch (error) {
    next(error);
  }
});

// POST /api/queue/jobs/:id/retry - Retry a failed job
queueRouter.post('/jobs/:id/retry', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const success = await queueService.retryJob(req.params.id);
    if (!success) {
      res.status(404).json({ error: 'Job not found or not in dead letter queue' });
      return;
    }
    res.json({ message: 'Job requeued successfully' });
  } catch (error) {
    next(error);
  }
});

// DELETE /api/queue/jobs/:id - Delete a job
queueRouter.delete('/jobs/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const success = await queueService.deleteJob(req.params.id);
    if (!success) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }
    res.json({ message: 'Job deleted successfully' });
  } catch (error) {
    next(error);
  }
});
