import { Router, Request, Response } from 'express';
import { JobsService } from '../services/jobs.js';

const router = Router();
const jobsService = new JobsService();

// GET /api/jobs - List all jobs with pagination and filtering
router.get('/', async (req: Request, res: Response) => {
  const {
    page = '1',
    limit = '20',
    status,
    domain,
    search,
    sortBy = 'createdAt',
    sortOrder = 'desc',
  } = req.query;

  const result = await jobsService.listJobs({
    page: parseInt(page as string, 10),
    limit: parseInt(limit as string, 10),
    status: status as string | undefined,
    domain: domain as string | undefined,
    search: search as string | undefined,
    sortBy: sortBy as string,
    sortOrder: sortOrder as 'asc' | 'desc',
  });

  res.json(result);
});

// GET /api/jobs/:id - Get single job details
router.get('/:id', async (req: Request, res: Response) => {
  const job = await jobsService.getJob(req.params.id);
  if (!job) {
    res.status(404).json({ error: 'Job not found' });
    return;
  }
  res.json(job);
});

// GET /api/jobs/:id/history - Get job execution history
router.get('/:id/history', async (req: Request, res: Response) => {
  const history = await jobsService.getJobHistory(req.params.id);
  res.json(history);
});

// GET /api/jobs/:id/validation - Get job validation results
router.get('/:id/validation', async (req: Request, res: Response) => {
  const validation = await jobsService.getJobValidation(req.params.id);
  res.json(validation);
});

// POST /api/jobs/:id/run - Trigger job execution
router.post('/:id/run', async (req: Request, res: Response) => {
  const result = await jobsService.triggerJob(req.params.id);
  res.json(result);
});

// POST /api/jobs/:id/cancel - Cancel running job
router.post('/:id/cancel', async (req: Request, res: Response) => {
  const result = await jobsService.cancelJob(req.params.id);
  res.json(result);
});

// GET /api/jobs/tree - Get job tree structure
router.get('/tree/structure', async (_req: Request, res: Response) => {
  const tree = await jobsService.getJobTree();
  res.json(tree);
});

// GET /api/jobs/tags - Get all tags
router.get('/tags/all', async (_req: Request, res: Response) => {
  const tags = await jobsService.getTags();
  res.json(tags);
});

// GET /api/jobs/dependencies - Get job dependencies
router.get('/dependencies/all', async (_req: Request, res: Response) => {
  const deps = await jobsService.getDependencies();
  res.json(deps);
});

export { router as jobsRouter };
