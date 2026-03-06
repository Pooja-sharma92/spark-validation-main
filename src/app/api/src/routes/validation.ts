import { Router, Request, Response } from 'express';
import { ValidationService } from '../services/validation.js';

const router = Router();
const validationService = new ValidationService();

// GET /api/validation/results - Get all validation results
router.get('/results', async (req: Request, res: Response) => {
  const { page = '1', limit = '50', status } = req.query;

  const results = await validationService.listResults({
    page: parseInt(page as string, 10),
    limit: parseInt(limit as string, 10),
    status: status as string | undefined,
  });

  res.json(results);
});

// GET /api/validation/results/:id - Get single validation result
router.get('/results/:id', async (req: Request, res: Response) => {
  const result = await validationService.getResult(req.params.id);
  if (!result) {
    res.status(404).json({ error: 'Validation result not found' });
    return;
  }
  res.json(result);
});

// POST /api/validation/run - Run validation on a job
router.post('/run', async (req: Request, res: Response) => {
  const { jobId, steps } = req.body;

  if (!jobId) {
    res.status(400).json({ error: 'jobId is required' });
    return;
  }

  const result = await validationService.runValidation(jobId, steps);
  res.json(result);
});

// GET /api/validation/stats - Get validation statistics
router.get('/stats', async (_req: Request, res: Response) => {
  const stats = await validationService.getStats();
  res.json(stats);
});

// GET /api/validation/rules - Get validation rules
router.get('/rules', async (_req: Request, res: Response) => {
  const rules = await validationService.getRules();
  res.json(rules);
});

export { router as validationRouter };
