import { Router, Request, Response } from 'express';
import { BatchService } from '../services/batch.js';

const router = Router();
const batchService = new BatchService();

// GET /api/batches - List all batches
router.get('/', async (req: Request, res: Response) => {
  const { page = '1', limit = '20', status } = req.query;

  const batches = await batchService.listBatches({
    page: parseInt(page as string, 10),
    limit: parseInt(limit as string, 10),
    status: status as string | undefined,
  });

  res.json(batches);
});

// GET /api/batches/:id - Get single batch
router.get('/:id', async (req: Request, res: Response) => {
  const batch = await batchService.getBatch(req.params.id);
  if (!batch) {
    res.status(404).json({ error: 'Batch not found' });
    return;
  }
  res.json(batch);
});

// POST /api/batches - Create new batch
router.post('/', async (req: Request, res: Response) => {
  const { name, jobIds } = req.body;

  if (!name || !jobIds || !Array.isArray(jobIds)) {
    res.status(400).json({ error: 'name and jobIds array are required' });
    return;
  }

  const batch = await batchService.createBatch(name, jobIds);
  res.status(201).json(batch);
});

// POST /api/batches/:id/run - Run batch
router.post('/:id/run', async (req: Request, res: Response) => {
  const result = await batchService.runBatch(req.params.id);
  res.json(result);
});

// POST /api/batches/:id/cancel - Cancel batch
router.post('/:id/cancel', async (req: Request, res: Response) => {
  const result = await batchService.cancelBatch(req.params.id);
  res.json(result);
});

// DELETE /api/batches/:id - Delete batch
router.delete('/:id', async (req: Request, res: Response) => {
  await batchService.deleteBatch(req.params.id);
  res.status(204).send();
});

export { router as batchRouter };
