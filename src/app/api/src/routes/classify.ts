/**
 * Classification API Routes
 *
 * Endpoints for job classification and batch processing.
 */

import { Router, Request, Response } from 'express';
import { classifyService } from '../services/classify.js';
import {
  ClassificationFilters,
  StartBatchRequest,
  isValidComplexity,
} from '../types/classification.js';

const router = Router();

// ============================================================================
// Classification Queries
// ============================================================================

/**
 * GET /api/classify/results
 * Get classification results with filtering and pagination
 */
router.get('/results', async (req: Request, res: Response) => {
  try {
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = Math.min(parseInt(req.query.limit as string, 10) || 20, 100);

    const filters: ClassificationFilters = {};

    if (req.query.domainId) {
      filters.domainId = req.query.domainId as string;
    }
    if (req.query.moduleId) {
      filters.moduleId = req.query.moduleId as string;
    }
    if (req.query.jobGroupId) {
      filters.jobGroupId = req.query.jobGroupId as string;
    }
    if (req.query.complexity) {
      const complexity = req.query.complexity as string;
      if (!isValidComplexity(complexity)) {
        return res.status(400).json({
          success: false,
          error: 'Invalid complexity. Must be one of: low, medium, high',
        });
      }
      filters.complexity = complexity;
    }
    if (req.query.batchId) {
      filters.batchId = req.query.batchId as string;
    }
    if (req.query.hasSuggestions !== undefined) {
      filters.hasSuggestions = req.query.hasSuggestions === 'true';
    }
    if (req.query.search) {
      filters.search = req.query.search as string;
    }

    const { data, total } = await classifyService.getClassifications(filters, page, limit);

    res.json({
      success: true,
      data,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    });
  } catch (error) {
    console.error('Error fetching classifications:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch classifications',
    });
  }
});

/**
 * GET /api/classify/results/:jobPath
 * Get classification for a specific job
 */
router.get('/results/*', async (req: Request, res: Response) => {
  try {
    // Handle paths with slashes - everything after /results/
    const jobPath = req.params[0];

    if (!jobPath) {
      return res.status(400).json({
        success: false,
        error: 'Job path is required',
      });
    }

    const classification = await classifyService.getClassificationByPath(jobPath);

    if (!classification) {
      return res.status(404).json({
        success: false,
        error: 'Classification not found',
      });
    }

    res.json({
      success: true,
      data: classification,
    });
  } catch (error) {
    console.error('Error fetching classification:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch classification',
    });
  }
});

/**
 * GET /api/classify/tree
 * Get jobs organized in a tree hierarchy: Domain > Module > Job Group > Job
 * Used by the TreeView component in Job Management
 */
router.get('/tree', async (_req: Request, res: Response) => {
  try {
    const tree = await classifyService.getJobTree();
    res.json({
      success: true,
      data: tree,
    });
  } catch (error) {
    console.error('Error fetching job tree:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch job tree',
    });
  }
});

/**
 * GET /api/classify/stats
 * Get classification statistics
 */
router.get('/stats', async (_req: Request, res: Response) => {
  try {
    const stats = await classifyService.getStats();
    res.json({
      success: true,
      data: stats,
    });
  } catch (error) {
    console.error('Error fetching classification stats:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch classification stats',
    });
  }
});

// ============================================================================
// Batch Operations
// ============================================================================

/**
 * POST /api/classify/batch
 * Start a new batch classification
 */
router.post('/batch', async (req: Request, res: Response) => {
  try {
    const body = req.body as StartBatchRequest;

    // Validate required fields
    if (!body.directories || !Array.isArray(body.directories) || body.directories.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'directories array is required and must not be empty',
      });
    }

    // Validate AI provider if specified
    if (body.aiProvider && !['ollama', 'azure-openai'].includes(body.aiProvider)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid aiProvider. Must be one of: ollama, azure-openai',
      });
    }

    const triggeredBy = req.headers['x-user'] as string || 'api';
    const batch = await classifyService.startBatch(body, triggeredBy);

    res.status(202).json({
      success: true,
      data: batch,
      message: 'Batch classification started',
    });
  } catch (error) {
    console.error('Error starting batch:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to start batch classification',
    });
  }
});

/**
 * GET /api/classify/batch
 * Get recent batches
 */
router.get('/batch', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string, 10) || 20, 50);
    const batches = await classifyService.getRecentBatches(limit);

    res.json({
      success: true,
      data: batches,
    });
  } catch (error) {
    console.error('Error fetching batches:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch batches',
    });
  }
});

/**
 * GET /api/classify/batch/:batchId
 * Get batch status
 */
router.get('/batch/:batchId', async (req: Request, res: Response) => {
  try {
    const status = await classifyService.getBatchStatus(req.params.batchId);

    if (!status) {
      return res.status(404).json({
        success: false,
        error: 'Batch not found',
      });
    }

    res.json({
      success: true,
      data: status,
    });
  } catch (error) {
    console.error('Error fetching batch status:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch batch status',
    });
  }
});

/**
 * POST /api/classify/batch/:batchId/cancel
 * Cancel a running batch
 */
router.post('/batch/:batchId/cancel', async (req: Request, res: Response) => {
  try {
    const cancelled = await classifyService.cancelBatch(req.params.batchId);

    if (!cancelled) {
      return res.status(400).json({
        success: false,
        error: 'Batch cannot be cancelled (not running or not found)',
      });
    }

    res.json({
      success: true,
      message: 'Batch cancellation requested',
    });
  } catch (error) {
    console.error('Error cancelling batch:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to cancel batch',
    });
  }
});

/**
 * DELETE /api/classify/batch/:batchId
 * Delete a batch and its results
 */
router.delete('/batch/:batchId', async (req: Request, res: Response) => {
  try {
    const deleted = await classifyService.deleteBatch(req.params.batchId);

    if (!deleted) {
      return res.status(404).json({
        success: false,
        error: 'Batch not found',
      });
    }

    res.json({
      success: true,
      message: 'Batch and associated classifications deleted',
    });
  } catch (error) {
    console.error('Error deleting batch:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to delete batch',
    });
  }
});

export { router as classifyRouter };
