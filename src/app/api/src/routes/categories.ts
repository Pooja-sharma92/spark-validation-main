/**
 * Categories API Routes
 *
 * Endpoints for managing classification categories.
 */

import { Router, Request, Response } from 'express';
import { categoryService } from '../services/categories.js';
import {
  CategoryType,
  CreateCategoryRequest,
  UpdateCategoryRequest,
  isValidCategoryType,
} from '../types/classification.js';

const router = Router();

// ============================================================================
// Category CRUD
// ============================================================================

/**
 * GET /api/categories
 * List all categories, optionally filtered by type
 */
router.get('/', async (req: Request, res: Response) => {
  try {
    const type = req.query.type as string | undefined;
    const approvedOnly = req.query.approved !== 'false';

    if (type && !isValidCategoryType(type)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid category type. Must be one of: domain, module, job_group',
      });
    }

    const categories = await categoryService.getCategories(
      type as CategoryType | undefined,
      approvedOnly
    );

    res.json({
      success: true,
      data: categories,
    });
  } catch (error) {
    console.error('Error fetching categories:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch categories',
    });
  }
});

/**
 * GET /api/categories/tree
 * Get categories as a hierarchical tree
 */
router.get('/tree', async (_req: Request, res: Response) => {
  try {
    const tree = await categoryService.getCategoryTree();
    res.json({
      success: true,
      data: tree,
    });
  } catch (error) {
    console.error('Error fetching category tree:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch category tree',
    });
  }
});

/**
 * GET /api/categories/names
 * Get category names grouped by type (for dropdowns)
 */
router.get('/names', async (_req: Request, res: Response) => {
  try {
    const names = await categoryService.getCategoryNames();
    res.json({
      success: true,
      data: names,
    });
  } catch (error) {
    console.error('Error fetching category names:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch category names',
    });
  }
});

/**
 * GET /api/categories/stats
 * Get category statistics
 */
router.get('/stats', async (_req: Request, res: Response) => {
  try {
    const stats = await categoryService.getStats();
    res.json({
      success: true,
      data: stats,
    });
  } catch (error) {
    console.error('Error fetching category stats:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch category stats',
    });
  }
});

/**
 * GET /api/categories/suggested
 * Get pending category suggestions
 */
router.get('/suggested', async (req: Request, res: Response) => {
  try {
    const type = req.query.type as string | undefined;

    if (type && !isValidCategoryType(type)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid category type',
      });
    }

    const suggestions = await categoryService.getSuggestedCategories(
      type as CategoryType | undefined
    );

    res.json({
      success: true,
      data: suggestions,
    });
  } catch (error) {
    console.error('Error fetching suggestions:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch suggestions',
    });
  }
});

/**
 * GET /api/categories/:id
 * Get a single category by ID
 */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const category = await categoryService.getCategoryById(req.params.id);

    if (!category) {
      return res.status(404).json({
        success: false,
        error: 'Category not found',
      });
    }

    res.json({
      success: true,
      data: category,
    });
  } catch (error) {
    console.error('Error fetching category:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch category',
    });
  }
});

/**
 * POST /api/categories
 * Create a new category
 */
router.post('/', async (req: Request, res: Response) => {
  try {
    const body = req.body as CreateCategoryRequest;

    // Validate required fields
    if (!body.type || !body.name) {
      return res.status(400).json({
        success: false,
        error: 'Type and name are required',
      });
    }

    if (!isValidCategoryType(body.type)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid category type',
      });
    }

    const category = await categoryService.createCategory(body);

    res.status(201).json({
      success: true,
      data: category,
      message: 'Category created successfully',
    });
  } catch (error) {
    console.error('Error creating category:', error);

    // Check for unique constraint violation
    if ((error as Error).message?.includes('unique')) {
      return res.status(409).json({
        success: false,
        error: 'A category with this name already exists',
      });
    }

    res.status(500).json({
      success: false,
      error: 'Failed to create category',
    });
  }
});

/**
 * PUT /api/categories/:id
 * Update a category
 */
router.put('/:id', async (req: Request, res: Response) => {
  try {
    const body = req.body as UpdateCategoryRequest;
    const category = await categoryService.updateCategory(req.params.id, body);

    if (!category) {
      return res.status(404).json({
        success: false,
        error: 'Category not found',
      });
    }

    res.json({
      success: true,
      data: category,
      message: 'Category updated successfully',
    });
  } catch (error) {
    console.error('Error updating category:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to update category',
    });
  }
});

/**
 * DELETE /api/categories/:id
 * Delete a category
 */
router.delete('/:id', async (req: Request, res: Response) => {
  try {
    const deleted = await categoryService.deleteCategory(req.params.id);

    if (!deleted) {
      return res.status(404).json({
        success: false,
        error: 'Category not found',
      });
    }

    res.json({
      success: true,
      message: 'Category deleted successfully',
    });
  } catch (error) {
    console.error('Error deleting category:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to delete category',
    });
  }
});

// ============================================================================
// Suggestion Management
// ============================================================================

/**
 * POST /api/categories/suggested/:id/approve
 * Approve a suggested category
 */
router.post('/suggested/:id/approve', async (req: Request, res: Response) => {
  try {
    const reviewedBy = req.body.reviewedBy || 'api';
    const category = await categoryService.approveSuggestion(req.params.id, reviewedBy);

    if (!category) {
      return res.status(404).json({
        success: false,
        error: 'Suggestion not found',
      });
    }

    res.json({
      success: true,
      data: category,
      message: 'Suggestion approved and category created',
    });
  } catch (error) {
    console.error('Error approving suggestion:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to approve suggestion',
    });
  }
});

/**
 * POST /api/categories/suggested/:id/reject
 * Reject a suggested category
 */
router.post('/suggested/:id/reject', async (req: Request, res: Response) => {
  try {
    const { reviewedBy = 'api', notes } = req.body;
    const rejected = await categoryService.rejectSuggestion(req.params.id, reviewedBy, notes);

    if (!rejected) {
      return res.status(404).json({
        success: false,
        error: 'Suggestion not found',
      });
    }

    res.json({
      success: true,
      message: 'Suggestion rejected',
    });
  } catch (error) {
    console.error('Error rejecting suggestion:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to reject suggestion',
    });
  }
});

/**
 * POST /api/categories/suggested/:id/merge
 * Merge a suggestion into an existing category
 */
router.post('/suggested/:id/merge', async (req: Request, res: Response) => {
  try {
    const { reviewedBy = 'api', targetCategoryId } = req.body;

    if (!targetCategoryId) {
      return res.status(400).json({
        success: false,
        error: 'targetCategoryId is required',
      });
    }

    const merged = await categoryService.mergeSuggestion(
      req.params.id,
      targetCategoryId,
      reviewedBy
    );

    if (!merged) {
      return res.status(404).json({
        success: false,
        error: 'Suggestion not found',
      });
    }

    res.json({
      success: true,
      message: 'Suggestion merged into existing category',
    });
  } catch (error) {
    console.error('Error merging suggestion:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to merge suggestion',
    });
  }
});

export { router as categoriesRouter };
