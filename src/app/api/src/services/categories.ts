/**
 * Category Service
 *
 * Handles category CRUD operations via PostgreSQL.
 */

import pg from 'pg';
import {
  Category,
  CategoryType,
  CreateCategoryRequest,
  UpdateCategoryRequest,
  CategoryTreeNode,
  SuggestedCategory,
  CategoryStats,
} from '../types/classification.js';

const { Pool } = pg;

class CategoryService {
  private pool: pg.Pool | null = null;

  constructor() {
    this.initPool();
  }

  private initPool(): void {
    this.pool = new Pool({
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
      database: process.env.POSTGRES_DB || 'validation_results',
      user: process.env.POSTGRES_USER || 'postgres',
      password: process.env.POSTGRES_PASSWORD || '',
      max: 10,
    });
  }

  private getPool(): pg.Pool {
    if (!this.pool) {
      this.initPool();
    }
    return this.pool!;
  }

  // =========================================================================
  // Category CRUD
  // =========================================================================

  async getCategories(type?: CategoryType, approvedOnly = true): Promise<Category[]> {
    let query = 'SELECT * FROM categories WHERE 1=1';
    const params: (string | boolean)[] = [];

    if (type) {
      params.push(type);
      query += ` AND type = $${params.length}`;
    }

    if (approvedOnly) {
      query += ' AND approved = true';
    }

    query += ' ORDER BY type, name';

    const result = await this.getPool().query(query, params);
    return result.rows.map(this.rowToCategory);
  }

  async getCategoryById(id: string): Promise<Category | null> {
    const result = await this.getPool().query(
      'SELECT * FROM categories WHERE id = $1',
      [id]
    );
    return result.rows.length > 0 ? this.rowToCategory(result.rows[0]) : null;
  }

  async createCategory(req: CreateCategoryRequest): Promise<Category> {
    const result = await this.getPool().query(
      `INSERT INTO categories (type, name, description, parent_id, ai_discovered, approved)
       VALUES ($1, $2, $3, $4, false, true)
       RETURNING *`,
      [req.type, req.name, req.description || null, req.parentId || null]
    );
    return this.rowToCategory(result.rows[0]);
  }

  async updateCategory(id: string, req: UpdateCategoryRequest): Promise<Category | null> {
    const updates: string[] = [];
    const params: (string | null)[] = [];
    let paramCount = 0;

    if (req.name !== undefined) {
      paramCount++;
      updates.push(`name = $${paramCount}`);
      params.push(req.name);
    }
    if (req.description !== undefined) {
      paramCount++;
      updates.push(`description = $${paramCount}`);
      params.push(req.description);
    }
    if (req.parentId !== undefined) {
      paramCount++;
      updates.push(`parent_id = $${paramCount}`);
      params.push(req.parentId);
    }

    if (updates.length === 0) {
      return this.getCategoryById(id);
    }

    updates.push('updated_at = NOW()');
    paramCount++;
    params.push(id);

    const result = await this.getPool().query(
      `UPDATE categories SET ${updates.join(', ')} WHERE id = $${paramCount} RETURNING *`,
      params
    );

    return result.rows.length > 0 ? this.rowToCategory(result.rows[0]) : null;
  }

  async deleteCategory(id: string): Promise<boolean> {
    const result = await this.getPool().query(
      'DELETE FROM categories WHERE id = $1',
      [id]
    );
    return (result.rowCount ?? 0) > 0;
  }

  // =========================================================================
  // Category Tree
  // =========================================================================

  async getCategoryTree(): Promise<CategoryTreeNode[]> {
    const result = await this.getPool().query(`
      WITH RECURSIVE category_tree AS (
        SELECT
          id, type, name, description, parent_id, approved, ai_discovered,
          created_at, updated_at,
          1 AS level,
          name::TEXT AS path,
          ARRAY[id] AS path_ids
        FROM categories
        WHERE parent_id IS NULL AND approved = true
        UNION ALL
        SELECT
          c.id, c.type, c.name, c.description, c.parent_id, c.approved, c.ai_discovered,
          c.created_at, c.updated_at,
          ct.level + 1,
          ct.path || ' > ' || c.name,
          ct.path_ids || c.id
        FROM categories c
        INNER JOIN category_tree ct ON c.parent_id = ct.id
        WHERE c.approved = true
      )
      SELECT * FROM category_tree ORDER BY path
    `);

    // Build tree structure
    const nodeMap = new Map<string, CategoryTreeNode>();
    const roots: CategoryTreeNode[] = [];

    for (const row of result.rows) {
      const node: CategoryTreeNode = {
        ...this.rowToCategory(row),
        children: [],
        level: row.level,
        path: row.path,
      };
      nodeMap.set(node.id, node);

      if (!row.parent_id) {
        roots.push(node);
      }
    }

    // Link children to parents
    for (const row of result.rows) {
      if (row.parent_id) {
        const parent = nodeMap.get(row.parent_id);
        const child = nodeMap.get(row.id);
        if (parent && child) {
          parent.children.push(child);
        }
      }
    }

    return roots;
  }

  async getCategoryNames(): Promise<{ domains: string[]; modules: string[]; jobGroups: string[] }> {
    const result = await this.getPool().query(
      "SELECT type, name FROM categories WHERE approved = true ORDER BY type, name"
    );

    const names = { domains: [] as string[], modules: [] as string[], jobGroups: [] as string[] };
    for (const row of result.rows) {
      if (row.type === 'domain') names.domains.push(row.name);
      else if (row.type === 'module') names.modules.push(row.name);
      else if (row.type === 'job_group') names.jobGroups.push(row.name);
    }

    return names;
  }

  // =========================================================================
  // Suggested Categories
  // =========================================================================

  async getSuggestedCategories(type?: CategoryType): Promise<SuggestedCategory[]> {
    let query = "SELECT * FROM suggested_categories WHERE status = 'pending'";
    const params: string[] = [];

    if (type) {
      params.push(type);
      query += ` AND type = $${params.length}`;
    }

    query += ' ORDER BY occurrence_count DESC, last_suggested_at DESC LIMIT 100';

    const result = await this.getPool().query(query, params);
    return result.rows.map(this.rowToSuggestion);
  }

  async approveSuggestion(id: string, reviewedBy: string): Promise<Category | null> {
    const pool = this.getPool();

    // Get suggestion
    const sugResult = await pool.query(
      'SELECT * FROM suggested_categories WHERE id = $1',
      [id]
    );

    if (sugResult.rows.length === 0) {
      return null;
    }

    const suggestion = sugResult.rows[0];

    // Update suggestion status
    await pool.query(
      `UPDATE suggested_categories
       SET status = 'approved', reviewed_by = $2, reviewed_at = NOW()
       WHERE id = $1`,
      [id, reviewedBy]
    );

    // Create the category
    const catResult = await pool.query(
      `INSERT INTO categories (type, name, description, ai_discovered, approved, approved_by, approved_at)
       VALUES ($1, $2, $3, true, true, $4, NOW())
       RETURNING *`,
      [suggestion.type, suggestion.name, suggestion.description, reviewedBy]
    );

    return this.rowToCategory(catResult.rows[0]);
  }

  async rejectSuggestion(id: string, reviewedBy: string, notes?: string): Promise<boolean> {
    const result = await this.getPool().query(
      `UPDATE suggested_categories
       SET status = 'rejected', reviewed_by = $2, reviewed_at = NOW(), review_notes = $3
       WHERE id = $1`,
      [id, reviewedBy, notes || null]
    );
    return (result.rowCount ?? 0) > 0;
  }

  async mergeSuggestion(id: string, targetCategoryId: string, reviewedBy: string): Promise<boolean> {
    const result = await this.getPool().query(
      `UPDATE suggested_categories
       SET status = 'merged', merged_into_id = $2, reviewed_by = $3, reviewed_at = NOW()
       WHERE id = $1`,
      [id, targetCategoryId, reviewedBy]
    );
    return (result.rowCount ?? 0) > 0;
  }

  // =========================================================================
  // Statistics
  // =========================================================================

  async getStats(): Promise<CategoryStats> {
    const pool = this.getPool();

    const totalResult = await pool.query('SELECT COUNT(*) FROM categories WHERE approved = true');
    const byTypeResult = await pool.query(
      "SELECT type, COUNT(*) as count FROM categories WHERE approved = true GROUP BY type"
    );
    const suggestionsResult = await pool.query(
      "SELECT COUNT(*) FROM suggested_categories WHERE status = 'pending'"
    );

    const byType: Record<CategoryType, number> = {
      domain: 0,
      module: 0,
      job_group: 0,
    };

    for (const row of byTypeResult.rows) {
      byType[row.type as CategoryType] = parseInt(row.count, 10);
    }

    return {
      totalCategories: parseInt(totalResult.rows[0].count, 10),
      byType,
      pendingSuggestions: parseInt(suggestionsResult.rows[0].count, 10),
    };
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private rowToCategory(row: Record<string, unknown>): Category {
    return {
      id: row.id as string,
      type: row.type as CategoryType,
      name: row.name as string,
      description: row.description as string | undefined,
      parentId: row.parent_id as string | undefined,
      aiDiscovered: row.ai_discovered as boolean,
      approved: row.approved as boolean,
      approvedBy: row.approved_by as string | undefined,
      approvedAt: row.approved_at ? (row.approved_at as Date).toISOString() : undefined,
      metadata: row.metadata as Record<string, unknown> | undefined,
      createdAt: (row.created_at as Date).toISOString(),
      updatedAt: (row.updated_at as Date).toISOString(),
    };
  }

  private rowToSuggestion(row: Record<string, unknown>): SuggestedCategory {
    return {
      id: row.id as string,
      type: row.type as CategoryType,
      name: row.name as string,
      description: row.description as string | undefined,
      parentName: row.parent_name as string | undefined,
      suggestedByJobs: (row.suggested_by_jobs as string[]) || [],
      occurrenceCount: row.occurrence_count as number,
      firstSuggestedAt: (row.first_suggested_at as Date).toISOString(),
      lastSuggestedAt: (row.last_suggested_at as Date).toISOString(),
      status: row.status as 'pending' | 'approved' | 'rejected' | 'merged',
      mergedIntoId: row.merged_into_id as string | undefined,
      reviewedBy: row.reviewed_by as string | undefined,
      reviewedAt: row.reviewed_at ? (row.reviewed_at as Date).toISOString() : undefined,
      reviewNotes: row.review_notes as string | undefined,
    };
  }

  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
  }
}

export const categoryService = new CategoryService();
