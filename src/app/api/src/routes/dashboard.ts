import { Router, Request, Response } from 'express';
import { DashboardService } from '../services/dashboard.js';

const router = Router();
const dashboardService = new DashboardService();

// GET /api/dashboard/stats - Get dashboard statistics
router.get('/stats', async (_req: Request, res: Response) => {
  const stats = await dashboardService.getStats();
  res.json(stats);
});

// GET /api/dashboard/recent-jobs - Get recent jobs
router.get('/recent-jobs', async (req: Request, res: Response) => {
  const { limit = '10' } = req.query;
  const jobs = await dashboardService.getRecentJobs(parseInt(limit as string, 10));
  res.json(jobs);
});

// GET /api/dashboard/activity - Get recent activity
router.get('/activity', async (req: Request, res: Response) => {
  const { limit = '20' } = req.query;
  const activity = await dashboardService.getRecentActivity(parseInt(limit as string, 10));
  res.json(activity);
});

// GET /api/dashboard/trends - Get trend data for charts
router.get('/trends', async (req: Request, res: Response) => {
  const { period = '7d' } = req.query;
  const trends = await dashboardService.getTrends(period as string);
  res.json(trends);
});

// GET /api/dashboard/alerts - Get active alerts
router.get('/alerts', async (_req: Request, res: Response) => {
  const alerts = await dashboardService.getAlerts();
  res.json(alerts);
});

export { router as dashboardRouter };
