import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import { jobsRouter } from './routes/jobs.js';
import { validationRouter } from './routes/validation.js';
import { batchRouter } from './routes/batch.js';
import { dashboardRouter } from './routes/dashboard.js';
import { queueRouter } from './routes/queue.js';
import { categoriesRouter } from './routes/categories.js';
import { classifyRouter } from './routes/classify.js';
import { errorHandler } from './middleware/errorHandler.js';

const app = express();
const PORT = process.env.PORT || 3801;

// Middleware
app.use(helmet());
app.use(cors({
  origin: process.env.CORS_ORIGIN || 'http://localhost:3800',
  credentials: true,
}));
app.use(morgan('combined'));
app.use(express.json());

// Health check
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// API Routes
app.use('/api/jobs', jobsRouter);
app.use('/api/validation', validationRouter);
app.use('/api/batches', batchRouter);
app.use('/api/dashboard', dashboardRouter);
app.use('/api/queue', queueRouter);
app.use('/api/categories', categoriesRouter);
app.use('/api/classify', classifyRouter);

// Error handling
app.use(errorHandler);

// 404 handler
app.use((_req, res) => {
  res.status(404).json({ error: 'Not Found' });
});

app.listen(PORT, () => {
  console.log(`API server running on port ${PORT}`);
});

export default app;
