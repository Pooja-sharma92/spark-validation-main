import { useState, useEffect, useCallback } from 'react';
import { QueueJob, QueueMetrics, PriorityQueueInfo, RateLimit } from '../../types/queue';
import { mockRateLimits } from '../../data/queueMockData';
import {
  QueueHealthCards,
  PriorityQueuesTab,
  AllJobsTab,
  DeadLetterTab,
  RateLimitsTab,
  QueueOperationsPanel,
  SubmitJobModal,
  JobDetailsModal,
  SubmitJobData,
} from '../queue';
import {
  Layers,
  List,
  AlertTriangle,
  Gauge,
  Plus,
  RefreshCw,
  Loader2,
  WifiOff,
} from 'lucide-react';

const API_BASE = '/api/queue';

type TabType = 'priority' | 'all' | 'dead_letter' | 'rate_limits';

export function QueueMonitorPage() {
  const [activeTab, setActiveTab] = useState<TabType>('priority');
  const [selectedJob, setSelectedJob] = useState<QueueJob | null>(null);
  const [showSubmitModal, setShowSubmitModal] = useState(false);
  const [rateLimits, setRateLimits] = useState<Record<string, RateLimit>>(mockRateLimits);

  // Data state
  const [metrics, setMetrics] = useState<QueueMetrics | null>(null);
  const [priorityQueues, setPriorityQueues] = useState<PriorityQueueInfo[]>([]);
  const [allJobs, setAllJobs] = useState<QueueJob[]>([]);
  const [deadLetterJobs, setDeadLetterJobs] = useState<QueueJob[]>([]);

  // Loading and error state
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  // Fetch all data
  const fetchData = useCallback(async (showRefreshing = false) => {
    if (showRefreshing) setRefreshing(true);
    else setLoading(true);
    setError(null);

    try {
      const [metricsRes, prioritiesRes, jobsRes, dlqRes] = await Promise.all([
        fetch(`${API_BASE}/metrics`),
        fetch(`${API_BASE}/priorities`),
        fetch(`${API_BASE}/jobs?limit=200`),
        fetch(`${API_BASE}/dead-letter`),
      ]);

      if (!metricsRes.ok || !prioritiesRes.ok || !jobsRes.ok || !dlqRes.ok) {
        throw new Error('Failed to fetch queue data');
      }

      const [metricsData, prioritiesData, jobsData, dlqData] = await Promise.all([
        metricsRes.json(),
        prioritiesRes.json(),
        jobsRes.json(),
        dlqRes.json(),
      ]);

      // Transform API data to match frontend types
      setMetrics({
        totalPending: metricsData.totalPending,
        pendingByPriority: metricsData.pendingByPriority,
        backpressureLevel: metricsData.backpressureLevel,
        activeWorkers: metricsData.activeWorkers,
        totalWorkers: metricsData.totalWorkers,
        throughput: 0, // Not tracked in current backend
        throughputHistory: [],
        throughputVsAverage: 0,
        oldestPendingAge: metricsData.oldestPendingAge || 0,
        oldestPendingPriority: metricsData.oldestPendingPriority,
      });

      // Transform priority queues
      const transformedQueues: PriorityQueueInfo[] = prioritiesData.map((q: any) => ({
        priority: q.priority,
        name: q.name,
        description: q.description,
        color: q.priority === 'P0' ? 'red' : q.priority === 'P1' ? 'orange' : q.priority === 'P2' ? 'blue' : 'gray',
        badgeColor: q.priority === 'P0' ? 'bg-red-600' : q.priority === 'P1' ? 'bg-orange-600' : q.priority === 'P2' ? 'bg-blue-600' : 'bg-gray-600',
        pendingCount: q.pendingCount,
        rateLimit: mockRateLimits[q.priority] || mockRateLimits.P2,
        jobs: q.jobs || [],
      }));
      setPriorityQueues(transformedQueues);

      setAllJobs(jobsData);
      setDeadLetterJobs(dlqData);
    } catch (err) {
      console.error('Failed to fetch queue data:', err);
      setError(err instanceof Error ? err.message : 'Failed to connect to API');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  // Initial fetch
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Auto-refresh every 10 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      fetchData(true);
    }, 10000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const tabs: { id: TabType; label: string; icon: React.ReactNode; count?: number }[] = [
    { id: 'priority', label: 'Priority Queues', icon: <Layers className="w-4 h-4" /> },
    { id: 'all', label: 'All Jobs', icon: <List className="w-4 h-4" />, count: allJobs.length },
    {
      id: 'dead_letter',
      label: 'Dead Letter',
      icon: <AlertTriangle className="w-4 h-4" />,
      count: deadLetterJobs.length,
    },
    { id: 'rate_limits', label: 'Rate Limits', icon: <Gauge className="w-4 h-4" /> },
  ];

  const handleJobClick = (job: QueueJob) => {
    setSelectedJob(job);
  };

  const handleRetryJob = async (job: QueueJob) => {
    try {
      const res = await fetch(`${API_BASE}/jobs/${job.id}/retry`, { method: 'POST' });
      if (res.ok) {
        fetchData(true);
        setSelectedJob(null);
      }
    } catch (err) {
      console.error('Failed to retry job:', err);
    }
  };

  const handleDeleteJob = async (job: QueueJob) => {
    try {
      const res = await fetch(`${API_BASE}/jobs/${job.id}`, { method: 'DELETE' });
      if (res.ok) {
        fetchData(true);
      }
    } catch (err) {
      console.error('Failed to delete job:', err);
    }
  };

  const handleCancelJob = (job: QueueJob) => {
    console.log('Cancelling job:', job.id);
  };

  const handleSubmitJob = (data: SubmitJobData) => {
    console.log('Submitting new job:', data);
  };

  const handleUpdateRateLimit = (priority: string, newLimit: number) => {
    setRateLimits((prev) => ({
      ...prev,
      [priority]: { ...prev[priority], limit: newLimit },
    }));
  };

  const handleToggleRateLimit = (priority: string, enabled: boolean) => {
    setRateLimits((prev) => ({
      ...prev,
      [priority]: { ...prev[priority], enabled },
    }));
  };

  const handlePauseQueue = (priority?: string) => {
    console.log('Pausing queue:', priority || 'all');
  };

  const handleResumeQueue = (priority?: string) => {
    console.log('Resuming queue:', priority || 'all');
  };

  const handleFlushQueue = (priority?: string) => {
    console.log('Flushing queue:', priority || 'all');
  };

  const handleRetryAllFailed = () => {
    console.log('Retrying all failed jobs');
  };

  const handleClearDeadLetter = () => {
    console.log('Clearing dead letter queue');
  };

  // Default metrics for loading state
  const defaultMetrics: QueueMetrics = {
    totalPending: 0,
    pendingByPriority: { P0: 0, P1: 0, P2: 0, P3: 0 },
    backpressureLevel: 'normal',
    activeWorkers: 0,
    totalWorkers: 5,
    throughput: 0,
    throughputHistory: [],
    throughputVsAverage: 0,
    oldestPendingAge: 0,
  };

  // Error state
  if (error && !metrics) {
    return (
      <div className="p-6">
        <div className="flex flex-col items-center justify-center p-12 bg-white rounded-lg border border-gray-200">
          <WifiOff className="w-12 h-12 text-gray-400 mb-4" />
          <h2 className="text-lg font-medium text-gray-900 mb-2">Unable to Connect</h2>
          <p className="text-sm text-gray-500 mb-4">{error}</p>
          <p className="text-xs text-gray-400 mb-4">Make sure the API server is running on port 3801 and Redis is available.</p>
          <button
            onClick={() => fetchData()}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">Queue Monitor</h1>
          <p className="text-sm text-gray-500 mt-1">
            Monitor and manage validation job queues in real-time
            {error && <span className="text-orange-500 ml-2">(Connection issue - showing cached data)</span>}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => fetchData(true)}
            disabled={refreshing}
            className="flex items-center gap-2 px-4 py-2 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
          <button
            onClick={() => setShowSubmitModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Plus className="w-4 h-4" />
            Submit Job
          </button>
        </div>
      </div>

      {/* Health Cards */}
      {loading ? (
        <div className="flex items-center justify-center p-8">
          <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
        </div>
      ) : (
        <QueueHealthCards metrics={metrics || defaultMetrics} />
      )}

      {/* Main Content */}
      <div className="grid grid-cols-12 gap-6">
        {/* Left Side - Tabs and Content */}
        <div className="col-span-9">
          {/* Tab Navigation */}
          <div className="border-b border-gray-200 mb-6">
            <nav className="flex gap-6">
              {tabs.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center gap-2 pb-3 border-b-2 transition-colors ${
                    activeTab === tab.id
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                >
                  {tab.icon}
                  <span>{tab.label}</span>
                  {tab.count !== undefined && (
                    <span
                      className={`px-2 py-0.5 text-xs rounded-full ${
                        tab.id === 'dead_letter' && tab.count > 0
                          ? 'bg-red-100 text-red-700'
                          : 'bg-gray-100 text-gray-600'
                      }`}
                    >
                      {tab.count}
                    </span>
                  )}
                </button>
              ))}
            </nav>
          </div>

          {/* Tab Content */}
          <div>
            {loading ? (
              <div className="flex items-center justify-center p-12">
                <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
              </div>
            ) : (
              <>
                {activeTab === 'priority' && (
                  <PriorityQueuesTab queues={priorityQueues} onJobClick={handleJobClick} />
                )}
                {activeTab === 'all' && (
                  <AllJobsTab jobs={allJobs} onJobClick={handleJobClick} />
                )}
                {activeTab === 'dead_letter' && (
                  <DeadLetterTab
                    jobs={deadLetterJobs}
                    onJobClick={handleJobClick}
                    onRetry={handleRetryJob}
                    onDelete={handleDeleteJob}
                  />
                )}
                {activeTab === 'rate_limits' && (
                  <RateLimitsTab
                    rateLimits={rateLimits}
                    onUpdateLimit={handleUpdateRateLimit}
                    onToggleEnabled={handleToggleRateLimit}
                  />
                )}
              </>
            )}
          </div>
        </div>

        {/* Right Side - Operations Panel */}
        <div className="col-span-3">
          <QueueOperationsPanel
            onPauseQueue={handlePauseQueue}
            onResumeQueue={handleResumeQueue}
            onFlushQueue={handleFlushQueue}
            onRetryAllFailed={handleRetryAllFailed}
            onClearDeadLetter={handleClearDeadLetter}
          />
        </div>
      </div>

      {/* Modals */}
      <SubmitJobModal
        isOpen={showSubmitModal}
        onClose={() => setShowSubmitModal(false)}
        onSubmit={handleSubmitJob}
      />
      <JobDetailsModal
        job={selectedJob}
        onClose={() => setSelectedJob(null)}
        onRetry={handleRetryJob}
        onCancel={handleCancelJob}
      />
    </div>
  );
}
