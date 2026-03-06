import {
  CheckCircle2,
  XCircle,
  Clock,
  Play,
  AlertTriangle,
  TrendingUp,
  BarChart3,
} from 'lucide-react';
import { mockJobs, mockBatches } from '../../data/migrationMockData';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';

interface DashboardPageProps {
  onNavigateToJobs: () => void;
}

export function DashboardPage({ onNavigateToJobs }: DashboardPageProps) {
  const completedJobs = mockJobs.filter(j => j.status === 'completed').length;
  const failedJobs = mockJobs.filter(j => j.status === 'failed').length;
  const runningJobs = mockJobs.filter(j => j.status === 'running').length;
  const pendingJobs = mockJobs.filter(j => j.status === 'pending').length;

  const totalSourceRows = mockJobs.reduce((sum, j) => sum + j.sourceRowCount, 0);
  const totalTargetRows = mockJobs.reduce((sum, j) => sum + j.targetRowCount, 0);
  const totalMismatches = mockJobs.reduce((sum, j) => sum + j.mismatchCount, 0);

  const statusData = [
    { name: 'Completed', value: completedJobs, color: '#10b981' },
    { name: 'Running', value: runningJobs, color: '#3b82f6' },
    { name: 'Failed', value: failedJobs, color: '#ef4444' },
    { name: 'Pending', value: pendingJobs, color: '#6b7280' },
  ];

  const recentIssues = mockJobs
    .filter(j => j.status === 'failed' || j.mismatchCount > 0)
    .slice(0, 5);

  const activeBatches = mockBatches.filter(b => b.status === 'active');

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-gray-900 mb-2">Migration Monitoring Dashboard</h1>
        <p className="text-gray-600">Real-time overview of data migration status</p>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="p-3 bg-green-100 rounded-lg">
              <CheckCircle2 className="w-6 h-6 text-green-600" />
            </div>
          </div>
          <div className="text-gray-900">{completedJobs}</div>
          <div className="text-gray-600">Completed Jobs</div>
          <div className="text-sm text-green-600 mt-2">
            {totalSourceRows > 0
              ? `${((totalTargetRows / totalSourceRows) * 100).toFixed(1)}% migrated`
              : '0% migrated'}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="p-3 bg-blue-100 rounded-lg">
              <Play className="w-6 h-6 text-blue-600" />
            </div>
          </div>
          <div className="text-gray-900">{runningJobs}</div>
          <div className="text-gray-600">Running Jobs</div>
          <div className="text-sm text-blue-600 mt-2">In progress</div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="p-3 bg-red-100 rounded-lg">
              <XCircle className="w-6 h-6 text-red-600" />
            </div>
          </div>
          <div className="text-gray-900">{failedJobs}</div>
          <div className="text-gray-600">Failed Jobs</div>
          <div className="text-sm text-red-600 mt-2">Requires attention</div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="p-3 bg-gray-100 rounded-lg">
              <Clock className="w-6 h-6 text-gray-600" />
            </div>
          </div>
          <div className="text-gray-900">{pendingJobs}</div>
          <div className="text-gray-600">Pending Jobs</div>
          <div className="text-sm text-gray-600 mt-2">Queued</div>
        </div>
      </div>

      {/* Data Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center gap-2 mb-3">
            <BarChart3 className="w-5 h-5 text-gray-600" />
            <h3 className="text-gray-900">Total Records</h3>
          </div>
          <div className="text-gray-900">{totalSourceRows.toLocaleString()}</div>
          <div className="text-sm text-gray-600">Source records</div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center gap-2 mb-3">
            <TrendingUp className="w-5 h-5 text-green-600" />
            <h3 className="text-gray-900">Migrated Records</h3>
          </div>
          <div className="text-gray-900">{totalTargetRows.toLocaleString()}</div>
          <div className="text-sm text-gray-600">
            {totalSourceRows > 0
              ? `${((totalTargetRows / totalSourceRows) * 100).toFixed(2)}%`
              : '0%'}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center gap-2 mb-3">
            <AlertTriangle className="w-5 h-5 text-yellow-600" />
            <h3 className="text-gray-900">Data Mismatches</h3>
          </div>
          <div className="text-gray-900">{totalMismatches.toLocaleString()}</div>
          <div className="text-sm text-yellow-600">Validation issues</div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Job Status Distribution */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h2 className="text-gray-900 mb-4">Job Status Distribution</h2>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie
                data={statusData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, value }) => `${name}: ${value}`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="value"
              >
                {statusData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Active Batches */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-gray-900">Active Batches</h2>
            <span className="text-sm text-gray-600">{activeBatches.length} active</span>
          </div>
          <div className="space-y-3">
            {activeBatches.map(batch => {
              const completionRate = (batch.completedJobs / batch.totalJobs) * 100;
              return (
                <div key={batch.id} className="p-4 border border-gray-200 rounded-lg">
                  <div className="flex items-center justify-between mb-2">
                    <div className="text-gray-900">{batch.name}</div>
                    <span className="text-sm text-gray-600">
                      {batch.completedJobs}/{batch.totalJobs}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2 mb-2">
                    <div
                      className="bg-blue-600 h-2 rounded-full transition-all"
                      style={{ width: `${completionRate}%` }}
                    ></div>
                  </div>
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-600">{completionRate.toFixed(1)}% complete</span>
                    {batch.failedJobs > 0 && (
                      <span className="text-red-600">{batch.failedJobs} failed</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Recent Issues */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-gray-900">Recent Issues & Alerts</h2>
          <button
            onClick={onNavigateToJobs}
            className="text-sm text-blue-600 hover:text-blue-700"
          >
            View All Jobs →
          </button>
        </div>
        <div className="space-y-3">
          {recentIssues.map(job => (
            <div key={job.id} className="flex items-start gap-3 p-4 bg-gray-50 rounded-lg border border-gray-200">
              {job.status === 'failed' ? (
                <XCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
              ) : (
                <AlertTriangle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
              )}
              <div className="flex-1">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-gray-900">{job.name}</span>
                  <span className={`text-xs px-2 py-1 rounded ${
                    job.status === 'failed'
                      ? 'bg-red-100 text-red-700'
                      : 'bg-yellow-100 text-yellow-700'
                  }`}>
                    {job.status === 'failed' ? 'Failed' : `${job.mismatchCount} mismatches`}
                  </span>
                </div>
                <div className="text-sm text-gray-600">
                  {job.sourceTable} → {job.targetTable}
                </div>
                {job.error && (
                  <div className="text-sm text-red-600 mt-1">{job.error}</div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
