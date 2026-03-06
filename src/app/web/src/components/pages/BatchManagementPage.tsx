import { useState } from 'react';
import { mockBatches, mockJobs } from '../../data/migrationMockData';
import { Layers, Play, Pause, StopCircle, Plus, Calendar } from 'lucide-react';

export function BatchManagementPage() {
  const [selectedBatch, setSelectedBatch] = useState<string | null>(null);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active':
        return 'bg-blue-100 text-blue-700';
      case 'completed':
        return 'bg-green-100 text-green-700';
      case 'paused':
        return 'bg-yellow-100 text-yellow-700';
      case 'cancelled':
        return 'bg-red-100 text-red-700';
      default:
        return 'bg-gray-100 text-gray-700';
    }
  };

  return (
    <div className="p-6">
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-gray-900 mb-2">Batch Management</h1>
            <p className="text-gray-600">Organize and manage migration jobs in batches</p>
          </div>
          <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2">
            <Plus className="w-4 h-4" />
            Create Batch
          </button>
        </div>
      </div>

      {/* Batch Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-6">
        {mockBatches.map((batch) => {
          const completionRate = (batch.completedJobs / batch.totalJobs) * 100;
          const successRate = batch.completedJobs > 0
            ? ((batch.completedJobs - batch.failedJobs) / batch.completedJobs) * 100
            : 0;

          return (
            <div
              key={batch.id}
              className={`bg-white rounded-lg shadow-sm border-2 transition-all cursor-pointer ${
                selectedBatch === batch.id
                  ? 'border-blue-500'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
              onClick={() => setSelectedBatch(batch.id)}
            >
              <div className="p-6">
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-blue-100 rounded-lg">
                      <Layers className="w-5 h-5 text-blue-600" />
                    </div>
                    <div>
                      <h3 className="text-gray-900">{batch.name}</h3>
                      <p className="text-sm text-gray-600">{batch.description}</p>
                    </div>
                  </div>
                </div>

                <div className="mb-4">
                  <div className="flex items-center justify-between text-sm mb-2">
                    <span className="text-gray-600">Progress</span>
                    <span className="text-gray-900">
                      {batch.completedJobs} / {batch.totalJobs}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div
                      className="bg-blue-600 h-2 rounded-full transition-all"
                      style={{ width: `${completionRate}%` }}
                    ></div>
                  </div>
                  <div className="text-sm text-gray-600 mt-1">
                    {completionRate.toFixed(1)}% complete
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4 mb-4">
                  <div className="bg-gray-50 rounded-lg p-3">
                    <div className="text-xs text-gray-600 mb-1">Success Rate</div>
                    <div className="text-green-700">{successRate.toFixed(1)}%</div>
                  </div>
                  <div className="bg-gray-50 rounded-lg p-3">
                    <div className="text-xs text-gray-600 mb-1">Failed Jobs</div>
                    <div className="text-red-700">{batch.failedJobs}</div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <span className={`px-2 py-1 rounded text-xs ${getStatusColor(batch.status)}`}>
                    {batch.status.charAt(0).toUpperCase() + batch.status.slice(1)}
                  </span>
                  <div className="flex items-center gap-1 text-xs text-gray-500">
                    <Calendar className="w-3 h-3" />
                    {batch.createdAt}
                  </div>
                </div>

                <div className="mt-4 pt-4 border-t border-gray-200 flex gap-2">
                  {batch.status === 'active' ? (
                    <>
                      <button className="flex-1 px-3 py-2 bg-yellow-100 text-yellow-700 rounded hover:bg-yellow-200 transition-colors text-sm flex items-center justify-center gap-1">
                        <Pause className="w-4 h-4" />
                        Pause
                      </button>
                      <button className="flex-1 px-3 py-2 bg-red-100 text-red-700 rounded hover:bg-red-200 transition-colors text-sm flex items-center justify-center gap-1">
                        <StopCircle className="w-4 h-4" />
                        Stop
                      </button>
                    </>
                  ) : batch.status === 'paused' ? (
                    <button className="flex-1 px-3 py-2 bg-blue-100 text-blue-700 rounded hover:bg-blue-200 transition-colors text-sm flex items-center justify-center gap-1">
                      <Play className="w-4 h-4" />
                      Resume
                    </button>
                  ) : null}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Batch Details */}
      {selectedBatch && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          {(() => {
            const batch = mockBatches.find(b => b.id === selectedBatch);
            if (!batch) return null;

            // For demo, show some sample jobs that would belong to this batch
            const batchJobs = mockJobs.slice(0, 5);

            return (
              <>
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-gray-900">Batch Details: {batch.name}</h2>
                  <button
                    onClick={() => setSelectedBatch(null)}
                    className="text-sm text-gray-600 hover:text-gray-900"
                  >
                    Close
                  </button>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                  <div className="p-4 bg-gray-50 rounded-lg">
                    <div className="text-sm text-gray-600 mb-1">Total Jobs</div>
                    <div className="text-gray-900">{batch.totalJobs}</div>
                  </div>
                  <div className="p-4 bg-gray-50 rounded-lg">
                    <div className="text-sm text-gray-600 mb-1">Completed</div>
                    <div className="text-green-700">{batch.completedJobs}</div>
                  </div>
                  <div className="p-4 bg-gray-50 rounded-lg">
                    <div className="text-sm text-gray-600 mb-1">Running</div>
                    <div className="text-blue-700">{batch.totalJobs - batch.completedJobs - batch.failedJobs}</div>
                  </div>
                  <div className="p-4 bg-gray-50 rounded-lg">
                    <div className="text-sm text-gray-600 mb-1">Failed</div>
                    <div className="text-red-700">{batch.failedJobs}</div>
                  </div>
                </div>

                <div>
                  <h3 className="text-gray-900 mb-4">Jobs in this Batch (Sample)</h3>
                  <div className="space-y-2">
                    {batchJobs.map((job) => (
                      <div key={job.id} className="p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors">
                        <div className="flex items-center justify-between">
                          <div>
                            <div className="text-gray-900">{job.name}</div>
                            <div className="text-sm text-gray-600">
                              {job.sourceTable} → {job.targetTable}
                            </div>
                          </div>
                          <div className="flex items-center gap-4">
                            <div className="text-right">
                              <div className="text-sm text-gray-900">{job.progress}%</div>
                              <div className="text-xs text-gray-600">{job.status}</div>
                            </div>
                            <div className="w-24 bg-gray-200 rounded-full h-2">
                              <div
                                className={`h-2 rounded-full ${
                                  job.status === 'completed' ? 'bg-green-600' : 'bg-blue-600'
                                }`}
                                style={{ width: `${job.progress}%` }}
                              ></div>
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}
