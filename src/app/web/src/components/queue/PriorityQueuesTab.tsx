import { PriorityQueueInfo, QueueJob } from '../../types/queue';
import {
  GitBranch,
  FileCode,
  User,
  Clock as ClockIcon,
  Calendar,
  GitCommit,
  Gauge,
} from 'lucide-react';

interface PriorityQueuesTabProps {
  queues: PriorityQueueInfo[];
  onJobClick: (job: QueueJob) => void;
}

export function PriorityQueuesTab({ queues, onJobClick }: PriorityQueuesTabProps) {
  const getTriggerIcon = (source: string) => {
    const iconClass = 'w-3 h-3';
    switch (source) {
      case 'webhook':
        return <GitBranch className={iconClass} />;
      case 'file':
        return <FileCode className={iconClass} />;
      case 'manual':
        return <User className={iconClass} />;
      case 'scheduled':
        return <Calendar className={iconClass} />;
      case 'ci_cd':
        return <GitCommit className={iconClass} />;
      default:
        return <GitBranch className={iconClass} />;
    }
  };

  const formatAge = (seconds: number) => {
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    return `${Math.floor(seconds / 3600)}h ago`;
  };

  return (
    <div className="space-y-6">
      {queues.map((queue) => (
        <div key={queue.priority} className="border border-gray-200 rounded-lg overflow-hidden">
          {/* Queue Header */}
          <div className={`px-4 py-3 flex items-center justify-between border-l-4 ${
            queue.priority === 'P0' ? 'border-red-600 bg-red-50' :
            queue.priority === 'P1' ? 'border-orange-600 bg-orange-50' :
            queue.priority === 'P2' ? 'border-blue-600 bg-blue-50' :
            'border-gray-600 bg-gray-50'
          }`}>
            <div className="flex items-center gap-3">
              <span className={`px-3 py-1 rounded text-white ${queue.badgeColor}`}>
                {queue.priority} {queue.name}
              </span>
              <span className="text-sm text-gray-600">{queue.description}</span>
              <span className="text-sm text-gray-900">
                <strong>{queue.pendingCount}</strong> pending
              </span>
            </div>

            {/* Rate Limit Status */}
            <div className="flex items-center gap-2">
              <Gauge className="w-4 h-4 text-gray-600" />
              <span className="text-sm text-gray-600">
                {queue.rateLimit.currentRate}/{queue.rateLimit.limit} req/{queue.rateLimit.window}s
              </span>
              <span className={`text-sm px-2 py-0.5 rounded ${
                queue.rateLimit.remainingTokens <= 2
                  ? 'bg-red-100 text-red-700'
                  : 'bg-green-100 text-green-700'
              }`}>
                {queue.rateLimit.remainingTokens} remaining
              </span>
            </div>
          </div>

          {/* Jobs List */}
          {queue.jobs.length > 0 ? (
            <div className="p-4 bg-white">
              <div className="flex gap-3 overflow-x-auto pb-2">
                {queue.jobs.map((job) => (
                  <button
                    key={job.id}
                    onClick={() => onJobClick(job)}
                    className="flex-shrink-0 w-72 p-3 border border-gray-200 rounded-lg hover:border-blue-400 hover:shadow-md transition-all bg-white text-left"
                  >
                    {/* Job Path */}
                    <div className="mb-2">
                      <div className="text-sm text-gray-900 font-mono truncate" title={job.jobPath}>
                        {job.jobPath.split('/').pop()}
                      </div>
                      <div className="text-xs text-gray-500 font-mono truncate">
                        {job.jobPath.split('/').slice(0, -1).join('/')}
                      </div>
                    </div>

                    {/* Job Info */}
                    <div className="flex items-center gap-2 mb-2">
                      {/* Status */}
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        job.status === 'pending' ? 'bg-gray-100 text-gray-700' :
                        job.status === 'running' ? 'bg-blue-100 text-blue-700' :
                        job.status === 'completed' ? 'bg-green-100 text-green-700' :
                        'bg-red-100 text-red-700'
                      }`}>
                        {job.status}
                      </span>

                      {/* Age */}
                      <span className="text-xs text-gray-600 flex items-center gap-1">
                        <ClockIcon className="w-3 h-3" />
                        {job.age ? formatAge(job.age) : 'just now'}
                      </span>
                    </div>

                    {/* Git Info */}
                    <div className="flex items-center gap-2 text-xs">
                      {/* Trigger Source */}
                      <div className="flex items-center gap-1 text-gray-600">
                        {getTriggerIcon(job.triggerSource)}
                        <span className="capitalize">{job.triggerSource.replace('_', ' ')}</span>
                      </div>

                      {/* Branch */}
                      {job.branch && (
                        <div className="flex items-center gap-1 text-gray-600">
                          <GitBranch className="w-3 h-3" />
                          <span className="truncate max-w-[100px]">{job.branch}</span>
                        </div>
                      )}

                      {/* Commit */}
                      {job.commitSha && (
                        <code className="text-xs bg-gray-100 px-1 rounded text-gray-700">
                          {job.commitSha}
                        </code>
                      )}
                    </div>
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div className="p-8 text-center text-gray-500 bg-white">
              <p className="text-sm">No pending jobs in this queue</p>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
