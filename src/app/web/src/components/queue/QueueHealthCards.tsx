import { QueueMetrics } from '../../types/queue';
import {
  Layers,
  Users,
  TrendingUp,
  Clock,
  AlertCircle,
} from 'lucide-react';

interface QueueHealthCardsProps {
  metrics: QueueMetrics;
}

export function QueueHealthCards({ metrics }: QueueHealthCardsProps) {
  const getBackpressureColor = () => {
    switch (metrics.backpressureLevel) {
      case 'normal':
        return 'bg-green-500';
      case 'warning':
        return 'bg-yellow-500';
      case 'critical':
        return 'bg-red-500';
      case 'rejecting':
        return 'bg-red-900';
      default:
        return 'bg-gray-500';
    }
  };

  const getBackpressureLabel = () => {
    switch (metrics.backpressureLevel) {
      case 'normal':
        return 'Normal';
      case 'warning':
        return 'Warning';
      case 'critical':
        return 'Critical';
      case 'rejecting':
        return 'Rejecting';
      default:
        return 'Unknown';
    }
  };

  const formatTime = (seconds: number) => {
    if (seconds < 60) {
      return `${seconds}s`;
    }
    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${minutes}m ${secs}s`;
  };

  const utilizationPercent = Math.round((metrics.activeWorkers / metrics.totalWorkers) * 100);
  const circumference = 2 * Math.PI * 40; // radius = 40
  const strokeDashoffset = circumference - (utilizationPercent / 100) * circumference;

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {/* Card 1 - Queue Depth */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-100 rounded-lg">
              <Layers className="w-5 h-5 text-blue-600" />
            </div>
            <h3 className="text-sm text-gray-600">Queue Depth</h3>
          </div>
          <div className="flex items-center gap-2">
            <span className={`w-2 h-2 ${getBackpressureColor()} rounded-full`}></span>
            <span className="text-xs text-gray-600">{getBackpressureLabel()}</span>
          </div>
        </div>

        <div className="mb-4">
          <div className="text-3xl text-gray-900 mb-1">{metrics.totalPending}</div>
          <p className="text-xs text-gray-500">Total pending jobs</p>
        </div>

        {/* Mini bar chart */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <div className="w-12 text-xs text-gray-600">P0</div>
            <div className="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
              <div
                className="bg-red-600 h-full rounded-full transition-all"
                style={{ width: `${(metrics.pendingByPriority.P0 / metrics.totalPending) * 100}%` }}
              ></div>
            </div>
            <div className="w-8 text-xs text-gray-900 text-right">{metrics.pendingByPriority.P0}</div>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-12 text-xs text-gray-600">P1</div>
            <div className="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
              <div
                className="bg-orange-600 h-full rounded-full transition-all"
                style={{ width: `${(metrics.pendingByPriority.P1 / metrics.totalPending) * 100}%` }}
              ></div>
            </div>
            <div className="w-8 text-xs text-gray-900 text-right">{metrics.pendingByPriority.P1}</div>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-12 text-xs text-gray-600">P2</div>
            <div className="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
              <div
                className="bg-blue-600 h-full rounded-full transition-all"
                style={{ width: `${(metrics.pendingByPriority.P2 / metrics.totalPending) * 100}%` }}
              ></div>
            </div>
            <div className="w-8 text-xs text-gray-900 text-right">{metrics.pendingByPriority.P2}</div>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-12 text-xs text-gray-600">P3</div>
            <div className="flex-1 bg-gray-100 rounded-full h-2 overflow-hidden">
              <div
                className="bg-gray-600 h-full rounded-full transition-all"
                style={{ width: `${(metrics.pendingByPriority.P3 / metrics.totalPending) * 100}%` }}
              ></div>
            </div>
            <div className="w-8 text-xs text-gray-900 text-right">{metrics.pendingByPriority.P3}</div>
          </div>
        </div>
      </div>

      {/* Card 2 - Active Workers */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-purple-100 rounded-lg">
            <Users className="w-5 h-5 text-purple-600" />
          </div>
          <h3 className="text-sm text-gray-600">Active Workers</h3>
        </div>

        <div className="flex items-center gap-6">
          {/* Circular Progress */}
          <div className="relative">
            <svg className="w-24 h-24 transform -rotate-90">
              {/* Background circle */}
              <circle
                cx="48"
                cy="48"
                r="40"
                stroke="#e5e7eb"
                strokeWidth="8"
                fill="none"
              />
              {/* Progress circle */}
              <circle
                cx="48"
                cy="48"
                r="40"
                stroke="#8b5cf6"
                strokeWidth="8"
                fill="none"
                strokeDasharray={circumference}
                strokeDashoffset={strokeDashoffset}
                strokeLinecap="round"
                className="transition-all duration-300"
              />
            </svg>
            <div className="absolute inset-0 flex flex-col items-center justify-center">
              <div className="text-xl text-gray-900">{utilizationPercent}%</div>
            </div>
          </div>

          {/* Stats */}
          <div className="flex-1">
            <div className="text-3xl text-gray-900 mb-1">
              {metrics.activeWorkers}/{metrics.totalWorkers}
            </div>
            <p className="text-xs text-gray-500 mb-3">slots utilized</p>
            <div className="text-xs text-gray-600">
              Concurrency Utilization
            </div>
          </div>
        </div>
      </div>

      {/* Card 3 - Throughput */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-green-100 rounded-lg">
            <TrendingUp className="w-5 h-5 text-green-600" />
          </div>
          <h3 className="text-sm text-gray-600">Throughput</h3>
        </div>

        <div className="mb-4">
          <div className="text-3xl text-gray-900 mb-1">{metrics.throughput}/min</div>
          <p className="text-xs text-gray-500">Jobs processed per minute</p>
        </div>

        {/* Sparkline */}
        <div className="mb-3">
          <svg className="w-full h-12" viewBox="0 0 300 48">
            <polyline
              points={metrics.throughputHistory.map((val, idx) => {
                const x = (idx / (metrics.throughputHistory.length - 1)) * 300;
                const y = 48 - ((val - 40) / (55 - 40)) * 48;
                return `${x},${y}`;
              }).join(' ')}
              fill="none"
              stroke="#10b981"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>

        {/* Comparison */}
        <div className={`text-sm flex items-center gap-1 ${
          metrics.throughputVsAverage >= 0 ? 'text-green-600' : 'text-red-600'
        }`}>
          <TrendingUp className="w-4 h-4" />
          <span>
            {metrics.throughputVsAverage >= 0 ? '+' : ''}
            {metrics.throughputVsAverage}% vs avg
          </span>
        </div>
      </div>

      {/* Card 4 - Oldest Pending */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-orange-100 rounded-lg">
            <Clock className="w-5 h-5 text-orange-600" />
          </div>
          <h3 className="text-sm text-gray-600">Oldest Pending</h3>
        </div>

        <div className="mb-4">
          <div className="flex items-center gap-2">
            <div className="text-3xl text-gray-900">{formatTime(metrics.oldestPendingAge)}</div>
            {metrics.oldestPendingAge > 300 && (
              <AlertCircle className="w-5 h-5 text-orange-600" />
            )}
          </div>
          <p className="text-xs text-gray-500 mt-1">Waiting in queue</p>
        </div>

        {metrics.oldestPendingPriority && (
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-600">Priority:</span>
            <span className={`px-2 py-0.5 rounded text-xs ${
              metrics.oldestPendingPriority === 'P0' ? 'bg-red-100 text-red-700' :
              metrics.oldestPendingPriority === 'P1' ? 'bg-orange-100 text-orange-700' :
              metrics.oldestPendingPriority === 'P2' ? 'bg-blue-100 text-blue-700' :
              'bg-gray-100 text-gray-700'
            }`}>
              {metrics.oldestPendingPriority}
            </span>
          </div>
        )}

        {metrics.oldestPendingAge > 300 && (
          <div className="mt-3 p-2 bg-orange-50 border border-orange-200 rounded text-xs text-orange-800">
            Job waiting over 5 minutes
          </div>
        )}
      </div>
    </div>
  );
}
