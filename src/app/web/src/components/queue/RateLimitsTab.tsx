import { RateLimit } from '../../types/queue';
import { Gauge, Clock, AlertTriangle, CheckCircle } from 'lucide-react';

interface RateLimitsTabProps {
  rateLimits: Record<string, RateLimit>;
  onUpdateLimit: (priority: string, newLimit: number) => void;
  onToggleEnabled: (priority: string, enabled: boolean) => void;
}

export function RateLimitsTab({ rateLimits, onUpdateLimit, onToggleEnabled }: RateLimitsTabProps) {
  const getPriorityConfig = (priority: string) => {
    switch (priority) {
      case 'P0':
        return { name: 'CRITICAL', color: 'red', bgLight: 'bg-red-50', bgDark: 'bg-red-600' };
      case 'P1':
        return { name: 'MANUAL', color: 'orange', bgLight: 'bg-orange-50', bgDark: 'bg-orange-600' };
      case 'P2':
        return { name: 'CI/CD', color: 'blue', bgLight: 'bg-blue-50', bgDark: 'bg-blue-600' };
      case 'P3':
        return { name: 'BATCH', color: 'gray', bgLight: 'bg-gray-50', bgDark: 'bg-gray-600' };
      default:
        return { name: 'UNKNOWN', color: 'gray', bgLight: 'bg-gray-50', bgDark: 'bg-gray-600' };
    }
  };

  const getUsagePercent = (rateLimit: RateLimit) => {
    return Math.round((rateLimit.currentRate / rateLimit.limit) * 100);
  };

  const getUsageColor = (percent: number) => {
    if (percent >= 90) return 'text-red-600';
    if (percent >= 70) return 'text-orange-600';
    return 'text-green-600';
  };

  const getProgressColor = (percent: number) => {
    if (percent >= 90) return 'bg-red-500';
    if (percent >= 70) return 'bg-orange-500';
    return 'bg-green-500';
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-medium text-gray-900">Rate Limit Configuration</h3>
          <p className="text-sm text-gray-500">
            Configure rate limits per priority queue to prevent system overload
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className="flex items-center gap-1 text-sm text-gray-500">
            <CheckCircle className="w-4 h-4 text-green-500" />
            All limits healthy
          </span>
        </div>
      </div>

      {/* Rate Limit Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {Object.entries(rateLimits).map(([priority, rateLimit]) => {
          const config = getPriorityConfig(priority);
          const usagePercent = getUsagePercent(rateLimit);

          return (
            <div
              key={priority}
              className={`border border-gray-200 rounded-lg overflow-hidden ${
                !rateLimit.enabled ? 'opacity-60' : ''
              }`}
            >
              {/* Card Header */}
              <div className={`p-4 ${config.bgLight} border-b border-gray-200`}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <span className={`px-3 py-1 text-white text-sm rounded ${config.bgDark}`}>
                      {priority} {config.name}
                    </span>
                    {usagePercent >= 80 && (
                      <AlertTriangle className="w-4 h-4 text-orange-500" />
                    )}
                  </div>
                  <label className="flex items-center gap-2 cursor-pointer">
                    <span className="text-xs text-gray-500">Enabled</span>
                    <div className="relative">
                      <input
                        type="checkbox"
                        checked={rateLimit.enabled}
                        onChange={(e) => onToggleEnabled(priority, e.target.checked)}
                        className="sr-only"
                      />
                      <div
                        className={`w-10 h-5 rounded-full transition-colors ${
                          rateLimit.enabled ? 'bg-blue-500' : 'bg-gray-300'
                        }`}
                      >
                        <div
                          className={`w-4 h-4 bg-white rounded-full shadow transform transition-transform ${
                            rateLimit.enabled ? 'translate-x-5' : 'translate-x-0.5'
                          } mt-0.5`}
                        />
                      </div>
                    </div>
                  </label>
                </div>
              </div>

              {/* Card Body */}
              <div className="p-4 bg-white">
                {/* Usage Bar */}
                <div className="mb-4">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm text-gray-600">Current Usage</span>
                    <span className={`text-sm font-medium ${getUsageColor(usagePercent)}`}>
                      {usagePercent}%
                    </span>
                  </div>
                  <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${getProgressColor(usagePercent)}`}
                      style={{ width: `${usagePercent}%` }}
                    />
                  </div>
                </div>

                {/* Stats Grid */}
                <div className="grid grid-cols-2 gap-4 mb-4">
                  <div className="flex items-center gap-2">
                    <Gauge className="w-4 h-4 text-gray-400" />
                    <div>
                      <div className="text-lg font-medium text-gray-900">
                        {rateLimit.currentRate}/{rateLimit.limit}
                      </div>
                      <div className="text-xs text-gray-500">requests / {rateLimit.window}s</div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <Clock className="w-4 h-4 text-gray-400" />
                    <div>
                      <div className="text-lg font-medium text-gray-900">{rateLimit.resetIn}s</div>
                      <div className="text-xs text-gray-500">until reset</div>
                    </div>
                  </div>
                </div>

                {/* Token Status */}
                <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <span className="text-sm text-gray-600">Remaining Tokens</span>
                  <span
                    className={`px-2 py-1 rounded text-sm font-medium ${
                      rateLimit.remainingTokens <= 2
                        ? 'bg-red-100 text-red-700'
                        : rateLimit.remainingTokens <= 5
                        ? 'bg-yellow-100 text-yellow-700'
                        : 'bg-green-100 text-green-700'
                    }`}
                  >
                    {rateLimit.remainingTokens} available
                  </span>
                </div>

                {/* Limit Adjustment */}
                <div className="mt-4">
                  <label className="block text-sm text-gray-600 mb-2">Max Requests per Window</label>
                  <div className="flex items-center gap-2">
                    <input
                      type="range"
                      min="1"
                      max="50"
                      value={rateLimit.limit}
                      onChange={(e) => onUpdateLimit(priority, parseInt(e.target.value))}
                      className="flex-1 h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
                      disabled={!rateLimit.enabled}
                    />
                    <input
                      type="number"
                      min="1"
                      max="50"
                      value={rateLimit.limit}
                      onChange={(e) => onUpdateLimit(priority, parseInt(e.target.value))}
                      className="w-16 px-2 py-1 border border-gray-200 rounded text-center"
                      disabled={!rateLimit.enabled}
                    />
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Info Box */}
      <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <h4 className="text-sm font-medium text-blue-900 mb-2">About Rate Limiting</h4>
        <p className="text-sm text-blue-800">
          Rate limits use a token bucket algorithm. Each priority queue has its own bucket with a
          configurable maximum capacity and refill rate. When a queue is rate-limited, new jobs
          will be delayed until tokens become available. P0 (Critical) jobs always have the highest
          priority and shortest delays.
        </p>
      </div>
    </div>
  );
}
