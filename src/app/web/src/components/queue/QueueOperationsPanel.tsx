import { useState } from 'react';
import {
  Pause,
  Play,
  RotateCcw,
  Trash2,
  AlertTriangle,
  Settings,
  Power,
  RefreshCw,
} from 'lucide-react';

interface QueueOperationsPanelProps {
  onPauseQueue: (priority?: string) => void;
  onResumeQueue: (priority?: string) => void;
  onFlushQueue: (priority?: string) => void;
  onRetryAllFailed: () => void;
  onClearDeadLetter: () => void;
}

export function QueueOperationsPanel({
  onPauseQueue,
  onResumeQueue,
  onFlushQueue,
  onRetryAllFailed,
  onClearDeadLetter,
}: QueueOperationsPanelProps) {
  const [selectedPriority, setSelectedPriority] = useState<string>('all');
  const [showConfirm, setShowConfirm] = useState<string | null>(null);
  const [isPaused, setIsPaused] = useState(false);

  const handlePauseToggle = () => {
    if (isPaused) {
      onResumeQueue(selectedPriority === 'all' ? undefined : selectedPriority);
    } else {
      onPauseQueue(selectedPriority === 'all' ? undefined : selectedPriority);
    }
    setIsPaused(!isPaused);
  };

  const handleFlush = () => {
    setShowConfirm('flush');
  };

  const confirmFlush = () => {
    onFlushQueue(selectedPriority === 'all' ? undefined : selectedPriority);
    setShowConfirm(null);
  };

  const handleClearDLQ = () => {
    setShowConfirm('dlq');
  };

  const confirmClearDLQ = () => {
    onClearDeadLetter();
    setShowConfirm(null);
  };

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <div className="flex items-center gap-2 mb-4">
        <Settings className="w-5 h-5 text-gray-500" />
        <h3 className="font-medium text-gray-900">Queue Operations</h3>
      </div>

      {/* Priority Selector */}
      <div className="mb-4">
        <label className="block text-sm text-gray-600 mb-2">Target Queue</label>
        <select
          value={selectedPriority}
          onChange={(e) => setSelectedPriority(e.target.value)}
          className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">All Queues</option>
          <option value="P0">P0 - Critical</option>
          <option value="P1">P1 - Manual</option>
          <option value="P2">P2 - CI/CD</option>
          <option value="P3">P3 - Batch</option>
        </select>
      </div>

      {/* Operation Buttons */}
      <div className="grid grid-cols-2 gap-3 mb-4">
        {/* Pause/Resume */}
        <button
          onClick={handlePauseToggle}
          className={`flex items-center justify-center gap-2 px-4 py-3 rounded-lg font-medium transition-colors ${
            isPaused
              ? 'bg-green-100 text-green-700 hover:bg-green-200'
              : 'bg-yellow-100 text-yellow-700 hover:bg-yellow-200'
          }`}
        >
          {isPaused ? (
            <>
              <Play className="w-4 h-4" />
              Resume
            </>
          ) : (
            <>
              <Pause className="w-4 h-4" />
              Pause
            </>
          )}
        </button>

        {/* Flush Queue */}
        <button
          onClick={handleFlush}
          className="flex items-center justify-center gap-2 px-4 py-3 bg-red-100 text-red-700 rounded-lg font-medium hover:bg-red-200 transition-colors"
        >
          <Trash2 className="w-4 h-4" />
          Flush
        </button>

        {/* Retry All Failed */}
        <button
          onClick={onRetryAllFailed}
          className="flex items-center justify-center gap-2 px-4 py-3 bg-blue-100 text-blue-700 rounded-lg font-medium hover:bg-blue-200 transition-colors"
        >
          <RotateCcw className="w-4 h-4" />
          Retry Failed
        </button>

        {/* Clear Dead Letter */}
        <button
          onClick={handleClearDLQ}
          className="flex items-center justify-center gap-2 px-4 py-3 bg-gray-100 text-gray-700 rounded-lg font-medium hover:bg-gray-200 transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Clear DLQ
        </button>
      </div>

      {/* Status Indicator */}
      <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
        <div className="flex items-center gap-2">
          <Power className={`w-4 h-4 ${isPaused ? 'text-yellow-500' : 'text-green-500'}`} />
          <span className="text-sm text-gray-600">
            Queue Status: <span className="font-medium">{isPaused ? 'Paused' : 'Active'}</span>
          </span>
        </div>
        <div
          className={`w-2 h-2 rounded-full ${
            isPaused ? 'bg-yellow-500 animate-pulse' : 'bg-green-500'
          }`}
        />
      </div>

      {/* Confirmation Dialog */}
      {showConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 max-w-md mx-4">
            <div className="flex items-center gap-3 mb-4">
              <div className="p-2 bg-red-100 rounded-full">
                <AlertTriangle className="w-6 h-6 text-red-600" />
              </div>
              <h3 className="text-lg font-medium text-gray-900">
                {showConfirm === 'flush' ? 'Flush Queue?' : 'Clear Dead Letter Queue?'}
              </h3>
            </div>
            <p className="text-sm text-gray-600 mb-6">
              {showConfirm === 'flush'
                ? `This will remove all pending jobs from ${
                    selectedPriority === 'all' ? 'all queues' : `the ${selectedPriority} queue`
                  }. This action cannot be undone.`
                : 'This will permanently delete all failed jobs from the dead letter queue. This action cannot be undone.'}
            </p>
            <div className="flex items-center justify-end gap-3">
              <button
                onClick={() => setShowConfirm(null)}
                className="px-4 py-2 text-gray-600 hover:text-gray-800 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={showConfirm === 'flush' ? confirmFlush : confirmClearDLQ}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
              >
                {showConfirm === 'flush' ? 'Flush Queue' : 'Clear DLQ'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
