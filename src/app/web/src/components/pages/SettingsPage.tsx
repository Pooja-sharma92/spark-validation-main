import { useState } from 'react';
import { Save, Database, Bell, Shield, Clock, Mail } from 'lucide-react';

export function SettingsPage() {
  const [settings, setSettings] = useState({
    scanInterval: '6',
    autoScan: true,
    emailNotifications: true,
    slackNotifications: false,
    retentionDays: '90',
    maxConnections: '10',
  });

  const handleSave = () => {
    alert('Settings saved successfully');
    console.log('Save settings:', settings);
  };

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-gray-900 mb-2">System Settings</h1>
        <p className="text-gray-600">Configure database monitoring system parameters</p>
      </div>

      <div className="max-w-4xl space-y-6">
        {/* Scan Settings */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <Database className="w-5 h-5 text-gray-600" />
            <h2 className="text-gray-900">Scan Settings</h2>
          </div>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-3 border-b border-gray-200">
              <div>
                <div className="text-gray-900 mb-1">Auto Scan</div>
                <div className="text-sm text-gray-600">Automatically execute database scans periodically</div>
              </div>
              <label className="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  checked={settings.autoScan}
                  onChange={(e) => setSettings({ ...settings, autoScan: e.target.checked })}
                  className="sr-only peer"
                />
                <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
              </label>
            </div>

            <div>
              <label className="block text-gray-900 mb-2">Scan Interval (hours)</label>
              <input
                type="number"
                value={settings.scanInterval}
                onChange={(e) => setSettings({ ...settings, scanInterval: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                min="1"
                max="24"
              />
              <p className="text-sm text-gray-600 mt-1">Set the time interval for automatic scans</p>
            </div>

            <div>
              <label className="block text-gray-900 mb-2">Data Retention (days)</label>
              <input
                type="number"
                value={settings.retentionDays}
                onChange={(e) => setSettings({ ...settings, retentionDays: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                min="7"
                max="365"
              />
              <p className="text-sm text-gray-600 mt-1">Historical scan data retention period</p>
            </div>
          </div>
        </div>

        {/* Notification Settings */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <Bell className="w-5 h-5 text-gray-600" />
            <h2 className="text-gray-900">Notification Settings</h2>
          </div>

          <div className="space-y-4">
            <div className="flex items-center justify-between py-3 border-b border-gray-200">
              <div className="flex items-center gap-3">
                <Mail className="w-5 h-5 text-gray-500" />
                <div>
                  <div className="text-gray-900 mb-1">Email Notifications</div>
                  <div className="text-sm text-gray-600">Send email when changes are detected</div>
                </div>
              </div>
              <label className="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  checked={settings.emailNotifications}
                  onChange={(e) => setSettings({ ...settings, emailNotifications: e.target.checked })}
                  className="sr-only peer"
                />
                <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
              </label>
            </div>

            <div className="flex items-center justify-between py-3 border-b border-gray-200">
              <div className="flex items-center gap-3">
                <Bell className="w-5 h-5 text-gray-500" />
                <div>
                  <div className="text-gray-900 mb-1">Slack Notifications</div>
                  <div className="text-sm text-gray-600">Send notifications to Slack channel</div>
                </div>
              </div>
              <label className="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  checked={settings.slackNotifications}
                  onChange={(e) => setSettings({ ...settings, slackNotifications: e.target.checked })}
                  className="sr-only peer"
                />
                <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
              </label>
            </div>
          </div>
        </div>

        {/* Connection Settings */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center gap-3 mb-6">
            <Shield className="w-5 h-5 text-gray-600" />
            <h2 className="text-gray-900">Connection Settings</h2>
          </div>

          <div className="space-y-4">
            <div>
              <label className="block text-gray-900 mb-2">Max Concurrent Connections</label>
              <input
                type="number"
                value={settings.maxConnections}
                onChange={(e) => setSettings({ ...settings, maxConnections: e.target.value })}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                min="1"
                max="50"
              />
              <p className="text-sm text-gray-600 mt-1">Maximum database connections for simultaneous scanning</p>
            </div>

            <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
              <div className="flex items-start gap-3">
                <Clock className="w-5 h-5 text-blue-600 flex-shrink-0 mt-0.5" />
                <div>
                  <div className="text-sm text-blue-900 mb-1">Database Connection Configuration</div>
                  <div className="text-sm text-blue-700">
                    Configure database connection information to monitor. Supports MySQL, PostgreSQL, MongoDB and more.
                  </div>
                  <button className="mt-3 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm">
                    Configure Connections
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Save Button */}
        <div className="flex items-center justify-end gap-4">
          <button className="px-6 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors">
            Reset
          </button>
          <button
            onClick={handleSave}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
          >
            <Save className="w-4 h-4" />
            Save Settings
          </button>
        </div>
      </div>
    </div>
  );
}
