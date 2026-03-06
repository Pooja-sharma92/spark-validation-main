import { useState } from 'react';
import { Sidebar } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { DashboardPage } from './components/pages/DashboardPage';
import { JobManagementPage } from './components/pages/JobManagementPage';
import { DataValidationPage } from './components/pages/DataValidationPage';
import { BatchManagementPage } from './components/pages/BatchManagementPage';
import { SettingsPage } from './components/pages/SettingsPage';
import { QueueMonitorPage } from './components/pages/QueueMonitorPage';
import { CategoryManagementPage } from './components/pages/CategoryManagementPage';

export type Page = 'dashboard' | 'jobs' | 'validation' | 'batches' | 'queue' | 'categories' | 'settings';

export default function App() {
  const [currentPage, setCurrentPage] = useState<Page>('dashboard');
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const renderPage = () => {
    switch (currentPage) {
      case 'dashboard':
        return <DashboardPage onNavigateToJobs={() => setCurrentPage('jobs')} />;
      case 'jobs':
        return <JobManagementPage />;
      case 'validation':
        return <DataValidationPage />;
      case 'batches':
        return <BatchManagementPage />;
      case 'queue':
        return <QueueMonitorPage />;
      case 'categories':
        return <CategoryManagementPage />;
      case 'settings':
        return <SettingsPage />;
      default:
        return <DashboardPage onNavigateToJobs={() => setCurrentPage('jobs')} />;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 flex">
      <Sidebar
        currentPage={currentPage}
        onNavigate={setCurrentPage}
        collapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
      />
      <div className="flex-1 flex flex-col">
        <Header />
        <main className="flex-1 overflow-auto">
          {renderPage()}
        </main>
      </div>
    </div>
  );
}
