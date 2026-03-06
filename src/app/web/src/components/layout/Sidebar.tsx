import { Page } from '../../App';
import {
  LayoutDashboard,
  ListChecks,
  CheckCircle2,
  Layers,
  Settings,
  ChevronLeft,
  ChevronRight,
  ArrowRightLeft,
  Gauge,
  Tags,
} from 'lucide-react';

interface SidebarProps {
  currentPage: Page;
  onNavigate: (page: Page) => void;
  collapsed: boolean;
  onToggleCollapse: () => void;
}

const menuItems = [
  { id: 'dashboard' as Page, label: 'Dashboard', icon: LayoutDashboard },
  { id: 'jobs' as Page, label: 'Job Management', icon: ListChecks },
  { id: 'validation' as Page, label: 'Data Validation', icon: CheckCircle2 },
  { id: 'batches' as Page, label: 'Batch Management', icon: Layers },
  { id: 'queue' as Page, label: 'Queue Monitor', icon: Gauge },
  { id: 'categories' as Page, label: 'Categories', icon: Tags },
  { id: 'settings' as Page, label: 'Settings', icon: Settings },
];

export function Sidebar({ currentPage, onNavigate, collapsed, onToggleCollapse }: SidebarProps) {
  return (
    <div
      className={`bg-gray-900 text-white transition-all duration-300 flex flex-col ${
        collapsed ? 'w-16' : 'w-64'
      }`}
    >
      <div className="p-4 flex items-center justify-between border-b border-gray-800">
        {!collapsed && (
          <div className="flex items-center gap-2">
            <ArrowRightLeft className="w-6 h-6 text-blue-400" />
            <span className="text-white">Migration Monitor</span>
          </div>
        )}
        {collapsed && (
          <div className="flex items-center justify-center w-full">
            <ArrowRightLeft className="w-6 h-6 text-blue-400" />
          </div>
        )}
      </div>

      <nav className="flex-1 py-4">
        {menuItems.map((item) => {
          const Icon = item.icon;
          const isActive = currentPage === item.id;
          return (
            <button
              key={item.id}
              onClick={() => onNavigate(item.id)}
              className={`w-full flex items-center gap-3 px-4 py-3 transition-colors ${
                isActive
                  ? 'bg-blue-600 text-white border-l-4 border-blue-400'
                  : 'text-gray-300 hover:bg-gray-800 hover:text-white'
              }`}
              title={collapsed ? item.label : undefined}
            >
              <Icon className="w-5 h-5 flex-shrink-0" />
              {!collapsed && <span>{item.label}</span>}
            </button>
          );
        })}
      </nav>

      <button
        onClick={onToggleCollapse}
        className="p-4 border-t border-gray-800 flex items-center justify-center hover:bg-gray-800 transition-colors"
      >
        {collapsed ? (
          <ChevronRight className="w-5 h-5" />
        ) : (
          <ChevronLeft className="w-5 h-5" />
        )}
      </button>
    </div>
  );
}
