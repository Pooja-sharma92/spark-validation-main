import { useState, useEffect } from 'react';
import {
  FolderTree,
  Plus,
  Edit2,
  Trash2,
  Check,
  X,
  RefreshCw,
  ChevronRight,
  ChevronDown,
  Tag,
  Folder,
  Layers,
  AlertCircle,
} from 'lucide-react';

interface Category {
  id: string;
  type: 'domain' | 'module' | 'job_group';
  name: string;
  description?: string;
  parentId?: string;
  aiDiscovered: boolean;
  approved: boolean;
  children?: Category[];
}

interface SuggestedCategory {
  id: string;
  type: 'domain' | 'module' | 'job_group';
  name: string;
  parentName?: string;
  occurrenceCount: number;
  suggestedByJobs: string[];
  status: string;
}

const API_BASE = 'http://localhost:3801/api';

export function CategoryManagementPage() {
  const [categories, setCategories] = useState<Category[]>([]);
  const [suggestions, setSuggestions] = useState<SuggestedCategory[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());

  // Modal state
  const [showModal, setShowModal] = useState(false);
  const [modalMode, setModalMode] = useState<'create' | 'edit'>('create');
  const [editingCategory, setEditingCategory] = useState<Category | null>(null);

  // Form state
  const [formType, setFormType] = useState<'domain' | 'module' | 'job_group'>('domain');
  const [formName, setFormName] = useState('');
  const [formDescription, setFormDescription] = useState('');
  const [formParentId, setFormParentId] = useState('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      const [catRes, sugRes] = await Promise.all([
        fetch(`${API_BASE}/categories/tree`),
        fetch(`${API_BASE}/categories/suggested`),
      ]);

      if (catRes.ok) {
        const data = await catRes.json();
        setCategories(data.data || []);
        // Expand all by default
        const allIds = new Set<string>();
        const collectIds = (cats: Category[]) => {
          cats.forEach(c => {
            allIds.add(c.id);
            if (c.children) collectIds(c.children);
          });
        };
        collectIds(data.data || []);
        setExpandedNodes(allIds);
      }

      if (sugRes.ok) {
        const data = await sugRes.json();
        setSuggestions(data.data || []);
      }
    } catch (err) {
      console.error('Failed to fetch categories:', err);
    } finally {
      setLoading(false);
    }
  };

  const openCreateModal = (type: 'domain' | 'module' | 'job_group' = 'domain', parentId?: string) => {
    setModalMode('create');
    setFormType(type);
    setFormName('');
    setFormDescription('');
    setFormParentId(parentId || '');
    setEditingCategory(null);
    setShowModal(true);
  };

  const openEditModal = (category: Category) => {
    setModalMode('edit');
    setFormType(category.type);
    setFormName(category.name);
    setFormDescription(category.description || '');
    setFormParentId(category.parentId || '');
    setEditingCategory(category);
    setShowModal(true);
  };

  const saveCategory = async () => {
    try {
      setSaving(true);

      if (modalMode === 'create') {
        const res = await fetch(`${API_BASE}/categories`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: formType,
            name: formName,
            description: formDescription || undefined,
            parentId: formParentId || undefined,
          }),
        });

        if (!res.ok) throw new Error('Failed to create category');
      } else if (editingCategory) {
        const res = await fetch(`${API_BASE}/categories/${editingCategory.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: formName,
            description: formDescription || undefined,
            parentId: formParentId || undefined,
          }),
        });

        if (!res.ok) throw new Error('Failed to update category');
      }

      setShowModal(false);
      fetchData();
    } catch (err) {
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  const deleteCategory = async (id: string) => {
    if (!confirm('Are you sure you want to delete this category?')) return;

    try {
      const res = await fetch(`${API_BASE}/categories/${id}`, { method: 'DELETE' });
      if (res.ok) fetchData();
    } catch (err) {
      console.error('Failed to delete category:', err);
    }
  };

  const approveSuggestion = async (id: string) => {
    try {
      const res = await fetch(`${API_BASE}/categories/suggested/${id}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reviewedBy: 'admin' }),
      });
      if (res.ok) fetchData();
    } catch (err) {
      console.error('Failed to approve suggestion:', err);
    }
  };

  const rejectSuggestion = async (id: string) => {
    try {
      const res = await fetch(`${API_BASE}/categories/suggested/${id}/reject`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reviewedBy: 'admin' }),
      });
      if (res.ok) fetchData();
    } catch (err) {
      console.error('Failed to reject suggestion:', err);
    }
  };

  const toggleNode = (id: string) => {
    const newExpanded = new Set(expandedNodes);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpandedNodes(newExpanded);
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'domain': return <Folder className="w-4 h-4 text-blue-600" />;
      case 'module': return <Layers className="w-4 h-4 text-green-600" />;
      case 'job_group': return <Tag className="w-4 h-4 text-purple-600" />;
      default: return <Folder className="w-4 h-4" />;
    }
  };

  const getTypeLabel = (type: string) => {
    switch (type) {
      case 'domain': return 'Domain';
      case 'module': return 'Module';
      case 'job_group': return 'Job Group';
      default: return type;
    }
  };

  const renderCategoryNode = (category: Category, depth = 0) => {
    const hasChildren = category.children && category.children.length > 0;
    const isExpanded = expandedNodes.has(category.id);

    return (
      <div key={category.id}>
        <div
          className={`flex items-center gap-2 py-2 px-3 hover:bg-gray-50 rounded-lg group ${
            depth > 0 ? 'ml-6' : ''
          }`}
        >
          {hasChildren ? (
            <button onClick={() => toggleNode(category.id)} className="p-0.5">
              {isExpanded ? (
                <ChevronDown className="w-4 h-4 text-gray-400" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-400" />
              )}
            </button>
          ) : (
            <span className="w-5" />
          )}

          {getTypeIcon(category.type)}

          <span className="flex-1 font-medium text-gray-900">{category.name}</span>

          <span className="text-xs text-gray-400 uppercase">{getTypeLabel(category.type)}</span>

          {category.aiDiscovered && (
            <span className="px-1.5 py-0.5 bg-purple-100 text-purple-700 text-xs rounded">AI</span>
          )}

          <div className="opacity-0 group-hover:opacity-100 flex items-center gap-1">
            <button
              onClick={() => openEditModal(category)}
              className="p-1 hover:bg-gray-200 rounded"
            >
              <Edit2 className="w-4 h-4 text-gray-500" />
            </button>
            <button
              onClick={() => deleteCategory(category.id)}
              className="p-1 hover:bg-red-100 rounded"
            >
              <Trash2 className="w-4 h-4 text-red-500" />
            </button>
            {category.type === 'domain' && (
              <button
                onClick={() => openCreateModal('module', category.id)}
                className="p-1 hover:bg-green-100 rounded"
                title="Add Module"
              >
                <Plus className="w-4 h-4 text-green-600" />
              </button>
            )}
          </div>
        </div>

        {hasChildren && isExpanded && (
          <div>
            {category.children!.map(child => renderCategoryNode(child, depth + 1))}
          </div>
        )}
      </div>
    );
  };

  // Organize categories by type for display
  const domains = categories.filter(c => c.type === 'domain');
  const modules = categories.filter(c => c.type === 'module');
  const jobGroups = categories.filter(c => c.type === 'job_group');

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <FolderTree className="w-7 h-7 text-green-600" />
            Category Management
          </h1>
          <p className="text-gray-500 mt-1">
            Manage domains, modules, and job groups for classification
          </p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={fetchData}
            className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 flex items-center gap-2"
          >
            <RefreshCw className="w-4 h-4" />
            Refresh
          </button>
          <button
            onClick={() => openCreateModal('domain')}
            className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2"
          >
            <Plus className="w-4 h-4" />
            Add Category
          </button>
        </div>
      </div>

      {/* Pending Suggestions */}
      {suggestions.length > 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-xl p-4">
          <h2 className="font-semibold text-yellow-800 flex items-center gap-2 mb-3">
            <AlertCircle className="w-5 h-5" />
            AI Suggested Categories ({suggestions.length})
          </h2>
          <div className="space-y-2">
            {suggestions.map((sug) => (
              <div key={sug.id} className="flex items-center justify-between bg-white rounded-lg p-3 shadow-sm">
                <div className="flex items-center gap-3">
                  {getTypeIcon(sug.type)}
                  <div>
                    <p className="font-medium text-gray-900">{sug.name}</p>
                    <p className="text-xs text-gray-500">
                      {getTypeLabel(sug.type)} • Suggested {sug.occurrenceCount} time(s)
                      {sug.parentName && ` • Parent: ${sug.parentName}`}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => approveSuggestion(sug.id)}
                    className="p-2 bg-green-100 text-green-700 rounded-lg hover:bg-green-200"
                    title="Approve"
                  >
                    <Check className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => rejectSuggestion(sug.id)}
                    className="p-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200"
                    title="Reject"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Category Trees */}
      <div className="grid grid-cols-3 gap-6">
        {/* Domains */}
        <div className="bg-white rounded-xl shadow-sm border">
          <div className="p-4 border-b flex items-center justify-between">
            <h2 className="font-semibold text-gray-900 flex items-center gap-2">
              <Folder className="w-5 h-5 text-blue-600" />
              Domains
            </h2>
            <button
              onClick={() => openCreateModal('domain')}
              className="p-1 hover:bg-gray-100 rounded"
            >
              <Plus className="w-4 h-4 text-gray-500" />
            </button>
          </div>
          <div className="p-2 max-h-96 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <RefreshCw className="w-5 h-5 animate-spin text-gray-400" />
              </div>
            ) : domains.length === 0 ? (
              <p className="text-gray-500 text-center py-8">No domains yet</p>
            ) : (
              domains.map(cat => renderCategoryNode(cat))
            )}
          </div>
        </div>

        {/* Modules */}
        <div className="bg-white rounded-xl shadow-sm border">
          <div className="p-4 border-b flex items-center justify-between">
            <h2 className="font-semibold text-gray-900 flex items-center gap-2">
              <Layers className="w-5 h-5 text-green-600" />
              Modules
            </h2>
            <button
              onClick={() => openCreateModal('module')}
              className="p-1 hover:bg-gray-100 rounded"
            >
              <Plus className="w-4 h-4 text-gray-500" />
            </button>
          </div>
          <div className="p-2 max-h-96 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <RefreshCw className="w-5 h-5 animate-spin text-gray-400" />
              </div>
            ) : modules.length === 0 ? (
              <p className="text-gray-500 text-center py-8">No modules yet</p>
            ) : (
              modules.map(cat => renderCategoryNode(cat))
            )}
          </div>
        </div>

        {/* Job Groups */}
        <div className="bg-white rounded-xl shadow-sm border">
          <div className="p-4 border-b flex items-center justify-between">
            <h2 className="font-semibold text-gray-900 flex items-center gap-2">
              <Tag className="w-5 h-5 text-purple-600" />
              Job Groups
            </h2>
            <button
              onClick={() => openCreateModal('job_group')}
              className="p-1 hover:bg-gray-100 rounded"
            >
              <Plus className="w-4 h-4 text-gray-500" />
            </button>
          </div>
          <div className="p-2 max-h-96 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <RefreshCw className="w-5 h-5 animate-spin text-gray-400" />
              </div>
            ) : jobGroups.length === 0 ? (
              <p className="text-gray-500 text-center py-8">No job groups yet</p>
            ) : (
              jobGroups.map(cat => renderCategoryNode(cat))
            )}
          </div>
        </div>
      </div>

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-md p-6">
            <h2 className="text-xl font-bold mb-4">
              {modalMode === 'create' ? 'Add Category' : 'Edit Category'}
            </h2>

            <div className="space-y-4">
              {modalMode === 'create' && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Type</label>
                  <select
                    value={formType}
                    onChange={(e) => setFormType(e.target.value as 'domain' | 'module' | 'job_group')}
                    className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-green-500"
                  >
                    <option value="domain">Domain</option>
                    <option value="module">Module</option>
                    <option value="job_group">Job Group</option>
                  </select>
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
                <input
                  type="text"
                  value={formName}
                  onChange={(e) => setFormName(e.target.value)}
                  placeholder="e.g., Finance, Loan Processing"
                  className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-green-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Description (optional)
                </label>
                <textarea
                  value={formDescription}
                  onChange={(e) => setFormDescription(e.target.value)}
                  placeholder="Brief description of this category..."
                  rows={3}
                  className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-green-500"
                />
              </div>
            </div>

            <div className="flex justify-end gap-3 mt-6">
              <button
                onClick={() => setShowModal(false)}
                className="px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg"
              >
                Cancel
              </button>
              <button
                onClick={saveCategory}
                disabled={!formName.trim() || saving}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
              >
                {saving && <RefreshCw className="w-4 h-4 animate-spin" />}
                {modalMode === 'create' ? 'Create' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
