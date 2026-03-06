import { useState, useMemo } from 'react';
import { MigrationJob } from '../../types/migration';
import {
  Code2,
  Database,
  Settings,
  Copy,
  Check,
  Download,
  ChevronDown,
  ChevronRight,
  FileCode,
  FileText,
  Search,
  Maximize2,
  Minimize2,
} from 'lucide-react';

interface JobSourceCodeViewerProps {
  job: MigrationJob;
}

type CodeTab = 'sql' | 'python' | 'config';

interface TokenSpan {
  text: string;
  className: string;
}

export function JobSourceCodeViewer({ job }: JobSourceCodeViewerProps) {
  const [activeTab, setActiveTab] = useState<CodeTab>('sql');
  const [copied, setCopied] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [showLineNumbers, setShowLineNumbers] = useState(true);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['main']));

  const sourceCode = job.sourceCode;

  const tabs: { id: CodeTab; label: string; icon: React.ReactNode; available: boolean }[] = [
    {
      id: 'sql',
      label: 'SQL Queries',
      icon: <Database className="w-4 h-4" />,
      available: !!sourceCode?.sql,
    },
    {
      id: 'python',
      label: 'Python/PySpark',
      icon: <Code2 className="w-4 h-4" />,
      available: !!sourceCode?.python,
    },
    {
      id: 'config',
      label: 'Configuration',
      icon: <Settings className="w-4 h-4" />,
      available: !!sourceCode?.config,
    },
  ];

  const getCurrentCode = (): string => {
    if (!sourceCode) return '';
    switch (activeTab) {
      case 'sql':
        return sourceCode.sql || '';
      case 'python':
        return sourceCode.python || '';
      case 'config':
        return typeof sourceCode.config === 'object'
          ? JSON.stringify(sourceCode.config, null, 2)
          : sourceCode.config || '';
      default:
        return '';
    }
  };

  const handleCopy = async () => {
    const code = getCurrentCode();
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const code = getCurrentCode();
    const extensions: Record<CodeTab, string> = {
      sql: 'sql',
      python: 'py',
      config: 'json',
    };
    const blob = new Blob([code], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${job.name}_${activeTab}.${extensions[activeTab]}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const toggleSection = (sectionId: string) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(sectionId)) {
      newExpanded.delete(sectionId);
    } else {
      newExpanded.add(sectionId);
    }
    setExpandedSections(newExpanded);
  };

  // Tokenize line for syntax highlighting (safe approach without innerHTML)
  const tokenizeLine = (line: string, language: string): TokenSpan[] => {
    const tokens: TokenSpan[] = [];

    if (!line) {
      return [{ text: ' ', className: '' }];
    }

    // Simple tokenization - split by words and special characters
    const sqlKeywords = new Set([
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
      'ON', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'AS', 'GROUP', 'BY',
      'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'INSERT', 'UPDATE',
      'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW',
      'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'CASE', 'WHEN',
      'THEN', 'ELSE', 'END', 'CAST', 'COALESCE'
    ]);

    const pythonKeywords = new Set([
      'def', 'class', 'if', 'elif', 'else', 'for', 'while', 'try', 'except',
      'finally', 'with', 'as', 'import', 'from', 'return', 'yield', 'lambda',
      'and', 'or', 'not', 'in', 'is', 'None', 'True', 'False', 'self',
      'pass', 'break', 'continue', 'raise', 'assert', 'global', 'nonlocal',
      'async', 'await'
    ]);

    if (language === 'sql') {
      // Check for comments first
      if (line.trim().startsWith('--')) {
        return [{ text: line, className: 'text-gray-500' }];
      }

      // Split and tokenize
      const parts = line.split(/(\s+|'[^']*'|"[^"]*"|\(|\)|,|\.)/);
      for (const part of parts) {
        if (!part) continue;
        if (sqlKeywords.has(part.toUpperCase())) {
          tokens.push({ text: part, className: 'text-blue-400' });
        } else if (part.startsWith("'") || part.startsWith('"')) {
          tokens.push({ text: part, className: 'text-green-400' });
        } else {
          tokens.push({ text: part, className: 'text-gray-300' });
        }
      }
    } else if (language === 'python') {
      // Check for comments first
      const commentIndex = line.indexOf('#');
      const beforeComment = commentIndex >= 0 ? line.slice(0, commentIndex) : line;
      const comment = commentIndex >= 0 ? line.slice(commentIndex) : '';

      // Split and tokenize the non-comment part
      const parts = beforeComment.split(/(\s+|'[^']*'|"[^"]*"|\(|\)|,|\.|\[|\]|:)/);
      for (const part of parts) {
        if (!part) continue;
        if (pythonKeywords.has(part)) {
          tokens.push({ text: part, className: 'text-purple-400' });
        } else if (part.startsWith("'") || part.startsWith('"')) {
          tokens.push({ text: part, className: 'text-green-400' });
        } else if (/^\d+$/.test(part)) {
          tokens.push({ text: part, className: 'text-orange-400' });
        } else {
          tokens.push({ text: part, className: 'text-gray-300' });
        }
      }

      if (comment) {
        tokens.push({ text: comment, className: 'text-gray-500' });
      }
    } else if (language === 'json' || language === 'config') {
      // Simple JSON tokenization
      const parts = line.split(/("(?:[^"\\]|\\.)*"|\d+|true|false|null|[{}[\],:])/);
      for (const part of parts) {
        if (!part) continue;
        if (part.startsWith('"') && part.endsWith('"')) {
          // Check if it's a key (followed by :) or a value
          const isKey = line.includes(part + ':') || line.includes(part + ' :');
          tokens.push({ text: part, className: isKey ? 'text-blue-400' : 'text-green-400' });
        } else if (/^\d+$/.test(part)) {
          tokens.push({ text: part, className: 'text-orange-400' });
        } else if (part === 'true' || part === 'false' || part === 'null') {
          tokens.push({ text: part, className: 'text-purple-400' });
        } else {
          tokens.push({ text: part, className: 'text-gray-300' });
        }
      }
    } else {
      tokens.push({ text: line, className: 'text-gray-300' });
    }

    return tokens.length > 0 ? tokens : [{ text: ' ', className: '' }];
  };

  // Render a single line with tokens
  const renderLine = (line: string, language: string, lineNumber: number, isMatch: boolean) => {
    const tokens = tokenizeLine(line, language);

    return (
      <tr key={lineNumber} className={`hover:bg-gray-800 ${isMatch ? 'bg-yellow-900/30' : ''}`}>
        {showLineNumbers && (
          <td className="px-3 py-0.5 text-right text-gray-500 select-none border-r border-gray-700 w-12">
            {lineNumber}
          </td>
        )}
        <td className="px-4 py-0.5 whitespace-pre font-mono text-sm">
          {tokens.map((token, idx) => (
            <span key={idx} className={token.className}>
              {searchQuery && token.text.toLowerCase().includes(searchQuery.toLowerCase())
                ? highlightSearchMatch(token.text, searchQuery, token.className)
                : token.text}
            </span>
          ))}
        </td>
      </tr>
    );
  };

  // Highlight search matches within a token
  const highlightSearchMatch = (text: string, query: string, baseClass: string) => {
    const parts: React.ReactNode[] = [];
    const lowerText = text.toLowerCase();
    const lowerQuery = query.toLowerCase();
    let lastIndex = 0;

    let index = lowerText.indexOf(lowerQuery);
    while (index !== -1) {
      if (index > lastIndex) {
        parts.push(
          <span key={`before-${index}`} className={baseClass}>
            {text.slice(lastIndex, index)}
          </span>
        );
      }
      parts.push(
        <mark key={`match-${index}`} className="bg-yellow-300 text-gray-900 rounded px-0.5">
          {text.slice(index, index + query.length)}
        </mark>
      );
      lastIndex = index + query.length;
      index = lowerText.indexOf(lowerQuery, lastIndex);
    }

    if (lastIndex < text.length) {
      parts.push(
        <span key={`after-${lastIndex}`} className={baseClass}>
          {text.slice(lastIndex)}
        </span>
      );
    }

    return <>{parts}</>;
  };

  const renderCodeBlock = (code: string, language: string) => {
    const lines = code.split('\n');
    const lowerQuery = searchQuery.toLowerCase();

    return (
      <div className={`font-mono text-sm ${isFullscreen ? 'h-[calc(100vh-200px)]' : 'max-h-96'} overflow-auto`}>
        <table className="w-full">
          <tbody>
            {lines.map((line, index) => {
              const isMatch = searchQuery && line.toLowerCase().includes(lowerQuery);
              return renderLine(line, language, index + 1, isMatch);
            })}
          </tbody>
        </table>
      </div>
    );
  };

  const matchCount = useMemo(() => {
    if (!searchQuery) return 0;
    const code = getCurrentCode();
    return (code.toLowerCase().match(new RegExp(searchQuery.toLowerCase(), 'g')) || []).length;
  }, [searchQuery, activeTab, sourceCode]);

  if (!sourceCode) {
    return (
      <div className="bg-gray-50 rounded-lg p-8 text-center">
        <FileCode className="w-12 h-12 text-gray-400 mx-auto mb-3" />
        <h4 className="text-gray-700 mb-2">No Source Code Available</h4>
        <p className="text-sm text-gray-500">
          Source code for this job has not been loaded or is not available.
        </p>
      </div>
    );
  }

  return (
    <div className={`${isFullscreen ? 'fixed inset-0 z-50 bg-white p-4' : ''}`}>
      <div className={`space-y-4 ${isFullscreen ? 'h-full flex flex-col' : ''}`}>
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Code2 className="w-5 h-5 text-gray-600" />
            <h3 className="text-gray-900">Source Code</h3>
            {sourceCode.version && (
              <span className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs">
                v{sourceCode.version}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setIsFullscreen(!isFullscreen)}
              className="p-2 hover:bg-gray-100 rounded transition-colors"
              title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
            >
              {isFullscreen ? (
                <Minimize2 className="w-4 h-4 text-gray-600" />
              ) : (
                <Maximize2 className="w-4 h-4 text-gray-600" />
              )}
            </button>
          </div>
        </div>

        {/* Last Updated */}
        {sourceCode.lastUpdated && (
          <div className="text-sm text-gray-600">
            Last updated: {new Date(sourceCode.lastUpdated).toLocaleString()}
          </div>
        )}

        {/* Tabs */}
        <div className="flex items-center gap-2 border-b border-gray-200">
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => tab.available && setActiveTab(tab.id)}
              disabled={!tab.available}
              className={`flex items-center gap-2 px-4 py-2.5 border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-blue-600 text-blue-600'
                  : tab.available
                  ? 'border-transparent text-gray-600 hover:text-gray-900 hover:border-gray-300'
                  : 'border-transparent text-gray-400 cursor-not-allowed'
              }`}
            >
              {tab.icon}
              <span className="text-sm">{tab.label}</span>
              {!tab.available && (
                <span className="text-xs bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded">N/A</span>
              )}
            </button>
          ))}
        </div>

        {/* Toolbar */}
        <div className="flex items-center justify-between bg-gray-100 rounded-lg p-2">
          <div className="flex items-center gap-2">
            {/* Search */}
            <div className="relative">
              <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                placeholder="Search in code..."
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                className="pl-9 pr-3 py-1.5 bg-white border border-gray-200 rounded text-sm w-64 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>

            {/* Line Numbers Toggle */}
            <button
              onClick={() => setShowLineNumbers(!showLineNumbers)}
              className={`px-3 py-1.5 rounded text-sm transition-colors ${
                showLineNumbers
                  ? 'bg-blue-100 text-blue-700'
                  : 'bg-white text-gray-600 hover:bg-gray-50'
              }`}
            >
              Line #
            </button>
          </div>

          <div className="flex items-center gap-2">
            {/* Copy Button */}
            <button
              onClick={handleCopy}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-gray-200 rounded text-sm text-gray-700 hover:bg-gray-50 transition-colors"
            >
              {copied ? (
                <>
                  <Check className="w-4 h-4 text-green-600" />
                  <span className="text-green-600">Copied!</span>
                </>
              ) : (
                <>
                  <Copy className="w-4 h-4" />
                  Copy
                </>
              )}
            </button>

            {/* Download Button */}
            <button
              onClick={handleDownload}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-gray-200 rounded text-sm text-gray-700 hover:bg-gray-50 transition-colors"
            >
              <Download className="w-4 h-4" />
              Download
            </button>
          </div>
        </div>

        {/* Code Display */}
        <div className={`bg-gray-900 rounded-lg overflow-hidden ${isFullscreen ? 'flex-1' : ''}`}>
          {getCurrentCode() ? (
            renderCodeBlock(getCurrentCode(), activeTab === 'config' ? 'json' : activeTab)
          ) : (
            <div className="p-8 text-center text-gray-500">
              <FileText className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p>No {activeTab.toUpperCase()} code available for this job</p>
            </div>
          )}
        </div>

        {/* Code Stats */}
        {getCurrentCode() && (
          <div className="flex items-center gap-6 text-sm text-gray-600">
            <span>Lines: {getCurrentCode().split('\n').length}</span>
            <span>Characters: {getCurrentCode().length.toLocaleString()}</span>
            <span>Size: {(new Blob([getCurrentCode()]).size / 1024).toFixed(1)} KB</span>
            {searchQuery && (
              <span className="text-blue-600">
                Matches: {matchCount}
              </span>
            )}
          </div>
        )}

        {/* SQL Sections (if SQL tab is active and has sections) */}
        {activeTab === 'sql' && sourceCode.sqlSections && sourceCode.sqlSections.length > 0 && (
          <div className="border-t border-gray-200 pt-4 mt-4">
            <h4 className="text-sm text-gray-700 mb-3 flex items-center gap-2">
              <Database className="w-4 h-4" />
              SQL Query Sections
            </h4>
            <div className="space-y-2">
              {sourceCode.sqlSections.map((section, index) => (
                <div key={index} className="border border-gray-200 rounded-lg overflow-hidden">
                  <button
                    onClick={() => toggleSection(`sql-${index}`)}
                    className="w-full px-4 py-2 bg-gray-50 flex items-center justify-between hover:bg-gray-100 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      {expandedSections.has(`sql-${index}`) ? (
                        <ChevronDown className="w-4 h-4 text-gray-400" />
                      ) : (
                        <ChevronRight className="w-4 h-4 text-gray-400" />
                      )}
                      <span className="text-sm text-gray-900">{section.name}</span>
                      <span className="text-xs text-gray-500">({section.type})</span>
                    </div>
                    <span className="text-xs text-gray-500">
                      {section.code.split('\n').length} lines
                    </span>
                  </button>
                  {expandedSections.has(`sql-${index}`) && (
                    <div className="bg-gray-900 p-4 overflow-x-auto">
                      <pre className="text-sm text-gray-300 font-mono whitespace-pre">
                        {section.code}
                      </pre>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
