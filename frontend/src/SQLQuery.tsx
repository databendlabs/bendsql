import React, { useState, useEffect, useCallback } from 'react';
import { useRouter } from 'next/router';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
import { autocompletion } from '@codemirror/autocomplete';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { xcodeLight, xcodeLightPatch } from './components/CodeMirrorTheme';

interface QueryResult {
  columns: string[];
  types: string[];
  data: string[][];
  rowCount: number;
  duration: string;
}

const SQLQuery: React.FC = () => {
  const router = useRouter();
  // Get query ID from path parameters (for catch-all routes like [slug])
  const pathQueryId = router.query.slug && Array.isArray(router.query.slug)
    ? router.query.slug.join('/')
    : router.query.slug;
  // Also check for legacy queryId parameter for backward compatibility
  const legacyQueryId = router.query.queryId;
  const queryId = pathQueryId || legacyQueryId;
  const [query, setQuery] = useState(``);

  const [results, setResults] = useState<QueryResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');

  // Load query from URL on component mount
  useEffect(() => {
    if (router.isReady && queryId && typeof queryId === 'string') {
      loadSharedQuery(queryId);
    } else {
      setQuery(`CREATE OR REPLACE TABLE students (uid Int16, name String, age Int16);

INSERT INTO students VALUES (1231, 'John', 33);
INSERT INTO students VALUES (6666, 'Ksenia', 48);
INSERT INTO students VALUES (8888, 'Alice', 50);

SELECT * FROM students;`);
    }
  }, [router.isReady, queryId]);

  const loadSharedQuery = async (queryId: string) => {
    try {
      setLoading(true);
      setError('');
      const response = await fetch(`/api/query/${queryId}`);
      if (response.ok) {
        const data = await response.json();
        setQuery(data.sql);
        setResults(data.results || []);
      } else {
        // Query not found, but still render the page
        setError(`Run ID "${queryId}" not found`);
        setResults([]);
      }
    } catch (error) {
      console.error('Failed to load shared query:', error);
      setError(`Failed to load run ID "${queryId}"`);
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  const executeQuery = useCallback(async () => {
    if (!query.trim()) {
      alert('Please enter a SQL query');
      return;
    }

    setLoading(true);
    setError(''); // Clear any previous errors
    try {
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          sql: query,
          kind: 0
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      setResults(data.results || []);

      // Update URL with the query ID if returned
      if (data.queryId) {
        router.push(`/${data.queryId}`, undefined, { shallow: true });
      }
    } catch (error) {
      console.error('Query execution failed:', error);
      alert('Query execution failed: ' + (error as Error).message);
    } finally {
      setLoading(false);
    }
  }, [query, router]);

  // Add global keyboard event listener for Cmd+Enter
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        executeQuery();
      }
    };

    window.addEventListener('keydown', handleKeyDown);

    // Cleanup event listener on component unmount
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [executeQuery]);

  const renderTable = (result: QueryResult, index: number) => {
    // Handle empty data
    if (!result.data || !Array.isArray(result.data) || result.data.length === 0) {
      return (
        <div
          key={index}
          className="bg-white border border-gray-300 rounded-lg mb-4 overflow-hidden shadow-sm"
        >
          <div className="bg-gray-50 border-b border-gray-300 px-4 py-3 font-medium text-gray-700">
            Query {index + 1} ({result.rowCount} rows, {result.duration})
          </div>
          <div className="p-4 text-center text-gray-500">
            No data returned
          </div>
        </div>
      );
    }

    return (
      <div
        key={index}
        className="bg-white border border-gray-300 rounded-lg mb-4 overflow-hidden shadow-sm"
      >
        <div className="bg-gray-50 border-b border-gray-300 px-4 py-3 font-medium text-gray-700">
          Query {index + 1} ({result.rowCount} rows, {result.duration})
        </div>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr className="bg-gray-100">
                {result.columns.map((column, i) => (
                  <th
                    key={i}
                    className="border border-gray-300 px-4 py-3 text-left font-semibold text-gray-900 bg-gray-50"
                  >
                    {column}
                    {result.types && result.types[i] && (
                      <div className="text-xs font-normal text-left text-gray-500 mt-1">
                        {result.types[i]}
                      </div>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.data.map((row, rowIndex) => (
                <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  {row.map((cell, cellIndex) => (
                    <td
                      key={cellIndex}
                      className="border border-gray-300 px-4 py-2 text-gray-900 font-mono text-sm"
                    >
                      {cell || ''}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div className="h-full bg-gray-100">
      {/* Header */}
      <div className="bg-yellow-400 px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <span className="font-bold">SQL Query</span>
          <button
            onClick={executeQuery}
            disabled={loading}
            className="bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 disabled:cursor-not-allowed text-white px-4 py-1.5 rounded-md flex items-center gap-2 text-sm"
          >
            {loading ? (
              <>
                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                RUNNING...
              </>
            ) : (
              <>
                ▶ RUN QUERY
              </>
            )}
          </button>
        </div>
      </div>

      {/* Content - Resizable Panels */}
      <div className="h-[calc(100vh-48px)] border border-gray-300">
        <PanelGroup direction="horizontal" className="h-full">
          {/* Left Panel - SQL Editor */}
          <Panel defaultSize={50} minSize={20} className="h-full">
            <div className="h-full border-r border-gray-300 relative">
              <CodeMirror
                value={query}
                placeholder="Enter your SQL queries here... (Press Cmd+Enter to run)"
                theme={xcodeLight}
                height="100%"
                extensions={[
                  xcodeLightPatch,
                  EditorView.lineWrapping,
                  autocompletion({
                    icons: false,
                  }),
                  sql(),
                ]}
                basicSetup={{
                lineNumbers: true,
                foldGutter: false,
                indentOnInput: false,
                autocompletion: true,
                highlightActiveLine: false,
                }}
                onChange={(value) => setQuery(value)}
                editable={!loading}
                style={{
                  height: '100%',
                  fontSize: '14px',
                }}
              />
            </div>
          </Panel>

          <PanelResizeHandle className="w-1 bg-gray-300 hover:bg-gray-400 cursor-col-resize" />

          {/* Right Panel - Results */}
          <Panel defaultSize={50} minSize={20} className="h-full">
            <div className="h-full overflow-auto bg-white relative">
              <div className="p-2 h-full">
                {error ? (
                  <div className="flex items-center justify-center h-full">
                    <div className="bg-red-50 border border-red-300 rounded-lg p-4 text-red-700">
                      <div className="font-medium text-red-800 mb-1">Error</div>
                      <div className="text-sm">{error}</div>
                    </div>
                  </div>
                ) : results.length > 0 ? (
                  <div className="space-y-4">
                    {results.map((result, index) => renderTable(result, index))}
                  </div>
                ) : (
                  <div className="flex items-center justify-center h-full text-gray-500">
                    Execute a query to see results
                  </div>
                )}
              </div>
            </div>
          </Panel>
        </PanelGroup>
      </div>
    </div>
  );
};

export default SQLQuery;