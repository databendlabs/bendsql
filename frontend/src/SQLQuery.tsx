import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useRouter } from 'next/router';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorState } from '@codemirror/state';
import { EditorView, keymap, lineNumbers } from '@codemirror/view';
import { autocompletion } from '@codemirror/autocomplete';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { xcodeLight, xcodeLightPatch } from './components/CodeMirrorTheme';
import { python_syntax } from './components/CodemirrorPython';

interface QueryResult {
  columns: string[];
  types: string[];
  data: string[][];
  rowCount: number;
  duration: string;
}

const SQLQuery: React.FC = () => {
  const isExecutionShortcut = (event: KeyboardEvent) => {
    if (!(event.metaKey || event.ctrlKey)) {
      return false;
    }
    return event.key === 'Enter' || event.key === 'NumpadEnter' || event.key === 'Return';
  };

  const router = useRouter();
  // Get query ID from path parameters (for catch-all routes like [slug])
  const pathQueryId = router.query.slug && Array.isArray(router.query.slug)
    ? router.query.slug.join('/')
    : router.query.slug;
  // Also check for legacy queryId parameter for backward compatibility
  const legacyQueryId = router.query.queryId;
  const queryId = pathQueryId || legacyQueryId;
  const [query, setQuery] = useState(``);
  const [engine, setEngine] = useState<'sql' | 'python'>('sql');

  const [results, setResults] = useState<QueryResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');
  const [showPythonHelp, setShowPythonHelp] = useState(false);

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
        if (data.kind === 3) {
          setEngine('python');
        } else {
          setEngine('sql');
        }
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
      setError('Please enter a SQL query');
      setResults([]); // Clear any previous results
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
          kind: engine === 'python' ? 3 : 0
        }),
      });

      if (!response.ok) {
        // Try to extract error message from response body
        let errorMessage = `HTTP error! status: ${response.status}`;
        try {
          const errorData = await response.json();
          if (errorData.error) {
            errorMessage = errorData.error;
          }
        } catch (e) {
          // If response is not JSON, use default error message
        }
        throw new Error(errorMessage);
      }

      const data = await response.json();
      setResults(data.results || []);

      // Update URL with the query ID if returned
      if (data.queryId) {
        router.push(`/${data.queryId}`, undefined, { shallow: true });
      }
    } catch (error) {
      console.error('Query execution failed:', error);
      // Replace \n with actual line breaks for better display
      const errorMessage = (error as Error).message.replace(/\\n/g, '\n');
      setError('Query execution failed: ' + errorMessage);
      setResults([]); // Clear any previous results
    } finally {
      setLoading(false);
    }
  }, [query, router, engine]);

  // Add global keyboard event listener for Cmd+Enter
  const runKeymap = useMemo(() => keymap.of([
    {
      key: 'Mod-Enter',
      run: () => {
        executeQuery();
        return true;
      },
      preventDefault: true,
    },
    {
      key: 'Ctrl-Enter',
      run: () => {
        executeQuery();
        return true;
      },
      preventDefault: true,
    },
  ]), [executeQuery]);

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

  const editorPlaceholder = engine === 'python'
    ? 'Enter your Python script here... (Press Cmd+Enter to run)'
    : 'Enter your SQL queries here... (Press Cmd+Enter to run)';

  const editorExtensions = useMemo(() => {
    const base = [
      xcodeLightPatch,
      EditorView.lineWrapping,
      lineNumbers(),
      autocompletion({ icons: false }),
    ];

    if (engine === 'python') {
      return [
        ...base,
        EditorState.tabSize.of(4),
        python_syntax(),
        runKeymap,
      ];
    }

    return [...base, sql(), runKeymap];
  }, [engine, runKeymap]);

  return (
    <div className="flex flex-1 min-h-0 flex-col bg-[#f9fbff]">
      <div className="border-b border-gray-200 bg-white px-4 py-3 flex items-center gap-3 text-sm text-gray-600">
        <button
          onClick={executeQuery}
          disabled={loading}
          className={`inline-flex items-center gap-2 rounded-full border px-4 py-1.5 text-sm font-semibold ${
            loading
              ? 'cursor-not-allowed border-gray-200 text-gray-400'
              : 'border-indigo-200 text-indigo-600 hover:bg-indigo-50'
          }`}
        >
          {loading ? (
            <>
              <span className="w-3.5 h-3.5 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin" />
              Running...
            </>
          ) : (
            <>
              <span className="text-base">▶</span>
              Run query
            </>
          )}
        </button>
        <span className="hidden sm:block">Press ⌘⏎ to run</span>
        <div className="ml-auto flex items-center gap-2 text-xs text-gray-500">
          {engine === 'python' && (
            <button
              type="button"
              onClick={() => setShowPythonHelp(true)}
              className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-gray-200 text-gray-500 hover:border-indigo-300 hover:text-indigo-600"
              title="Python engine help"
            >
              ?
            </button>
          )}
          <span>Engine</span>
          <select
            value={engine}
            onChange={(e) => setEngine(e.target.value as 'sql' | 'python')}
            className="rounded-full border border-gray-200 bg-white px-2 py-1 text-sm text-gray-700 focus:border-indigo-400 focus:outline-none"
            disabled={loading}
          >
            <option value="sql">SQL</option>
            <option value="python">Python</option>
          </select>
        </div>
      </div>
      <div className="flex-1 min-h-0 border border-gray-200 bg-white">
        <PanelGroup
          direction="horizontal"
          className="h-full min-h-0"
          style={{ height: '100%' }}
        >
          {/* Left Panel - SQL Editor */}
          <Panel defaultSize={50} minSize={20} className="h-full min-h-0">
            <div className="h-full min-h-0 border-r border-gray-300 relative">
              <CodeMirror
                value={query}
                placeholder={editorPlaceholder}
                theme={xcodeLight}
                height="100%"
                extensions={editorExtensions}
                basicSetup={false}
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
          <Panel defaultSize={50} minSize={20} className="h-full min-h-0">
            <div className="h-full min-h-0 overflow-auto bg-white relative">
              <div className="p-2 h-full min-h-0">
                {error ? (
                  <div className="flex items-center justify-center h-full">
                    <div className="bg-red-50 border border-red-300 rounded-lg p-4 text-red-700 max-w-full">
                      <div className="font-medium text-red-800 mb-1">Error</div>
                      <pre className="text-sm whitespace-pre-wrap break-words font-mono">{error}</pre>
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
      {engine === 'python' && showPythonHelp && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-2xl">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-gray-900">Python Engine Help</h2>
              <button
                onClick={() => setShowPythonHelp(false)}
                className="rounded-full border border-gray-200 px-2 py-1 text-sm text-gray-500 hover:border-gray-300"
              >
                Close
              </button>
            </div>
            <div className="space-y-4 text-sm text-gray-700">
              <p>
                Python scripts run inside <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">ghcr.io/astral-sh/uv:debian</code> with
                <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">uv run --script</code>. The following helpers are injected for you:
              </p>
              <ul className="list-disc pl-5 space-y-1 text-gray-600">
                <li><code className="text-xs bg-gray-100 px-1 py-0.5 rounded">async_client</code>: an <code>AsyncDatabendClient</code> connected to your DSN.</li>
                <li><code className="text-xs bg-gray-100 px-1 py-0.5 rounded">client</code>: a <code>BlockingDatabendClient</code> using the same DSN.</li>
              </ul>
              <p>Sync Example:</p>
              <pre className="bg-gray-900 text-gray-100 text-xs rounded-xl p-3 overflow-auto">
{`conn = client.get_conn()
rows = conn.query_iter("SELECT * FROM numbers(10)")
for row in rows:
    print(row.values())
conn.close()`}
              </pre>
              <p>Async Example:</p>
              <pre className="bg-gray-900 text-gray-100 text-xs rounded-xl p-3 overflow-auto">
{`async def main():
  conn = await async_client.get_conn()
  rows = await conn.query_iter("SELECT * FROM numbers(10)")
  async for row in rows:
      print(row.values())
  await conn.close()

asyncio.run(main())`}
              </pre>
              <p className="text-xs text-gray-500">Note: Docker must be installed locally for Python execution.</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default SQLQuery;
