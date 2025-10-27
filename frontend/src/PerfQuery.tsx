import React, { useState, useEffect, useCallback } from 'react';
import { useRouter } from 'next/router';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
import { autocompletion } from '@codemirror/autocomplete';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { xcodeLight, xcodeLightPatch } from './components/CodeMirrorTheme';
import dynamic from 'next/dynamic';

const ProfileGraphDashboard = dynamic(() => import('./ProfileGraphDashboard'), {
  ssr: false
});

interface QueryResult {
  columns: string[];
  types: string[];
  data: string[][];
  rowCount: number;
  duration: string;
}

interface QueryRequest {
  sql: string;
  kind?: number;
}

interface QueryResponse {
  results: QueryResult[];
  queryId?: string;
}

const PerfQuery: React.FC = () => {
  const router = useRouter();
  // Get query ID from path parameters (for catch-all routes like [slug])
  const pathQueryId = router.query.slug && Array.isArray(router.query.slug)
    ? router.query.slug.join('/')
    : router.query.slug;
  const queryId = pathQueryId;
  const [query, setQuery] = useState(`SELECT number % 7 as a, number % 11 as b, number % 13 as c, count(distinct number) FROM numbers(100000000) group by a, b, c;`);
  const [analysisType, setAnalysisType] = useState<'graph' | 'perf'>('graph');

  const [perfData, setPerfData] = useState<string>('');
  const [graphData, setGraphData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');

  // Process perf results: concatenate multi-row data and parse JSON
  const processPerfResults = useCallback((results: QueryResult[], kind: number): { perfData: string; graphData: any } => {
    if (!results || results.length === 0) {
      return { perfData: '', graphData: null };
    }

    // For EXPLAIN ANALYZE GRAPHICAL + SQL, we expect one result with multiple rows
    const result = results[0];
    if (!result.data || result.data.length === 0) {
      return { perfData: '', graphData: null };
    }

    // Concatenate all row values into a single string
    let concatenatedData = '';
    result.data.forEach(row => {
      if (row && row.length > 0) {
        // Each row should have one column with the data
        concatenatedData += row[0] + '\n';
      }
    });

    if (kind === 1) { // EXPLAIN ANALYZE GRAPHICAL - parse as JSON
      try {
        const jsonData = JSON.parse(concatenatedData.trim());
        return {
          perfData: JSON.stringify(jsonData, null, 2),
          graphData: jsonData
        };
      } catch (e) {
        console.error('JSON parsing error:', e);
        // If not valid JSON, return the raw concatenated string
        return {
          perfData: concatenatedData.trim(),
          graphData: null
        };
      }
    } else { // EXPLAIN PERF - return as HTML string
      return {
        perfData: concatenatedData.trim(),
        graphData: null
      };
    }
  }, []);

  const loadSharedPerfQuery = useCallback(async (queryId: string) => {
    try {
      setLoading(true);
      setError('');
      const response = await fetch(`/api/query/${queryId}`);
      if (response.ok) {
        const data = await response.json();
        setQuery(data.sql);
        // Update analysisType based on stored kind
        if (data.kind === 1) {
          setAnalysisType('graph');
        } else {
          setAnalysisType('perf');
        }
        // For perf queries, we need to reconstruct the perf data from results
        if (data.results && data.results.length > 0) {
          const { perfData, graphData } = processPerfResults(data.results, data.kind || 2);
          setPerfData(perfData);
          setGraphData(graphData);
        } else {
          setPerfData('');
          setGraphData(null);
        }
      } else {
        // Query not found, but still render the page
        setError(`Run ID "${queryId}" not found`);
        setPerfData('');
      }
    } catch (error) {
      console.error('Failed to load shared perf query:', error);
      setError(`Failed to load run ID "${queryId}"`);
      setPerfData('');
    } finally {
      setLoading(false);
    }
  }, [processPerfResults]);

  const executePerfQuery = useCallback(async () => {
    if (!query.trim()) {
      alert('Please enter a SQL query');
      return;
    }

    setLoading(true);
    setError(''); // Clear any previous errors
    try {
      const kind = analysisType === 'graph' ? 1 : 2; // 1 for EXPLAIN ANALYZE GRAPHICAL, 2 for EXPLAIN PERF
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          sql: query,
          kind
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data: QueryResponse = await response.json();

      // Process the results to concatenate multi-row data and parse JSON
      const { perfData, graphData } = processPerfResults(data.results, kind);
      setPerfData(perfData);
      setGraphData(graphData);

      // Update URL with the query ID if returned
      if (data.queryId) {
        router.push(`/perf/${data.queryId}`, undefined, { shallow: true });
      }
    } catch (error) {
      console.error('Perf query execution failed:', error);
      alert('Perf query execution failed: ' + (error as Error).message);
    } finally {
      setLoading(false);
    }
  }, [query, router, processPerfResults, analysisType]);

  // Load query from URL on component mount
  useEffect(() => {
    if (router.isReady && queryId && typeof queryId === 'string') {
      loadSharedPerfQuery(queryId);
    } else {
      setQuery(`SELECT * FROM system.tables LIMIT 10;`);
    }
  }, [router.isReady, queryId, loadSharedPerfQuery]);

  // Add global keyboard event listener for Cmd+Enter
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        executePerfQuery();
      }
    };

    window.addEventListener('keydown', handleKeyDown);

    // Cleanup event listener on component unmount
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [executePerfQuery]);

  // Add resizeIframe function for HTML content
  useEffect(() => {
    // Define the resizeIframe function that the HTML content expects
    (window as any).resizeIframe = function(iframe: HTMLIFrameElement) {
      try {
        if (iframe && iframe.contentWindow && iframe.contentWindow.document) {
          const height = iframe.contentWindow.document.body.scrollHeight;
          iframe.style.height = height + 'px';
        }
      } catch (error) {
        console.warn('Could not resize iframe:', error);
      }
    };

    // Cleanup function
    return () => {
      delete (window as any).resizeIframe;
    };
  }, []);

  const renderPerfData = () => {
    if (!perfData) {
      return (
        <div className="flex items-center justify-center h-full text-gray-500">
          Execute a query to see performance analysis
        </div>
      );
    }

    if (analysisType === 'graph' && graphData) {
      return (
        <div className="h-full overflow-auto">
          <ProfileGraphDashboard perfData={graphData} />
        </div>
      );
    } else {
      // For perf mode, render the HTML content
      return (
        <div
          className="h-full overflow-auto p-4 bg-white"
          dangerouslySetInnerHTML={{ __html: perfData }}
        />
      );
    }
  };

  return (
    <div className="h-full bg-gray-100">
      {/* Header */}
      <div className="bg-yellow-400 px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <span className="font-bold">Performance Analysis</span>
          <div className="flex items-center gap-2">
            <select
              value={analysisType}
              onChange={(e) => setAnalysisType(e.target.value as 'perf' | 'graph')}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value="perf">Perf</option>
              <option value="graph">Graph</option>
            </select>
            <button
              onClick={executePerfQuery}
              disabled={loading}
              className="bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 disabled:cursor-not-allowed text-white px-4 py-1.5 rounded-md flex items-center gap-2 text-sm"
            >
              {loading ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                  ANALYZING...
                </>
              ) : (
                <>
                  ▶ ANALYZE PERFORMANCE
                </>
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Content - Resizable Vertical Panels */}
      <div className="h-[calc(100vh-48px)] border border-gray-300">
        <PanelGroup direction="vertical" className="h-full">
          {/* Top Panel - SQL Editor (small initial height) */}
          <Panel defaultSize={15} minSize={5} maxSize={40} className="h-full">
            <div className="h-full border-b border-gray-300 relative">
              <CodeMirror
                value={query}
                placeholder="Enter your SQL query here... (Press Cmd+Enter to analyze performance)"
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

          <PanelResizeHandle className="h-1 bg-gray-300 hover:bg-gray-400 cursor-row-resize" />

          {/* Bottom Panel - Performance Results */}
          <Panel defaultSize={80} minSize={60} className="h-full">
            <div className="h-full bg-white relative">
              <div className="p-2 h-full">
                {error ? (
                  <div className="flex items-center justify-center h-full">
                    <div className="bg-red-50 border border-red-300 rounded-lg p-4 text-red-700">
                      <div className="font-medium text-red-800 mb-1">Error</div>
                      <div className="text-sm">{error}</div>
                    </div>
                  </div>
                ) : (
                  renderPerfData()
                )}
              </div>
            </div>
          </Panel>
        </PanelGroup>
      </div>
    </div>
  );
};

export default PerfQuery;