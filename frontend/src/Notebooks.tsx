import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
} from 'react';
import { PanelGroup, Panel, PanelResizeHandle } from 'react-resizable-panels';
import NotebookCellComponent from './components/NotebookCell';
import { Notebook, NotebookCell } from './types/notebook';
import { notebookStorage } from './utills/notebookStorage';
import { formatRelativeTime } from './utills/time';

const Notebooks: React.FC = () => {
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [currentNotebook, setCurrentNotebook] = useState<Notebook | null>(null);
  const [activeCellId, setActiveCellId] = useState<string | null>(null);
  const [notebookSearch, setNotebookSearch] = useState('');
  const [resultSearch, setResultSearch] = useState('');
  const [draggingCellId, setDraggingCellId] = useState<string | null>(null);
  const [dragOverCellId, setDragOverCellId] = useState<string | null>(null);
  const [dragOverTail, setDragOverTail] = useState(false);

  // Load stored notebooks on mount
  useEffect(() => {
    const storage = notebookStorage.getNotebooks();
    setNotebooks(storage.notebooks);

    let initial = storage.notebooks[0];
    if (storage.currentNotebookId) {
      const found = storage.notebooks.find(nb => nb.id === storage.currentNotebookId);
      if (found) {
        initial = found;
      }
    }

    if (initial) {
      setCurrentNotebook(initial);
      setActiveCellId(initial.cells[0]?.id ?? null);
    }
  }, []);

  // Persist notebooks whenever they change
  useEffect(() => {
    if (notebooks.length > 0) {
      notebookStorage.saveNotebooks({
        notebooks,
        currentNotebookId: currentNotebook?.id,
      });
    }
  }, [notebooks, currentNotebook]);

  // Ensure the active cell always exists inside the current notebook
  useEffect(() => {
    if (!currentNotebook) {
      setActiveCellId(null);
      return;
    }

    const hasActive = currentNotebook.cells.some(cell => cell.id === activeCellId);
    if (!hasActive) {
      setActiveCellId(currentNotebook.cells[0]?.id ?? null);
    }
  }, [currentNotebook, activeCellId]);

  // Reset filters when switching cells
  useEffect(() => {
    setResultSearch('');
  }, [activeCellId]);

  const filteredNotebooks = useMemo(() => {
    if (!notebookSearch.trim()) {
      return notebooks;
    }

    const term = notebookSearch.toLowerCase();
    return notebooks.filter(nb => nb.name.toLowerCase().includes(term));
  }, [notebooks, notebookSearch]);

  const activeCell = currentNotebook?.cells.find(cell => cell.id === activeCellId) || null;
  const activeResult = activeCell?.result;

  const filteredResultRows = useMemo(() => {
    if (!activeResult) {
      return [];
    }

    if (!resultSearch.trim()) {
      return activeResult.data;
    }

    const term = resultSearch.toLowerCase();
    return activeResult.data.filter(row =>
      row.some(value => String(value ?? '').toLowerCase().includes(term))
    );
  }, [activeResult, resultSearch]);

  const createNewNotebook = useCallback(() => {
    const newNotebook = notebookStorage.createNotebook();
    setNotebooks(prev => [...prev, newNotebook]);
    setCurrentNotebook(newNotebook);
    setActiveCellId(newNotebook.cells[0]?.id ?? null);
  }, []);

  const selectNotebook = useCallback((notebookId: string) => {
    const notebook = notebooks.find(nb => nb.id === notebookId);
    if (!notebook) {
      return;
    }
    setCurrentNotebook(notebook);
    setActiveCellId(notebook.cells[0]?.id ?? null);
  }, [notebooks]);

  const updateNotebookName = useCallback((notebookId: string, name: string) => {
    setNotebooks(prev =>
      prev.map(nb =>
        nb.id === notebookId
          ? { ...nb, name, updatedAt: new Date() }
          : nb
      )
    );
    if (currentNotebook?.id === notebookId) {
      setCurrentNotebook(prev => prev ? { ...prev, name, updatedAt: new Date() } : null);
    }
  }, [currentNotebook]);

  const addCell = useCallback(() => {
    if (!currentNotebook) {
      return;
    }

    const newCell = notebookStorage.createCell();
    const updatedNotebook = {
      ...currentNotebook,
      cells: [...currentNotebook.cells, newCell],
      updatedAt: new Date(),
    };

    setActiveCellId(newCell.id);
    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
  }, [currentNotebook]);

  const updateCellSql = useCallback((cellId: string, sql: string) => {
    if (!currentNotebook) {
      return;
    }

    const updatedNotebook = {
      ...currentNotebook,
      cells: currentNotebook.cells.map(cell =>
        cell.id === cellId ? { ...cell, sql } : cell
      ),
      updatedAt: new Date(),
    };

    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
  }, [currentNotebook]);

  const toggleCellSection = useCallback((cellId: string, field: keyof Pick<NotebookCell, 'collapsed' | 'hideEditor' | 'hideResult'>) => {
    if (!currentNotebook) {
      return;
    }

    const updatedNotebook = {
      ...currentNotebook,
      cells: currentNotebook.cells.map(cell =>
        cell.id === cellId
          ? { ...cell, [field]: !cell[field] }
          : cell
      ),
      updatedAt: new Date(),
    };

    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
  }, [currentNotebook]);

  const reorderCells = useCallback((sourceId: string, targetId: string, placeAfter = false) => {
    if (!currentNotebook || sourceId === targetId) {
      return;
    }

    const cells = [...currentNotebook.cells];
    const sourceIndex = cells.findIndex(cell => cell.id === sourceId);
    if (sourceIndex === -1) {
      return;
    }

    const [moved] = cells.splice(sourceIndex, 1);
    const targetIndex = cells.findIndex(cell => cell.id === targetId);
    if (targetIndex === -1) {
      cells.splice(sourceIndex, 0, moved);
      return;
    }

    const insertIndex = placeAfter ? targetIndex + 1 : targetIndex;
    cells.splice(insertIndex, 0, moved);

    const updatedNotebook = {
      ...currentNotebook,
      cells,
      updatedAt: new Date(),
    };

    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
    setActiveCellId(moved.id);
  }, [currentNotebook]);

  const moveCellPosition = useCallback((cellId: string, direction: 'up' | 'down') => {
    if (!currentNotebook) {
      return;
    }
    const cells = currentNotebook.cells;
    const currentIndex = cells.findIndex(cell => cell.id === cellId);
    if (currentIndex === -1) {
      return;
    }
    const targetIndex = direction === 'up' ? currentIndex - 1 : currentIndex + 1;
    if (targetIndex < 0 || targetIndex >= cells.length) {
      return;
    }
    const targetId = cells[targetIndex].id;
    reorderCells(cellId, targetId, direction === 'down');
  }, [currentNotebook, reorderCells]);

  const handleDragStart = useCallback((cellId: string) => {
    setDraggingCellId(cellId);
    setDragOverCellId(null);
    setDragOverTail(false);
  }, []);

  const handleDragEnd = useCallback(() => {
    setDraggingCellId(null);
    setDragOverCellId(null);
    setDragOverTail(false);
  }, []);

  const handleDragOverCell = useCallback((cellId: string) => {
    if (!draggingCellId || draggingCellId === cellId) {
      return;
    }
    setDragOverCellId(cellId);
    setDragOverTail(false);
  }, [draggingCellId]);

  const handleDropOnCell = useCallback((cellId: string) => {
    if (draggingCellId && draggingCellId !== cellId) {
      reorderCells(draggingCellId, cellId);
    }
    setDraggingCellId(null);
    setDragOverCellId(null);
    setDragOverTail(false);
  }, [draggingCellId, reorderCells]);

  const handleDropAtEnd = useCallback(() => {
    if (!currentNotebook || !draggingCellId) {
      return;
    }
    const lastCell = currentNotebook.cells[currentNotebook.cells.length - 1];
    if (!lastCell || lastCell.id === draggingCellId) {
      handleDragEnd();
      return;
    }
    reorderCells(draggingCellId, lastCell.id, true);
    setDraggingCellId(null);
    setDragOverCellId(null);
    setDragOverTail(false);
  }, [currentNotebook, draggingCellId, reorderCells, handleDragEnd]);

  const executeCell = useCallback(async (cellId: string) => {
    if (!currentNotebook) {
      return;
    }

    const cell = currentNotebook.cells.find(c => c.id === cellId);
    if (!cell || !cell.sql.trim()) {
      return;
    }

    setActiveCellId(cellId);

    const updatedNotebook = {
      ...currentNotebook,
      cells: currentNotebook.cells.map(c =>
        c.id === cellId ? { ...c, loading: true, error: undefined } : c
      ),
    };
    setCurrentNotebook(updatedNotebook);

    try {
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql: cell.sql, kind: 0 }),
      });

      if (!response.ok) {
        let errorMessage = `HTTP error! status: ${response.status}`;
        try {
          const errorData = await response.json();
          if (errorData.error) {
            errorMessage = errorData.error;
          }
        } catch (e) {
          // response was not JSON – ignore
        }
        throw new Error(errorMessage);
      }

      const data = await response.json();
      const results = data.results || [];
      const lastResult = results.length > 0 ? results[results.length - 1] : undefined;

      const finalNotebook = {
        ...currentNotebook,
        cells: currentNotebook.cells.map(c =>
          c.id === cellId
            ? {
                ...c,
                loading: false,
                result: lastResult,
                error: undefined,
                lastExecutedAt: new Date(),
              }
            : c
        ),
        updatedAt: new Date(),
      };

      setCurrentNotebook(finalNotebook);
      setNotebooks(prev => prev.map(nb => nb.id === finalNotebook.id ? finalNotebook : nb));
    } catch (error) {
      console.error('Cell execution failed:', error);
      const errorMessage = (error as Error).message.replace(/\\n/g, '\n');

      const finalNotebook = {
        ...currentNotebook,
        cells: currentNotebook.cells.map(c =>
          c.id === cellId
            ? {
                ...c,
                loading: false,
                error: 'Query execution failed: ' + errorMessage,
                result: undefined,
              }
            : c
        ),
        updatedAt: new Date(),
      };

      setCurrentNotebook(finalNotebook);
      setNotebooks(prev => prev.map(nb => nb.id === finalNotebook.id ? finalNotebook : nb));
    }
  }, [currentNotebook]);

  const deleteCell = useCallback((cellId: string) => {
    if (!currentNotebook || currentNotebook.cells.length <= 1) {
      return;
    }

    const cellIndex = currentNotebook.cells.findIndex(cell => cell.id === cellId);
    const fallbackCell = currentNotebook.cells[cellIndex + 1] || currentNotebook.cells[cellIndex - 1];

    const updatedNotebook = {
      ...currentNotebook,
      cells: currentNotebook.cells.filter(cell => cell.id !== cellId),
      updatedAt: new Date(),
    };

    setActiveCellId(prev => (prev === cellId ? fallbackCell?.id ?? null : prev));
    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
  }, [currentNotebook]);

  const rowCount = activeResult?.rowCount ?? activeResult?.data.length ?? 0;
  const columnCount = activeResult?.columns.length ?? 0;

  const renderSidebar = () => (
    <div className="flex h-full flex-col border-r border-gray-200 bg-white">
      <div className="p-4 border-b border-gray-100">
        <label className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2 block">
          Search
        </label>
        <div className="relative">
          <span className="absolute inset-y-0 left-3 flex items-center text-gray-400">
            <svg className="h-4 w-4" viewBox="0 0 20 20" fill="none" stroke="currentColor">
              <path
                d="m13.5 13.5 3 3"
                strokeWidth="1.6"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <circle cx="9" cy="9" r="5.5" strokeWidth="1.6" />
            </svg>
          </span>
          <input
            value={notebookSearch}
            onChange={(e) => setNotebookSearch(e.target.value)}
            placeholder="Find notebook"
            className="w-full rounded-xl border border-gray-200 bg-gray-50 pl-9 pr-3 py-2 text-sm text-gray-700 focus:border-indigo-400 focus:bg-white focus:outline-none"
          />
        </div>
        <button
          onClick={createNewNotebook}
          className="mt-4 w-full rounded-xl border border-dashed border-gray-300 py-2 text-sm font-semibold text-gray-700 hover:border-indigo-300 hover:text-indigo-600"
        >
          + New Notebook
        </button>
      </div>

      <div className="flex-1 overflow-y-auto">
        <div className="px-4 py-3 text-xs font-semibold uppercase tracking-wide text-gray-500">
          Notebooks
        </div>

        <div className="px-2 pb-4 space-y-1">
          {filteredNotebooks.length === 0 && (
            <div className="text-center text-sm text-gray-500 px-2 py-6">
              No notebooks found
            </div>
          )}
          {filteredNotebooks.map(nb => {
            const isActive = currentNotebook?.id === nb.id;
            return (
              <button
                key={nb.id}
                onClick={() => selectNotebook(nb.id)}
                className={`w-full text-left px-3 py-3 rounded-xl border transition-all ${
                  isActive
                    ? 'border-indigo-300 bg-indigo-50/70 text-indigo-700'
                    : 'border-transparent hover:border-gray-200 hover:bg-gray-50'
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="font-semibold text-sm truncate">{nb.name || 'Untitled Notebook'}</span>
                  <span className="text-[10px] uppercase text-gray-400">
                    {formatRelativeTime(nb.updatedAt) || 'new'}
                  </span>
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  {nb.cells.length} cell{nb.cells.length === 1 ? '' : 's'}
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );

  const renderResultPanel = () => (
    <div className="flex h-full flex-col border-l border-gray-200 bg-white">
      <div className="px-5 py-4 border-b border-gray-100">
        <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">
          Result Inspector
        </p>
        <div className="mt-3 flex items-baseline gap-2">
          <span className="text-2xl font-semibold text-gray-900">{rowCount}</span>
          <span className="text-sm text-gray-500">Row{rowCount === 1 ? '' : 's'}</span>
        </div>
        <div className="text-sm text-gray-500 mt-1">
          {columnCount} Column{columnCount === 1 ? '' : 's'}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-5 space-y-4">
        {!currentNotebook && (
          <p className="text-sm text-gray-500">
            Create a notebook to start running queries.
          </p>
        )}

        {currentNotebook && !activeCell && (
          <p className="text-sm text-gray-500">
            Add a cell to see results.
          </p>
        )}

        {currentNotebook && activeCell && (
          <>
            {activeCell.loading && (
              <div className="flex items-center gap-2 text-sm text-gray-500">
                <span className="w-3 h-3 border-2 border-gray-300 border-t-indigo-500 rounded-full animate-spin" />
                Running query...
              </div>
            )}

            {activeCell.error && (
              <div className="rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                {activeCell.error}
              </div>
            )}

            {!activeCell.loading && !activeCell.error && activeResult && (
              <>
                <div>
                  <label className="text-xs font-semibold uppercase tracking-wide text-gray-400">
                    Filter rows
                  </label>
                  <div className="relative mt-2">
                    <span className="absolute inset-y-0 left-3 flex items-center text-gray-400">
                      <svg className="h-4 w-4" viewBox="0 0 20 20" fill="none" stroke="currentColor">
                        <path
                          d="m13.5 13.5 3 3"
                          strokeWidth="1.6"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                        <circle cx="9" cy="9" r="5.5" strokeWidth="1.6" />
                      </svg>
                    </span>
                    <input
                      value={resultSearch}
                      onChange={(e) => setResultSearch(e.target.value)}
                      className="w-full rounded-xl border border-gray-200 bg-gray-50 pl-9 pr-3 py-2 text-sm text-gray-700 focus:border-indigo-400 focus:bg-white focus:outline-none"
                      placeholder="Search results"
                    />
                  </div>
                </div>

                <div className="rounded-2xl border border-gray-200 overflow-hidden">
                  <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100 text-xs text-gray-500">
                    <span>{activeResult.duration || 'Result'}</span>
                    <span>{filteredResultRows.length} shown</span>
                  </div>
                  <div className="overflow-auto max-h-80">
                    <table className="min-w-full text-sm">
                      <thead className="bg-gray-50">
                        <tr>
                          {activeResult.columns.map((column, index) => (
                            <th key={column + index} className="px-3 py-2 text-left font-semibold text-gray-700">
                              <div>{column}</div>
                              {activeResult.types[index] && (
                                <div className="text-xs text-gray-400">{activeResult.types[index]}</div>
                              )}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {filteredResultRows.map((row, rowIndex) => (
                          <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                            {row.map((value, cellIndex) => (
                              <td key={cellIndex} className="px-3 py-2 text-gray-800 font-mono text-xs">
                                {value}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="rounded-2xl border border-dashed border-gray-200 p-3">
                  <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">
                    Column types
                  </p>
                  <div className="space-y-1">
                    {activeResult.columns.map((column, index) => (
                      <div key={column + index} className="flex items-center justify-between text-sm">
                        <span className="text-gray-700">{column}</span>
                        <span className="text-gray-500 font-mono text-xs">{activeResult.types[index]}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            )}

            {!activeCell.loading && !activeCell.error && !activeResult && (
              <p className="text-sm text-gray-500">
                Run the selected cell to preview its data.
              </p>
            )}
          </>
        )}
      </div>
    </div>
  );

  return (
    <div className="flex h-full flex-1 flex-col bg-[#f9fbff]">
      <PanelGroup direction="horizontal" className="flex-1 overflow-hidden">
        <Panel defaultSize={18} minSize={15} maxSize={30} className="h-full">
          {renderSidebar()}
        </Panel>
        <PanelResizeHandle className="group flex w-3 cursor-col-resize items-center justify-center">
          <span className="h-10 w-0.5 rounded-full bg-gray-200 group-hover:bg-indigo-400" />
        </PanelResizeHandle>
        <Panel defaultSize={60} minSize={40} className="h-full">
          <div className="flex h-full flex-col overflow-hidden">
            {currentNotebook ? (
              <>
                <header className="border-b border-gray-200 bg-white px-8 py-6 flex items-center justify-between">
                  <div className="min-w-0">
                    <input
                      type="text"
                      value={currentNotebook.name}
                      onChange={(e) => updateNotebookName(currentNotebook.id, e.target.value)}
                      className="w-full border-none bg-transparent text-2xl font-semibold text-gray-900 focus:outline-none"
                      placeholder="Notebook name"
                    />
                    <p className="text-sm text-gray-500 mt-1">
                      Updated {formatRelativeTime(currentNotebook.updatedAt) || 'just now'} · {currentNotebook.cells.length} cell{currentNotebook.cells.length === 1 ? '' : 's'}
                    </p>
                  </div>
                </header>

                <div className="flex-1 overflow-y-auto px-8 py-6 space-y-5">
                  {currentNotebook.cells.map((cell, index) => (
                    <NotebookCellComponent
                      key={cell.id}
                      cell={cell}
                      index={index}
                      onSqlChange={(sql) => updateCellSql(cell.id, sql)}
                      onExecute={() => executeCell(cell.id)}
                      onDelete={() => deleteCell(cell.id)}
                      canDelete={currentNotebook.cells.length > 1}
                      isActive={cell.id === activeCellId}
                      onSelect={() => setActiveCellId(cell.id)}
                      onToggleCollapse={() => toggleCellSection(cell.id, 'collapsed')}
                      onToggleEditor={() => toggleCellSection(cell.id, 'hideEditor')}
                      onToggleResult={() => toggleCellSection(cell.id, 'hideResult')}
                      onMoveUp={() => moveCellPosition(cell.id, 'up')}
                      onMoveDown={() => moveCellPosition(cell.id, 'down')}
                      dragState={{
                        isDragging: draggingCellId === cell.id,
                        isDragOver: dragOverCellId === cell.id,
                      }}
                      onDragStart={() => handleDragStart(cell.id)}
                      onDragEnd={handleDragEnd}
                      onDragOver={() => handleDragOverCell(cell.id)}
                      onDrop={() => handleDropOnCell(cell.id)}
                    />
                  ))}

                  <button
                    onClick={addCell}
                    onDragOver={(event) => {
                      event.preventDefault();
                      if (draggingCellId) {
                        setDragOverTail(true);
                      }
                    }}
                    onDragLeave={() => setDragOverTail(false)}
                    onDrop={(event) => {
                      event.preventDefault();
                      handleDropAtEnd();
                    }}
                    className={`w-full rounded-2xl border-2 border-dashed py-4 text-sm font-semibold text-gray-500 transition ${
                      dragOverTail
                        ? 'border-indigo-300 text-indigo-600 bg-indigo-50/30'
                        : 'border-gray-300 hover:border-indigo-300 hover:text-indigo-600'
                    }`}
                  >
                    + Add Cell
                  </button>
                </div>
              </>
            ) : (
              <div className="flex-1 flex items-center justify-center">
                <div className="text-center max-w-xs">
                  <p className="text-lg font-semibold text-gray-800 mb-2">No notebooks yet</p>
                  <p className="text-sm text-gray-500 mb-4">
                    Create your first notebook to start experimenting with queries.
                  </p>
                  <button
                    onClick={createNewNotebook}
                    className="rounded-xl bg-indigo-600 px-5 py-2 text-sm font-medium text-white hover:bg-indigo-500"
                  >
                    Create Notebook
                  </button>
                </div>
              </div>
            )}
          </div>
        </Panel>
        <PanelResizeHandle className="group flex w-3 cursor-col-resize items-center justify-center">
          <span className="h-10 w-0.5 rounded-full bg-gray-200 group-hover:bg-indigo-400" />
        </PanelResizeHandle>
        <Panel defaultSize={22} minSize={20} maxSize={35} className="h-full">
          {renderResultPanel()}
        </Panel>
      </PanelGroup>
    </div>
  );
};

export default Notebooks;
