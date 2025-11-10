import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
} from 'react';
import { PanelGroup, Panel, PanelResizeHandle } from 'react-resizable-panels';
import NotebookCellComponent from './components/NotebookCell';
import { EllipsisIcon } from './components/icons';
import { Notebook, NotebookCell } from './types/notebook';
import { notebookStorage } from './utills/notebookStorage';
import { formatRelativeTime } from './utills/time';
import { useDsn } from './context/DsnContext';

type DeletedCellEntry = {
  notebookId: string;
  cell: NotebookCell;
  index: number;
};
const Notebooks: React.FC = () => {
  const { currentDsn } = useDsn();
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [currentNotebook, setCurrentNotebook] = useState<Notebook | null>(null);
  const [activeCellId, setActiveCellId] = useState<string | null>(null);
  const [notebookSearch, setNotebookSearch] = useState('');
  const [draggingCellId, setDraggingCellId] = useState<string | null>(null);
  const [dragOverState, setDragOverState] = useState<{ id: string | null; placeAfter: boolean }>({
    id: null,
    placeAfter: false,
  });
  const [fullscreenCellId, setFullscreenCellId] = useState<string | null>(null);
  const [notebookMenuOpen, setNotebookMenuOpen] = useState(false);
  const [openNotebookMenuId, setOpenNotebookMenuId] = useState<string | null>(null);
  const notebookNameRef = useRef<HTMLInputElement | null>(null);
  const notebookMenuRef = useRef<HTMLDivElement | null>(null);
  const [notebookListCollapsed, setNotebookListCollapsed] = useState(false);
  const currentNotebookRef = useRef<Notebook | null>(null);
  const [editingCellId, setEditingCellId] = useState<string | null>(null);
  const [deletedCellsHistory, setDeletedCellsHistory] = useState<DeletedCellEntry[]>([]);
  const lastCommandRef = useRef<{ key: string; timestamp: number } | null>(null);

  useEffect(() => {
    currentNotebookRef.current = currentNotebook;
  }, [currentNotebook]);

  useEffect(() => {
    if (!currentNotebook) {
      setEditingCellId(null);
      return;
    }
    if (editingCellId && !currentNotebook.cells.some(cell => cell.id === editingCellId)) {
      setEditingCellId(null);
    }
  }, [currentNotebook, editingCellId]);

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

  // Persist notebooks whenever they change (including empty state)
  useEffect(() => {
    notebookStorage.saveNotebooks({
      notebooks,
      currentNotebookId: currentNotebook?.id,
    });
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

  useEffect(() => {
    if (!notebookMenuOpen) {
      return;
    }
    const handleClick = (event: MouseEvent) => {
      if (notebookMenuRef.current && !notebookMenuRef.current.contains(event.target as Node)) {
        setNotebookMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [notebookMenuOpen]);

  useEffect(() => {
    if (!openNotebookMenuId) {
      return;
    }
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (!target.closest('[data-notebook-menu]')) {
        setOpenNotebookMenuId(null);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [openNotebookMenuId]);

  const filteredNotebooks = useMemo(() => {
    if (!notebookSearch.trim()) {
      return notebooks;
    }

    const term = notebookSearch.toLowerCase();
    return notebooks.filter(nb => nb.name.toLowerCase().includes(term));
  }, [notebooks, notebookSearch]);

  const createNewNotebook = useCallback(() => {
    const newNotebook = notebookStorage.createNotebook();
    setNotebooks(prev => [...prev, newNotebook]);
    setCurrentNotebook(newNotebook);
    setActiveCellId(newNotebook.cells[0]?.id ?? null);
    setEditingCellId(null);
  }, []);

  const selectNotebook = useCallback((notebookId: string) => {
    const notebook = notebooks.find(nb => nb.id === notebookId);
    if (!notebook) {
      return;
    }
    setCurrentNotebook(notebook);
    setActiveCellId(notebook.cells[0]?.id ?? null);
    setEditingCellId(null);
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

  const addCellAt = useCallback((insertIndex: number) => {
    if (!currentNotebook) {
      return;
    }

    const newCell = notebookStorage.createCell();
    const cells = [...currentNotebook.cells];
    const clampedIndex = Math.max(0, Math.min(insertIndex, cells.length));
    cells.splice(clampedIndex, 0, newCell);

    const updatedNotebook = {
      ...currentNotebook,
      cells,
      updatedAt: new Date(),
    };

    setActiveCellId(newCell.id);
    setEditingCellId(null);
    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
  }, [currentNotebook]);

  const focusNotebookName = useCallback(() => {
    notebookNameRef.current?.focus();
  }, []);

  const closeNotebook = useCallback(() => {
    setCurrentNotebook(null);
    setActiveCellId(null);
    setFullscreenCellId(null);
    setEditingCellId(null);
  }, []);

  const deleteAllCells = useCallback(() => {
    if (!currentNotebook) {
      return;
    }

    const freshCell = notebookStorage.createCell();
    const updatedNotebook = {
      ...currentNotebook,
      cells: [freshCell],
      updatedAt: new Date(),
    };

    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
    setActiveCellId(freshCell.id);
    setFullscreenCellId(null);
    setEditingCellId(null);
  }, [currentNotebook]);

  const deleteNotebook = useCallback((notebookId: string) => {
    setNotebooks(prev => {
      const index = prev.findIndex(nb => nb.id === notebookId);
      if (index === -1) {
        return prev;
      }

      const updated = prev.filter(nb => nb.id !== notebookId);

      if (currentNotebook?.id === notebookId) {
        const fallback = updated.length > 0 ? updated[0] : null;
        setCurrentNotebook(fallback);
        setActiveCellId(fallback?.cells[0]?.id ?? null);
        setFullscreenCellId(null);
        setEditingCellId(null);
      } else if (editingCellId) {
        const notebook = prev.find(nb => nb.id === notebookId);
        if (notebook?.cells.some(cell => cell.id === editingCellId)) {
          setEditingCellId(null);
        }
      }

      return updated;
    });

    setDeletedCellsHistory(prev => prev.filter(entry => entry.notebookId !== notebookId));
    setOpenNotebookMenuId(null);
  }, [currentNotebook, editingCellId]);

  const closeFullscreen = useCallback(() => {
    setFullscreenCellId(null);
  }, []);

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
      return false;
    }

    const cells = [...currentNotebook.cells];
    const sourceIndex = cells.findIndex(cell => cell.id === sourceId);
    const targetIndex = cells.findIndex(cell => cell.id === targetId);
    if (sourceIndex === -1 || targetIndex === -1) {
      return false;
    }

    const [moved] = cells.splice(sourceIndex, 1);
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
    return true;
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
    setDragOverState({ id: null, placeAfter: false });
  }, []);

  const handleDragEnd = useCallback(() => {
    setDraggingCellId(null);
    setDragOverState({ id: null, placeAfter: false });
  }, []);

  const handleDragOverCell = useCallback((cellId: string, placeAfter = false) => {
    if (!draggingCellId || draggingCellId === cellId) {
      return;
    }
    if (reorderCells(draggingCellId, cellId, placeAfter)) {
      setDragOverState({ id: cellId, placeAfter });
    }
  }, [draggingCellId, reorderCells]);

  const handleDropOnCell = useCallback((cellId: string, placeAfter = false) => {
    if (draggingCellId && draggingCellId !== cellId) {
      reorderCells(draggingCellId, cellId, placeAfter);
    }
    setDraggingCellId(null);
    setDragOverState({ id: null, placeAfter: false });
  }, [draggingCellId, reorderCells]);

  const executeCell = useCallback(async (cellId: string) => {
    const notebook = currentNotebookRef.current;
    if (!notebook) {
      return { success: false as const };
    }

    const cell = notebook.cells.find(c => c.id === cellId);
    if (!cell) {
      return { success: false as const };
    }
    setActiveCellId(cellId);

    const processingNotebook = {
      ...notebook,
      cells: notebook.cells.map(c => {
        if (c.id !== cellId) {
          return c;
        }
        const baseCell = {
          ...c,
          loading: true,
          error: undefined,
          result: undefined,
        };
        if (c.kind === 'markdown') {
          return { ...baseCell, renderedMarkdown: undefined };
        }
        return baseCell;
      }),
    };
    setCurrentNotebook(processingNotebook);
    currentNotebookRef.current = processingNotebook;

    if (cell.kind === 'markdown') {
      const finalNotebook = {
        ...processingNotebook,
        cells: processingNotebook.cells.map(c =>
          c.id === cellId
            ? {
                ...c,
                loading: false,
                renderedMarkdown: cell.sql,
                lastExecutedAt: new Date(),
              }
            : c
        ),
        updatedAt: new Date(),
      };

      setCurrentNotebook(finalNotebook);
      currentNotebookRef.current = finalNotebook;
      setNotebooks(prev => prev.map(nb => nb.id === finalNotebook.id ? finalNotebook : nb));

      return { success: true as const };
    }
    if (!cell.sql.trim()) {
      const finalNotebook = {
        ...processingNotebook,
        cells: processingNotebook.cells.map(c =>
          c.id === cellId
            ? {
                ...c,
                loading: false,
              }
            : c
        ),
        updatedAt: new Date(),
      };
      setCurrentNotebook(finalNotebook);
      currentNotebookRef.current = finalNotebook;
      setNotebooks(prev => prev.map(nb => nb.id === finalNotebook.id ? finalNotebook : nb));
      return { success: true as const };
    }

    try {
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql: cell.sql, kind: 0, dsn: currentDsn.dsn || undefined }),
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
        ...processingNotebook,
        cells: processingNotebook.cells.map(c =>
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
      currentNotebookRef.current = finalNotebook;
      setNotebooks(prev => prev.map(nb => nb.id === finalNotebook.id ? finalNotebook : nb));

      return { success: true as const };
    } catch (error) {
      console.error('Cell execution failed:', error);
      const errorMessage = (error as Error).message.replace(/\\n/g, '\n');

      const finalNotebook = {
        ...processingNotebook,
        cells: processingNotebook.cells.map(c =>
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
      currentNotebookRef.current = finalNotebook;
      setNotebooks(prev => prev.map(nb => nb.id === finalNotebook.id ? finalNotebook : nb));

      return { success: false as const, error: errorMessage };
    }
  }, [currentDsn]);

  const runCellsFrom = useCallback(async (startIndex: number) => {
    const notebook = currentNotebookRef.current;
    if (!notebook) {
      return;
    }
    const ids = notebook.cells.slice(startIndex).map(cell => cell.id);
    for (const id of ids) {
      const result = await executeCell(id);
      if (!result?.success) {
        break;
      }
    }
  }, [executeCell]);

  const runCellsTo = useCallback(async (endIndex: number) => {
    const notebook = currentNotebookRef.current;
    if (!notebook) {
      return;
    }
    const ids = notebook.cells.slice(0, Math.min(endIndex + 1, notebook.cells.length)).map(cell => cell.id);
    for (const id of ids) {
      const result = await executeCell(id);
      if (!result?.success) {
        break;
      }
    }
  }, [executeCell]);

  const deleteCell = useCallback((cellId: string, options?: { recordHistory?: boolean }) => {
    if (!currentNotebook || currentNotebook.cells.length <= 1) {
      return;
    }

    const cellIndex = currentNotebook.cells.findIndex(cell => cell.id === cellId);
    if (cellIndex === -1) {
      return;
    }

    const targetCell = currentNotebook.cells[cellIndex];
    const fallbackCell = currentNotebook.cells[cellIndex + 1] || currentNotebook.cells[cellIndex - 1];

    const updatedNotebook = {
      ...currentNotebook,
      cells: currentNotebook.cells.filter(cell => cell.id !== cellId),
      updatedAt: new Date(),
    };

    if (options?.recordHistory !== false) {
      setDeletedCellsHistory(prev => [...prev, { notebookId: currentNotebook.id, cell: targetCell, index: cellIndex }]);
    }

    setActiveCellId(prev => (prev === cellId ? fallbackCell?.id ?? null : prev));
    setEditingCellId(prev => (prev === cellId ? null : prev));
    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
    setFullscreenCellId(prev => (prev === cellId ? null : prev));
  }, [currentNotebook]);

  const undoLastDeletion = useCallback(() => {
    setDeletedCellsHistory(prev => {
      const next = [...prev];
      while (next.length > 0) {
        const entry = next.pop()!;
        const restoreNotebook = (source: Notebook): Notebook => {
          const cells = [...source.cells];
          const insertIndex = Math.min(entry.index, cells.length);
          cells.splice(insertIndex, 0, { ...entry.cell, loading: false });
          return { ...source, cells, updatedAt: new Date() };
        };

        let restored = false;
        setNotebooks(prevNotebooks => {
          if (!prevNotebooks.some(nb => nb.id === entry.notebookId)) {
            return prevNotebooks;
          }
          restored = true;
          return prevNotebooks.map(nb =>
            nb.id === entry.notebookId ? restoreNotebook(nb) : nb
          );
        });

        if (restored && currentNotebookRef.current?.id === entry.notebookId) {
          const updated = restoreNotebook(currentNotebookRef.current);
          setCurrentNotebook(updated);
          currentNotebookRef.current = updated;
          setActiveCellId(entry.cell.id);
          setEditingCellId(null);
          break;
        } else if (restored) {
          break;
        }
      }
      return next;
    });
  }, [setNotebooks]);

  const changeCellKind = useCallback((cellId: string, nextKind: 'sql' | 'markdown') => {
    if (!currentNotebook) {
      return;
    }
    const cellExists = currentNotebook.cells.some(cell => cell.id === cellId);
    if (!cellExists) {
      return;
    }

    const updatedNotebook = {
      ...currentNotebook,
      cells: currentNotebook.cells.map(cell =>
        cell.id === cellId
          ? {
              ...cell,
              kind: nextKind,
              hideEditor: false,
              result: nextKind === 'markdown' ? undefined : cell.result,
              error: nextKind === 'markdown' ? undefined : cell.error,
              renderedMarkdown: undefined,
            }
          : cell
      ),
      updatedAt: new Date(),
    };

    setCurrentNotebook(updatedNotebook);
    setNotebooks(prev => prev.map(nb => nb.id === updatedNotebook.id ? updatedNotebook : nb));
    if (nextKind === 'markdown') {
      setEditingCellId(null);
    }
  }, [currentNotebook]);

  const runAllCells = useCallback(async () => {
    await runCellsFrom(0);
  }, [runCellsFrom]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const notebook = currentNotebookRef.current;
      if (!notebook || !activeCellId) {
        return;
      }

      const isEditing = editingCellId === activeCellId;
      if (isEditing) {
        if (event.key === 'Escape') {
          event.preventDefault();
          setEditingCellId(null);
          const activeElement = document.activeElement as HTMLElement | null;
          if (activeElement && typeof activeElement.blur === 'function') {
            activeElement.blur();
          }
        }
        return;
      }

      const target = event.target as HTMLElement | null;
      const tagName = target?.tagName?.toLowerCase();
      if (tagName === 'input' || tagName === 'textarea' || target?.isContentEditable) {
        return;
      }

      const activeIndex = notebook.cells.findIndex(cell => cell.id === activeCellId);
      if (activeIndex === -1) {
        return;
      }

      if (event.metaKey || event.ctrlKey) {
        return;
      }

      const key = event.key.toLowerCase();
      if (key === 'd') {
        const prev = lastCommandRef.current;
        const now = Date.now();
        if (prev && prev.key === 'd' && now - prev.timestamp < 500) {
          event.preventDefault();
          lastCommandRef.current = null;
          deleteCell(activeCellId);
          return;
        }
        lastCommandRef.current = { key: 'd', timestamp: now };
        return;
      }

      lastCommandRef.current = null;

      switch (key) {
        case 'a':
          event.preventDefault();
          addCellAt(activeIndex);
          break;
        case 'b':
          event.preventDefault();
          addCellAt(activeIndex + 1);
          break;
        case 'x':
          event.preventDefault();
          deleteCell(activeCellId);
          break;
        case 'z':
          if (deletedCellsHistory.length > 0) {
            event.preventDefault();
            undoLastDeletion();
          }
          break;
        case 'm':
          event.preventDefault();
          changeCellKind(activeCellId, 'markdown');
          break;
        case 'y':
          event.preventDefault();
          changeCellKind(activeCellId, 'sql');
          break;
        default:
          break;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [activeCellId, editingCellId, addCellAt, deleteCell, undoLastDeletion, changeCellKind, deletedCellsHistory.length]);

  useEffect(() => {
    if (!fullscreenCellId) {
      return;
    }
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        closeFullscreen();
      }
    };
    window.addEventListener('keydown', handleEscape);
    return () => window.removeEventListener('keydown', handleEscape);
  }, [fullscreenCellId, closeFullscreen]);

  const renderAddControl = (label: 'above' | 'between' | 'below', insertIndex: number) => {
    if (fullscreenCellId) {
      return null;
    }
    const labelText = label === 'between' ? 'Add cell between' : label === 'above' ? 'Add cell above' : 'Add cell below';
    return (
      <button
        key={`add-control-${insertIndex}`}
        type="button"
        onClick={() => addCellAt(insertIndex)}
        className="group relative flex w-full items-center justify-center py-1 focus:outline-none"
        title={labelText}
      >
        <span className="pointer-events-none h-px w-full bg-gray-200 transition-colors group-hover:bg-indigo-300" />
        <span className="pointer-events-none absolute left-1/2 -translate-x-1/2 -translate-y-1/2 whitespace-nowrap rounded-full border border-gray-200 bg-white px-3 py-1 text-xs font-semibold text-gray-600 opacity-0 shadow group-hover:-translate-y-1 group-hover:opacity-100 group-hover:border-indigo-300 group-hover:text-indigo-600">
          + {labelText}
        </span>
      </button>
    );
  };

  const renderSidebar = () => (
    <div className="flex h-full flex-col border-r border-gray-200 bg-white">
      <div className="p-3 border-b border-gray-100">
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
            className="w-full rounded-lg border border-gray-200 bg-gray-50 pl-8 pr-3 py-2 text-sm text-gray-700 focus:border-indigo-400 focus:bg-white focus:outline-none"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        <div className="flex items-center justify-between px-3 py-2 text-xs font-semibold uppercase tracking-wide text-gray-500">
          <button
            type="button"
            className="inline-flex items-center gap-2 rounded-full border border-transparent px-2 py-1 text-xs font-semibold text-gray-500 hover:text-gray-700"
            onClick={() => setNotebookListCollapsed(prev => !prev)}
          >
            <span>{notebookListCollapsed ? '▶' : '▼'}</span>
            <span>Notebooks</span>
          </button>
          <button
            type="button"
            onClick={createNewNotebook}
            className="rounded-full border border-gray-200 px-2 py-1 text-sm font-semibold text-gray-600 hover:border-indigo-300 hover:text-indigo-600"
            title="Add notebook"
          >
            +
          </button>
        </div>

        {!notebookListCollapsed && (
          <div className="px-2 pb-3 space-y-1">
            {filteredNotebooks.length === 0 && (
              <div className="text-center text-sm text-gray-500 px-2 py-4">
                No notebooks found
              </div>
            )}
            {filteredNotebooks.map(nb => {
              const isActive = currentNotebook?.id === nb.id;
              const isMenuOpen = openNotebookMenuId === nb.id;
              return (
                <div key={nb.id} className="space-y-1" data-notebook-menu={isMenuOpen ? 'true' : undefined}>
                  <button
                    onClick={() => selectNotebook(nb.id)}
                    className={`w-full text-left px-3 py-2 rounded-lg border transition-all ${
                      isActive
                        ? 'border-indigo-300 bg-indigo-50/70 text-indigo-700'
                        : 'border-transparent hover:border-gray-200 hover:bg-gray-50'
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-semibold text-sm truncate">{nb.name || 'Untitled Notebook'}</span>
                      <div className="flex items-center gap-2 text-[10px] text-gray-400">
                        <span className="uppercase">{formatRelativeTime(nb.updatedAt) || 'new'}</span>
                        <div className="relative" data-notebook-menu-button>
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              setOpenNotebookMenuId(prev => (prev === nb.id ? null : nb.id));
                            }}
                            className={`rounded-full border p-1 transition ${
                              isMenuOpen ? 'border-indigo-300 text-indigo-600' : 'border-gray-200 text-gray-500 hover:border-gray-300'
                            }`}
                            title="Notebook options"
                          >
                            <EllipsisIcon className="h-3.5 w-3.5" />
                          </button>
                          {isMenuOpen && (
                            <div className="absolute right-0 mt-2 w-40 rounded-xl border border-gray-200 bg-white shadow-lg z-10">
                              <button
                                type="button"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  deleteNotebook(nb.id);
                                  setOpenNotebookMenuId(null);
                                }}
                                className="flex w-full items-center gap-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50"
                              >
                                Delete notebook
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                    <div className="text-xs text-gray-500 mt-1">
                      {nb.cells.length} cell{nb.cells.length === 1 ? '' : 's'}
                    </div>
                  </button>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );

  return (
    <div className="relative flex h-full flex-1 flex-col bg-[#f4f6fb]">
      <PanelGroup direction="horizontal" className="flex-1 overflow-hidden">
        <Panel defaultSize={15} minSize={15} maxSize={30} className="h-full">
          {renderSidebar()}
        </Panel>
        <PanelResizeHandle className="group flex w-3 cursor-col-resize items-center justify-center">
          <span className="h-10 w-0.5 rounded-full bg-gray-200 group-hover:bg-indigo-400" />
        </PanelResizeHandle>
        <Panel defaultSize={60} minSize={40} className="h-full">
          <div className="flex h-full flex-col overflow-hidden">
            {currentNotebook ? (
              <>
                <header className="border-b border-gray-200 bg-white px-4 py-3 flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <input
                      ref={notebookNameRef}
                      type="text"
                      value={currentNotebook.name}
                      onChange={(e) => updateNotebookName(currentNotebook.id, e.target.value)}
                      className="w-full border-none bg-transparent text-xl font-semibold text-gray-900 focus:outline-none"
                      placeholder="Notebook name"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Updated {formatRelativeTime(currentNotebook.updatedAt) || 'just now'} · {currentNotebook.cells.length} cell{currentNotebook.cells.length === 1 ? '' : 's'}
                    </p>
                  </div>
                  <div className="flex items-center gap-2 text-xs text-gray-500">
                    <button
                      type="button"
                      onClick={() => { void runAllCells(); }}
                      className="inline-flex items-center gap-1 rounded-full border border-indigo-200 bg-indigo-50 px-3 py-1.5 text-xs font-semibold text-indigo-600 shadow-sm hover:bg-indigo-100"
                    >
                      Run all cells
                    </button>
                    <div className="relative" ref={notebookMenuRef}>
                      <button
                        type="button"
                        onClick={() => setNotebookMenuOpen(prev => !prev)}
                        className={`rounded-full border p-1.5 transition ${
                          notebookMenuOpen ? 'border-indigo-300 text-indigo-600' : 'border-gray-200 text-gray-500 hover:border-gray-300'
                        }`}
                        title="Notebook options"
                      >
                        <EllipsisIcon className="h-4 w-4" />
                      </button>
                      {notebookMenuOpen && (
                        <div className="absolute right-0 mt-2 w-44 rounded-xl border border-gray-200 bg-white shadow-lg z-10">
                          <button
                            className="flex w-full items-center gap-2 px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                            onClick={() => {
                              focusNotebookName();
                              setNotebookMenuOpen(false);
                            }}
                          >
                            Rename
                          </button>
                          <button
                            className="flex w-full items-center gap-2 px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                            onClick={() => {
                              closeNotebook();
                              setNotebookMenuOpen(false);
                            }}
                          >
                            Close
                          </button>
                          <button
                            className="flex w-full items-center gap-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50"
                            onClick={() => {
                              deleteAllCells();
                              setNotebookMenuOpen(false);
                            }}
                          >
                            Delete all cells
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                </header>

                <div className={`flex-1 overflow-y-auto px-4 py-4 ${fullscreenCellId ? 'space-y-0' : 'space-y-3'}`}>
                  {!fullscreenCellId && renderAddControl('above', 0)}
                  {currentNotebook.cells.map((cell, index) => {
                    if (fullscreenCellId && cell.id !== fullscreenCellId) {
                      return null;
                    }
                    return (
                      <React.Fragment key={cell.id}>
                        <NotebookCellComponent
                          cell={cell}
                          index={index}
                          onSqlChange={(sql) => updateCellSql(cell.id, sql)}
                          onExecute={() => executeCell(cell.id)}
                          onDelete={() => deleteCell(cell.id)}
                          canDelete={currentNotebook.cells.length > 1}
                          isActive={cell.id === activeCellId || fullscreenCellId === cell.id}
                          isEditing={editingCellId === cell.id}
                          onSelect={() => {
                            setActiveCellId(cell.id);
                            setEditingCellId(null);
                          }}
                          onEnterEditMode={() => {
                            setActiveCellId(cell.id);
                            setEditingCellId(cell.id);
                          }}
                          onExitEditMode={() => {
                            setEditingCellId(prev => (prev === cell.id ? null : prev));
                          }}
                          onToggleCollapse={() => toggleCellSection(cell.id, 'collapsed')}
                          onToggleEditor={() => toggleCellSection(cell.id, 'hideEditor')}
                          onToggleResult={() => toggleCellSection(cell.id, 'hideResult')}
                          onToggleFullscreen={() =>
                            setFullscreenCellId(prev => (prev === cell.id ? null : cell.id))
                          }
                          isFullscreen={fullscreenCellId === cell.id}
                          onMoveUp={() => moveCellPosition(cell.id, 'up')}
                          onMoveDown={() => moveCellPosition(cell.id, 'down')}
                          dragState={{
                            isDragging: draggingCellId === cell.id,
                            isDragOver: dragOverState.id === cell.id,
                            dragOverPosition:
                              dragOverState.id === cell.id
                                ? dragOverState.placeAfter
                                  ? 'after'
                                  : 'before'
                                : undefined,
                          }}
                          onDragStart={() => handleDragStart(cell.id)}
                          onDragEnd={handleDragEnd}
                          onDragOver={(placeAfter) => handleDragOverCell(cell.id, placeAfter)}
                          onDrop={(placeAfter) => handleDropOnCell(cell.id, placeAfter)}
                          onRunFromHere={() => { void runCellsFrom(index); }}
                          onRunToHere={() => { void runCellsTo(index); }}
                        />
                        {!fullscreenCellId && renderAddControl(
                          index === currentNotebook.cells.length - 1 ? 'below' : 'between',
                          index + 1
                        )}
                  </React.Fragment>
                );
              })}
              {!fullscreenCellId && renderAddControl('below', currentNotebook.cells.length)}
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
      </PanelGroup>

      {fullscreenCellId && (
        <div className="absolute inset-x-0 top-[68px] z-10 flex justify-end px-6">
          <button
            onClick={closeFullscreen}
            className="rounded-full border border-gray-300 bg-white px-3 py-1 text-xs font-semibold text-gray-600 shadow-sm hover:border-gray-400"
          >
            Exit fullscreen ⎋
          </button>
        </div>
      )}
    </div>
  );
};

export default Notebooks;
