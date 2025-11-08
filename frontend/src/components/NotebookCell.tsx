import React, { useEffect, useMemo, useRef, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView, keymap, lineNumbers } from '@codemirror/view';
import { autocompletion } from '@codemirror/autocomplete';
import { xcodeLight, xcodeLightPatch } from './CodeMirrorTheme';
import { NotebookCell, QueryResult } from '../types/notebook';
import { formatRelativeTime } from '../utills/time';
import {
  PlayIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  ColumnsIcon,
  CodeIcon,
  GripIcon,
  ExpandIcon,
  EllipsisIcon,
} from './icons';

interface NotebookCellProps {
  cell: NotebookCell;
  index: number;
  isActive: boolean;
  canDelete: boolean;
  onSqlChange: (sql: string) => void;
  onExecute: () => void;
  onDelete: () => void;
  onSelect: () => void;
  onToggleCollapse: () => void;
  onToggleEditor: () => void;
  onToggleResult: () => void;
  onToggleFullscreen: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  isFullscreen?: boolean;
  dragState: {
    isDragging: boolean;
    isDragOver: boolean;
    dragOverPosition?: 'before' | 'after';
  };
  onDragStart: () => void;
  onDragEnd: () => void;
  onDragOver: (placeAfter?: boolean) => void;
  onDrop: (placeAfter?: boolean) => void;
  onRunFromHere?: () => void;
  onRunToHere?: () => void;
}

const NotebookCellComponent: React.FC<NotebookCellProps> = ({
  cell,
  index,
  isActive,
  canDelete,
  onSqlChange,
  onExecute,
  onDelete,
  onSelect,
  onToggleCollapse,
  onToggleEditor,
  onToggleResult,
  onToggleFullscreen,
  onMoveUp,
  onMoveDown,
  isFullscreen = false,
  dragState,
  onDragStart,
  onDragEnd,
  onDragOver,
  onDrop,
  onRunFromHere = () => {},
  onRunToHere = () => {},
}) => {
  const cardRef = useRef<HTMLDivElement | null>(null);
  const isExecutionShortcut = (event: KeyboardEvent) => {
    if (!(event.metaKey || event.ctrlKey)) {
      return false;
    }
    return event.key === 'Enter' || event.key === 'NumpadEnter' || event.key === 'Return';
  };
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const [runMenuOpen, setRunMenuOpen] = useState(false);
  const runMenuRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!menuOpen) return;
    const handleClick = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [menuOpen]);

  useEffect(() => {
    if (!runMenuOpen) return;
    const handleClick = (event: MouseEvent) => {
      if (runMenuRef.current && !runMenuRef.current.contains(event.target as Node)) {
        setRunMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [runMenuOpen]);

  const showEditor = !cell.hideEditor && !cell.collapsed;
  const showResult = !cell.hideResult && !cell.collapsed;

  const statusLabel = useMemo(() => {
    if (cell.loading) return 'Running query';
    if (cell.lastExecutedAt) {
      return `Ran ${formatRelativeTime(cell.lastExecutedAt)}`;
    }
    return 'Ready to run';
  }, [cell.loading, cell.lastExecutedAt]);

  const renderTable = (result: QueryResult) => {
    if (!result.data || result.data.length === 0) {
      return (
        <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-center text-sm text-gray-500">
          No data returned
        </div>
      );
    }

    return (
      <div className="rounded-lg border border-gray-200 overflow-hidden">
        <div className="overflow-auto max-h-72">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                {result.columns.map((column, columnIndex) => (
                  <th key={column + columnIndex} className="px-4 py-3 text-left font-semibold text-gray-700">
                    <div>{column}</div>
                    {result.types && result.types[columnIndex] && (
                      <div className="text-xs text-gray-400">{result.types[columnIndex]}</div>
                    )}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.data.map((row, rowIndex) => (
                <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  {row.map((value, cellIndex) => (
                    <td key={cellIndex} className="px-4 py-2 text-gray-800 font-mono text-xs">
                      {value}
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


  const baseCardClasses = [
    'flex',
    'gap-3',
    'rounded-2xl',
    'border',
    'bg-white/95',
    'backdrop-blur-[2px]',
    'transition-all',
    dragState.isDragOver ? 'ring-1 ring-indigo-300 border-indigo-300' : 'border-gray-100',
    isFullscreen
      ? 'shadow-[0_0_0_2px_rgba(99,102,241,0.35)] border-indigo-300'
      : isActive
        ? 'shadow-[0_15px_35px_-20px_rgba(79,70,229,0.45)]'
        : 'shadow-sm hover:shadow-md hover:border-gray-200',
    cell.loading ? 'ring-1 ring-blue-300 border-blue-200 bg-blue-50/80' : '',
  ].join(' ');

  // 放置指示器样式
  const dropIndicatorClass = [
    'absolute',
    'left-0',
    'right-0',
    'h-1',
    'bg-indigo-500/80',
    'rounded-full',
    'transition-opacity',
    'pointer-events-none',
    'opacity-0',
  ].join(' ');

  const dragTransformClass = dragState.isDragOver
    ? dragState.dragOverPosition === 'after'
      ? 'translate-y-1.5'
      : '-translate-y-1.5'
    : 'translate-y-0';
  const showTopIndicator = dragState.isDragOver && dragState.dragOverPosition === 'before';
  const showBottomIndicator = dragState.isDragOver && dragState.dragOverPosition === 'after';

  const editorHeight = isFullscreen ? 'auto' : 'auto';
  const editorMinHeight = isFullscreen ? '44px' : '44px';
  const showSideControls = !isFullscreen;
  const showMenu = !isFullscreen;

  const runKeymap = useMemo(() => keymap.of([
    {
      key: 'Mod-Enter',
      run: () => {
        onExecute();
        return true;
      },
      preventDefault: true,
    },
    {
      key: 'Mod-NumpadEnter',
      run: () => {
        onExecute();
        return true;
      },
      preventDefault: true,
    },
    {
      key: 'Alt-Enter',
      run: () => {
        onRunFromHere?.();
        return true;
      },
      preventDefault: true,
    },
    {
      key: 'Shift-Enter',
      run: () => {
        onRunToHere?.();
        return true;
      },
      preventDefault: true,
    },
  ]), [onExecute, onRunFromHere, onRunToHere]);

  const getPlaceAfter = (event: React.DragEvent) => {
    if (!cardRef.current) {
      return false;
    }
    const rect = cardRef.current.getBoundingClientRect();
    return event.clientY - rect.top > rect.height / 2;
  };

  const handleDragOver = (event: React.DragEvent) => {
    event.preventDefault();
    onDragOver(getPlaceAfter(event));
  };

  const handleDrop = (event: React.DragEvent) => {
    event.preventDefault();
    onDrop(getPlaceAfter(event));
  };

  const handleDragStartWrapper = (event: React.DragEvent) => {
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', cell.id);
    if (cardRef.current) {
      const rect = cardRef.current.getBoundingClientRect();
      event.dataTransfer.setDragImage(
        cardRef.current,
        event.clientX - rect.left,
        event.clientY - rect.top,
      );
    }
    onDragStart();
  };

  const handleDragEndWrapper = (event: React.DragEvent) => {
    event.preventDefault();
    onDragEnd();
  };

  return (
    <div className="relative">
      {showTopIndicator && (
        <div className={`${dropIndicatorClass} top-0 opacity-100`} />
      )}

      <div
        ref={cardRef}
        className={`${baseCardClasses} ${isFullscreen ? 'h-full min-h-[calc(100vh-180px)]' : ''} ${dragState.isDragging ? 'opacity-50 cursor-grabbing' : ''} transform transition-transform duration-150 ${dragTransformClass}`}
        onClick={onSelect}
        draggable={!isFullscreen}
        onDragStart={handleDragStartWrapper}
        onDragEnd={handleDragEndWrapper}
        onDragOver={handleDragOver}
        onDrop={handleDrop}
      >
        {showSideControls && (
          <div className="flex flex-col items-center gap-2 border-r border-gray-100 px-2 py-3">
            <div
              className={`cursor-grab rounded-full border bg-white p-2 text-gray-400 hover:text-gray-600 ${
                dragState.isDragging ? 'border-indigo-300 text-indigo-500 cursor-grabbing' : 'border-gray-200'
              }`}
            >
              <GripIcon className="h-4 w-4" />
            </div>
            <div className="flex flex-col gap-1 text-gray-400">
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleCollapse();
                }}
                className={`rounded-md border px-1.5 py-1 text-xs ${cell.collapsed ? 'border-indigo-300 text-indigo-500' : 'border-gray-200 hover:text-gray-600'}`}
                title={cell.collapsed ? 'Expand cell' : 'Collapse cell'}
              >
                {cell.collapsed ? <ChevronDownIcon className="h-3 w-3" /> : <ChevronUpIcon className="h-3 w-3" />}
              </button>
              {!cell.collapsed && (
                <>
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      onToggleEditor();
                    }}
                    className={`rounded-md border px-1.5 py-1 text-xs ${cell.hideEditor ? 'border-indigo-300 text-indigo-500' : 'border-gray-200 hover:text-gray-600'}`}
                    title={cell.hideEditor ? 'Show SQL editor' : 'Collapse SQL editor'}
                  >
                    <CodeIcon className="h-3.5 w-3.5" />
                  </button>
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      onToggleResult();
                    }}
                    className={`rounded-md border px-1.5 py-1 text-xs ${cell.hideResult ? 'border-indigo-300 text-indigo-500' : 'border-gray-200 hover:text-gray-600'}`}
                    title={cell.hideResult ? 'Show results' : 'Collapse results'}
                  >
                    <ColumnsIcon className="h-3.5 w-3.5" />
                  </button>
                </>
              )}
            </div>
          </div>
        )}

      <div className={`flex-1 ${showSideControls ? 'p-3' : 'p-1 sm:p-3'} ${showSideControls ? '' : 'md:px-6'}`} draggable={false}>
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <div
                className="relative"
                ref={runMenuRef}
                onMouseEnter={() => {
                  if (!cell.loading && cell.sql.trim()) {
                    setRunMenuOpen(true);
                  }
                }}
                onMouseLeave={() => setRunMenuOpen(false)}
              >
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    if (cell.loading || !cell.sql.trim()) {
                      return;
                    }
                    onExecute();
                    setRunMenuOpen(false);
                  }}
                  disabled={cell.loading || !cell.sql.trim()}
                  className={`flex h-8 w-8 items-center justify-center rounded-full border text-sm font-semibold transition ${
                    cell.loading || !cell.sql.trim()
                      ? 'border-gray-200 text-gray-400 cursor-not-allowed'
                      : runMenuOpen
                        ? 'border-indigo-400 text-indigo-600 bg-indigo-50'
                        : 'border-gray-300 text-gray-600 hover:border-indigo-300 hover:text-indigo-600'
                  }`}
                  title="Run cell"
                >
                  <PlayIcon className="h-4 w-4" />
                </button>
                {runMenuOpen && !cell.loading && cell.sql.trim() && (
                  <div className="absolute left-0 mt-2 w-44 rounded-xl border border-gray-200 bg-white shadow-lg z-10">
                    <button
                      className="flex w-full items-center justify-between px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                      onClick={(e) => {
                        e.stopPropagation();
                        onExecute();
                        setRunMenuOpen(false);
                      }}
                    >
                      Run cell
                      <span className="text-xs text-gray-400">⌘⏎</span>
                    </button>
                    <button
                      className="flex w-full items-center justify-between px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                      onClick={(e) => {
                        e.stopPropagation();
                        onRunFromHere();
                        setRunMenuOpen(false);
                      }}
                    >
                      Run from here
                      <span className="text-xs text-gray-400">⌥⏎</span>
                    </button>
                    <button
                      className="flex w-full items-center justify-between px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                      onClick={(e) => {
                        e.stopPropagation();
                        onRunToHere();
                        setRunMenuOpen(false);
                      }}
                    >
                      Run to here
                      <span className="text-xs text-gray-400">⇧⏎</span>
                    </button>
                  </div>
                )}
              </div>
              <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-semibold ${cell.loading ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'}`}>
                {statusLabel}
              </span>
            </div>

            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleFullscreen();
                }}
                className={`rounded-full border p-1.5 text-gray-500 hover:text-gray-700 ${
                  isFullscreen ? 'border-indigo-300 text-indigo-500' : 'border-gray-200 hover:border-gray-300'
                }`}
                title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen cell'}
              >
                <ExpandIcon className="h-4 w-4" />
              </button>
              {showMenu && (
                <div className="relative" ref={menuRef}>
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setMenuOpen((prev) => !prev);
                    }}
                    className="rounded-full border border-gray-200 p-1.5 text-gray-400 hover:border-gray-300 hover:text-gray-600"
                    title="Cell options"
                  >
                    <EllipsisIcon className="h-4 w-4" />
                  </button>
                  {menuOpen && (
                    <div className="absolute right-0 mt-2 w-40 rounded-xl border border-gray-200 bg-white shadow-lg z-10">
                      <button
                        className="flex w-full items-center justify-between px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                        onClick={(e) => {
                          e.stopPropagation();
                          onMoveUp();
                          setMenuOpen(false);
                        }}
                      >
                        Move cell up
                        <span className="text-xs text-gray-400">⌘↑</span>
                      </button>
                      <button
                        className="flex w-full items-center justify-between px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
                        onClick={(e) => {
                          e.stopPropagation();
                          onMoveDown();
                          setMenuOpen(false);
                        }}
                      >
                        Move cell down
                        <span className="text-xs text-gray-400">⌘↓</span>
                      </button>
                      {canDelete && (
                        <button
                          className="flex w-full items-center justify-between px-3 py-2 text-sm text-red-600 hover:bg-red-50"
                          onClick={(e) => {
                            e.stopPropagation();
                            onDelete();
                            setMenuOpen(false);
                          }}
                        >
                          Delete
                        </button>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          {!cell.collapsed && (
            <div className={`mt-2 space-y-3 ${isFullscreen ? 'flex-1 flex flex-col overflow-hidden' : ''}`} onClick={onSelect}>
              {showEditor ? (
                <div className="rounded-xl bg-white">
                  <CodeMirror
                    value={cell.sql}
                    theme={xcodeLight}
                    height={editorHeight}
                    extensions={[
                      xcodeLightPatch,
                      EditorView.lineWrapping,
                      lineNumbers(),
                      autocompletion({ icons: false }),
                      sql(),
                      runKeymap,
                    ]}
                    basicSetup={false}
                    onChange={onSqlChange}
                    editable={!cell.loading}
                    onFocus={onSelect}
                    style={{
                      fontSize: '14px',
                      height: editorHeight,
                      minHeight: editorMinHeight,
                    }}
                  />
                </div>
              ) : (
                <div
                  className="w-full rounded-xl border border-dashed border-gray-200 px-3 py-2 text-left text-sm text-gray-600 bg-white cursor-pointer hover:border-indigo-300 hover:text-indigo-600"
                  onClick={(e) => {
                    e.stopPropagation();
                    onToggleEditor();
                  }}
                >
                  <div className="font-mono whitespace-pre-wrap overflow-hidden" style={{ maxHeight: '48px' }}>
                    {cell.sql || 'SQL editor collapsed'}
                  </div>
                  <div className="text-xs text-indigo-500 font-semibold mt-1">Show editor</div>
                </div>
              )}

              {cell.error && showResult && (
                <div className="rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                  {cell.error}
                </div>
              )}

              {!cell.error && showResult && cell.result && (
                <div className={`${isFullscreen ? 'flex-1 flex flex-col' : ''}`}>
                <div className="text-xs font-semibold uppercase tracking-wide text-gray-400">
                  {cell.result.rowCount} row{cell.result.rowCount === 1 ? '' : 's'} returned {cell.result.duration ? `in ${cell.result.duration}` : ''}
                </div>
                <div className={`${isFullscreen ? 'flex-1 overflow-auto rounded-xl border border-gray-100 bg-white mt-2' : 'space-y-3'}`}>
                  {renderTable(cell.result)}
                </div>
              </div>
              )}

              {!cell.error && !showResult && !cell.collapsed}
            </div>
          )}
        </div>
      </div>

      {/* 下方放置指示器 */}
      {showBottomIndicator && (
        <div className={`${dropIndicatorClass} bottom-0 opacity-100`} />
      )}
    </div>
  );
};

export default NotebookCellComponent;
