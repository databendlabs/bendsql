import React, { useEffect, useMemo, useRef, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
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
  onMoveUp: () => void;
  onMoveDown: () => void;
  dragState: {
    isDragging: boolean;
    isDragOver: boolean;
  };
  onDragStart: () => void;
  onDragEnd: () => void;
  onDragOver: () => void;
  onDrop: () => void;
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
  onMoveUp,
  onMoveDown,
  dragState,
  onDragStart,
  onDragEnd,
  onDragOver,
  onDrop,
}) => {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement | null>(null);

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
        <div className="rounded-xl border border-gray-200 bg-gray-50 p-4 text-center text-sm text-gray-500">
          No data returned
        </div>
      );
    }

    return (
      <div className="rounded-xl border border-gray-200 overflow-hidden">
        <div className="overflow-auto max-h-80">
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

  const renderTypeInfo = (result: QueryResult) => {
    if (!result.types || result.types.length === 0) {
      return (
        <div className="rounded-xl border border-dashed border-gray-200 p-4 text-sm text-gray-500">
          No type information available
        </div>
      );
    }

    return (
      <div className="rounded-xl border border-dashed border-gray-200 p-4">
        <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-2">
          Column types
        </p>
        <div className="space-y-1">
          {result.columns.map((column, idx) => (
            <div key={column + idx} className="flex items-center justify-between text-sm">
              <span className="text-gray-700">{column}</span>
              <span className="font-mono text-xs text-gray-500">{result.types[idx]}</span>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const baseCardClasses = [
    'flex',
    'gap-4',
    'rounded-2xl',
    'border',
    'bg-white',
    'shadow-sm',
    'transition-all',
    dragState.isDragOver ? 'ring-2 ring-indigo-300 border-indigo-300' : 'border-gray-200',
    isActive ? 'shadow-[0_0_0_2px_rgba(99,102,241,0.15)]' : 'hover:border-gray-300',
  ].join(' ');

  const handleDragOver = (event: React.DragEvent) => {
    event.preventDefault();
    onDragOver();
  };

  const handleDrop = (event: React.DragEvent) => {
    event.preventDefault();
    onDrop();
  };

  return (
    <div
      className={baseCardClasses}
      onClick={onSelect}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      <div className="flex flex-col items-center gap-3 border-r border-gray-100 px-2 py-4">
        <div
          className={`cursor-grab rounded-full border bg-white p-2 text-gray-400 hover:text-gray-600 ${
            dragState.isDragging ? 'border-indigo-300 text-indigo-500 cursor-grabbing' : 'border-gray-200'
          }`}
          draggable
          onDragStart={(event) => {
            event.dataTransfer.effectAllowed = 'move';
            event.dataTransfer.setData('text/plain', cell.id);
            onDragStart();
          }}
          onDragEnd={(event) => {
            event.preventDefault();
            onDragEnd();
          }}
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
        </div>
      </div>

      <div className="flex-1 px-4 py-4" draggable={false}>
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onExecute();
              }}
              disabled={cell.loading || !cell.sql.trim()}
              className={`inline-flex items-center rounded-full border px-3 py-1 text-sm font-semibold ${
                cell.loading || !cell.sql.trim()
                  ? 'border-gray-200 text-gray-400 cursor-not-allowed'
                  : 'border-indigo-200 text-indigo-600 hover:bg-indigo-50'
              }`}
            >
              <PlayIcon className="mr-1 h-3.5 w-3.5" />
              Run
            </button>
            <div>
              <p className="text-sm font-semibold text-gray-900">Cell {index + 1}</p>
              <p className="text-xs text-gray-500">{statusLabel}</p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onSelect();
              }}
              className="rounded-full border border-gray-200 p-1.5 text-gray-400 hover:border-gray-300 hover:text-gray-600"
              title="Expand cell"
            >
              <ExpandIcon className="h-4 w-4" />
            </button>
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
          </div>
        </div>

        {!cell.collapsed && (
          <div className="mt-4 space-y-4" onClick={onSelect}>
            {showEditor ? (
              <div className="rounded-2xl border border-gray-200 bg-gray-50 focus-within:border-indigo-300">
                <CodeMirror
                  value={cell.sql}
                  placeholder="Type SQL here..."
                  theme={xcodeLight}
                  height="220px"
                  extensions={[
                    xcodeLightPatch,
                    EditorView.lineWrapping,
                    autocompletion({ icons: false }),
                    sql(),
                  ]}
                  basicSetup={{
                    lineNumbers: true,
                    foldGutter: false,
                    indentOnInput: false,
                    autocompletion: true,
                    highlightActiveLine: false,
                  }}
                  onChange={onSqlChange}
                  editable={!cell.loading}
                  onFocus={onSelect}
                  style={{
                    fontSize: '14px',
                    height: '220px',
                  }}
                />
              </div>
            ) : (
              <button
                type="button"
                className="w-full rounded-xl border border-dashed border-gray-300 px-3 py-2 text-left text-sm text-gray-500 hover:border-indigo-300"
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleEditor();
                }}
              >
                SQL editor collapsed – tap to expand
              </button>
            )}

            {cell.error && showResult && (
              <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">
                {cell.error}
              </div>
            )}

            {!cell.error && showResult && cell.result && (
              <div className="space-y-3">
                <div className="text-xs font-semibold uppercase tracking-wide text-gray-400">
                  {cell.result.rowCount} row{cell.result.rowCount === 1 ? '' : 's'} returned in {cell.result.duration}
                </div>
                {renderTable(cell.result)}
                {renderTypeInfo(cell.result)}
              </div>
            )}

            {!cell.error && showResult && !cell.result && (
              <div className="rounded-2xl border border-dashed border-gray-200 p-4 text-sm text-gray-500">
                Execute the query to see results and schema details.
              </div>
            )}

            {!showResult && (
              <button
                type="button"
                className="w-full rounded-xl border border-dashed border-gray-300 px-3 py-2 text-left text-sm text-gray-500 hover:border-indigo-300"
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleResult();
                }}
              >
                Result set hidden – tap to expand
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default NotebookCellComponent;
