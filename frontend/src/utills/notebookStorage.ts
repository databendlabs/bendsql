import { Notebook, NotebookCell, NotebookStorage, NotebookCellKind } from '../types/notebook';

const STORAGE_KEY = 'bendsql-notebooks';

export const notebookStorage = {
  // Get all notebooks from localStorage
  getNotebooks(): NotebookStorage {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const data = JSON.parse(stored);
        // Convert date strings back to Date objects
        return {
          notebooks: data.notebooks.map((nb: any) => ({
            ...nb,
            createdAt: new Date(nb.createdAt),
            updatedAt: new Date(nb.updatedAt),
            cells: nb.cells.map((cell: any) => ({
              ...cell,
              collapsed: cell.collapsed ?? false,
              hideEditor: cell.hideEditor ?? false,
              hideResult: cell.hideResult ?? false,
              lastExecutedAt: cell.lastExecutedAt ? new Date(cell.lastExecutedAt) : undefined,
              kind: (cell.kind as NotebookCellKind) ?? 'sql',
              renderedMarkdown: typeof cell.renderedMarkdown === 'string' ? cell.renderedMarkdown : undefined,
            })),
          })),
          currentNotebookId: data.currentNotebookId,
        };
      }
    } catch (error) {
      console.error('Failed to load notebooks from storage:', error);
    }

    // Return default empty state
    return { notebooks: [] };
  },

  // Save notebooks to localStorage
  saveNotebooks(storage: NotebookStorage): void {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(storage));
    } catch (error) {
      console.error('Failed to save notebooks to storage:', error);
    }
  },

  // Create a new notebook
  createNotebook(name: string = 'Untitled Notebook'): Notebook {
    const now = new Date();
    return {
      id: generateId(),
      name,
      cells: [{
        id: generateId(),
        sql: '',
        loading: false,
        collapsed: false,
        hideEditor: false,
        hideResult: false,
        kind: 'sql',
        renderedMarkdown: undefined,
      }],
      createdAt: now,
      updatedAt: now,
    };
  },

  // Create a new cell
  createCell(): NotebookCell {
    return {
      id: generateId(),
      sql: '',
      loading: false,
      collapsed: false,
      hideEditor: false,
      hideResult: false,
      kind: 'sql',
      renderedMarkdown: undefined,
    };
  },
};

function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).substr(2);
}
