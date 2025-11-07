export interface QueryResult {
  columns: string[];
  types: string[];
  data: string[][];
  rowCount: number;
  duration: string;
}

export interface NotebookCell {
  id: string;
  sql: string;
  result?: QueryResult;
  error?: string;
  loading: boolean;
  lastExecutedAt?: Date;
  collapsed?: boolean;
  hideEditor?: boolean;
  hideResult?: boolean;
}

export interface Notebook {
  id: string;
  name: string;
  cells: NotebookCell[];
  createdAt: Date;
  updatedAt: Date;
}

export interface NotebookStorage {
  notebooks: Notebook[];
  currentNotebookId?: string;
}
