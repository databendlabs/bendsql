import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';

export interface SavedDsn {
  id: string;
  name: string;
  dsn: string | null;
  isDefault?: boolean;
}

interface DsnContextValue {
  dsns: SavedDsn[];
  currentDsn: SavedDsn;
  currentDsnId: string;
  selectDsn: (id: string) => void;
  addDsn: (name: string, dsn: string) => SavedDsn | null;
  updateDsn: (id: string, payload: { name?: string; dsn?: string }) => SavedDsn | null;
  removeDsn: (id: string) => void;
}

const STORAGE_KEY = 'bendsql-dsns';
const DEFAULT_DSN_ID = 'default';

const DEFAULT_DSN_ENTRY: SavedDsn = Object.freeze({
  id: DEFAULT_DSN_ID,
  name: 'Default',
  dsn: null,
  isDefault: true,
});

const DsnContext = createContext<DsnContextValue | undefined>(undefined);

const generateId = (): string => {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return `dsn-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
};

const ensureDefaultFirst = (entries: SavedDsn[]): SavedDsn[] => {
  const withoutDefault = entries.filter(entry => entry.id !== DEFAULT_DSN_ID);
  return [DEFAULT_DSN_ENTRY, ...withoutDefault];
};

const normalizeCustomEntries = (raw: any): SavedDsn[] => {
  if (!Array.isArray(raw)) {
    return [];
  }
  const normalized: SavedDsn[] = [];
  raw.forEach(item => {
    if (!item || typeof item !== 'object') {
      return;
    }
    if (item.id === DEFAULT_DSN_ID) {
      return;
    }
    const id =
      typeof item.id === 'string' && item.id.trim().length > 0
        ? item.id
        : generateId();
    const name =
      typeof item.name === 'string' && item.name.trim().length > 0
        ? item.name.trim()
        : 'Custom DSN';
    const dsn =
      typeof item.dsn === 'string' && item.dsn.trim().length > 0
        ? item.dsn.trim()
        : null;
    if (!dsn) {
      return;
    }
    normalized.push({ id, name, dsn });
  });
  return normalized;
};

export const DsnProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [dsns, setDsns] = useState<SavedDsn[]>([DEFAULT_DSN_ENTRY]);
  const [currentId, setCurrentId] = useState<string>(DEFAULT_DSN_ID);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        const custom = normalizeCustomEntries(parsed?.custom);
        const list = ensureDefaultFirst(custom);
        setDsns(list);
        const storedId =
          typeof parsed?.currentId === 'string' ? parsed.currentId : DEFAULT_DSN_ID;
        const nextId = list.some(entry => entry.id === storedId)
          ? storedId
          : DEFAULT_DSN_ID;
        setCurrentId(nextId);
      } else {
        setDsns([DEFAULT_DSN_ENTRY]);
        setCurrentId(DEFAULT_DSN_ID);
      }
    } catch (error) {
      console.error('Failed to load DSNs from storage:', error);
      setDsns([DEFAULT_DSN_ENTRY]);
      setCurrentId(DEFAULT_DSN_ID);
    } finally {
      setHydrated(true);
    }
  }, []);

  useEffect(() => {
    if (!hydrated || typeof window === 'undefined') {
      return;
    }
    try {
      const payload = {
        custom: dsns.filter(entry => entry.id !== DEFAULT_DSN_ID),
        currentId,
      };
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (error) {
      console.error('Failed to save DSNs to storage:', error);
    }
  }, [dsns, currentId, hydrated]);

  const selectDsn = useCallback((id: string) => {
    setCurrentId(prev => {
      if (id === prev) {
        return prev;
      }
      return dsns.some(entry => entry.id === id) ? id : DEFAULT_DSN_ID;
    });
  }, [dsns]);

  const addDsn = useCallback((name: string, dsnValue: string) => {
    const trimmedDsn = dsnValue.trim();
    if (!trimmedDsn) {
      return null;
    }
    const entry: SavedDsn = {
      id: generateId(),
      name: name.trim() || 'Custom DSN',
      dsn: trimmedDsn,
    };
    setDsns(prev => ensureDefaultFirst([...prev, entry]));
    setCurrentId(entry.id);
    return entry;
  }, []);

  const updateDsn = useCallback((id: string, payload: { name?: string; dsn?: string }) => {
    if (id === DEFAULT_DSN_ID) {
      setDsns(prev => ensureDefaultFirst(prev));
      return DEFAULT_DSN_ENTRY;
    }
    let updatedEntry: SavedDsn | null = null;
    setDsns(prev =>
      ensureDefaultFirst(
        prev.map(entry => {
          if (entry.id !== id) {
            return entry;
          }
          const nextName =
            typeof payload.name === 'string' && payload.name.trim().length > 0
              ? payload.name.trim()
              : entry.name;
          const nextDsn =
            typeof payload.dsn === 'string' && payload.dsn.trim().length > 0
              ? payload.dsn.trim()
              : entry.dsn;
          if (!nextDsn) {
            return entry;
          }
          const nextEntry = { ...entry, name: nextName, dsn: nextDsn };
          updatedEntry = nextEntry;
          return nextEntry;
        }),
      ),
    );
    return updatedEntry;
  }, []);

  const removeDsn = useCallback((id: string) => {
    if (id === DEFAULT_DSN_ID) {
      return;
    }
    setDsns(prev => ensureDefaultFirst(prev.filter(entry => entry.id !== id)));
    setCurrentId(prev => (prev === id ? DEFAULT_DSN_ID : prev));
  }, []);

  const currentDsn = useMemo(() => {
    const found = dsns.find(entry => entry.id === currentId);
    return found ?? DEFAULT_DSN_ENTRY;
  }, [dsns, currentId]);

  const value = useMemo<DsnContextValue>(() => ({
    dsns,
    currentDsn,
    currentDsnId: currentId,
    selectDsn,
    addDsn,
    updateDsn,
    removeDsn,
  }), [dsns, currentDsn, currentId, selectDsn, addDsn, updateDsn, removeDsn]);

  return (
    <DsnContext.Provider value={value}>
      {children}
    </DsnContext.Provider>
  );
};

export const useDsn = (): DsnContextValue => {
  const context = useContext(DsnContext);
  if (!context) {
    throw new Error('useDsn must be used within a DsnProvider');
  }
  return context;
};
