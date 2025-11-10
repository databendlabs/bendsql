import React, { useEffect, useRef, useState } from 'react';
import { useDsn } from '../context/DsnContext';

const DsnSelector: React.FC = () => {
  const { dsns, currentDsn, selectDsn, addDsn, removeDsn } = useDsn();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [dsn, setDsn] = useState('');
  const [error, setError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) {
      return;
    }
    const handleClick = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  useEffect(() => {
    if (error) {
      setError(null);
    }
  }, [dsn, name, error]);

  const handleAdd = (event: React.FormEvent) => {
    event.preventDefault();
    const trimmedDsn = dsn.trim();
    if (!trimmedDsn) {
      setError('Please enter a DSN value');
      return;
    }
    const entryName = name.trim() || `Connection ${dsns.length}`;
    const entry = addDsn(entryName, trimmedDsn);
    if (entry) {
      setName('');
      setDsn('');
      setError(null);
      setOpen(false);
    }
  };

  const handleRemove = (event: React.MouseEvent, id: string) => {
    event.stopPropagation();
    removeDsn(id);
  };

  return (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={() => setOpen(prev => !prev)}
        className="flex items-center gap-2 rounded-full border border-black/10 bg-white/80 px-4 py-1.5 text-left shadow-sm hover:shadow transition text-sm"
        title="Select DSN"
      >
        <div className="flex flex-col">
          <span className="text-gray-900 font-semibold">{currentDsn.name}</span>
        </div>
        <svg
          className={`h-3 w-3 text-gray-500 transition-transform ${open ? 'rotate-180' : ''}`}
          viewBox="0 0 12 8"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
          aria-hidden="true"
        >
          <path
            d="M11 1.5 6 6.5 1 1.5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>

      {open && (
        <div className="absolute right-0 z-20 mt-2 w-80 rounded-2xl border border-gray-200 bg-white shadow-xl">
          <div className="border-b border-gray-100 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-wide text-gray-500">
              Connections
            </p>
            <p className="text-xs text-gray-400">Stored locally in this browser</p>
          </div>

          <div className="max-h-72 overflow-y-auto px-3 py-3 space-y-2">
            {dsns.map(entry => (
              <div key={entry.id} className="rounded-xl border border-gray-200 bg-white">
                <button
                  type="button"
                  onClick={() => {
                    selectDsn(entry.id);
                    setOpen(false);
                  }}
                  className={`w-full rounded-xl px-3 py-3 text-left transition ${
                    currentDsn.id === entry.id
                      ? 'border border-indigo-200 bg-indigo-50'
                      : 'hover:border hover:border-gray-200 hover:bg-gray-50'
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-gray-900">{entry.name}</p>
                      <p className="text-xs text-gray-500 break-all">
                        {entry.dsn ?? 'Use BendSQL web DSN'}
                      </p>
                    </div>
                    {currentDsn.id === entry.id && (
                      <span className="rounded-full bg-indigo-100 px-2 py-0.5 text-[10px] font-semibold uppercase text-indigo-600">
                        Active
                      </span>
                    )}
                  </div>
                </button>
                {!entry.isDefault && (
                  <div className="flex justify-end px-2 pb-2">
                    <button
                      type="button"
                      onClick={(event) => handleRemove(event, entry.id)}
                      className="text-xs font-medium text-red-500 hover:text-red-600"
                    >
                      Remove
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>

          <div className="border-t border-gray-100 px-4 py-3">
            <form className="space-y-2" onSubmit={handleAdd}>
              <div>
                <label className="text-xs font-semibold text-gray-500">Name</label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="My connection"
                  className="mt-1 w-full rounded-lg border border-gray-200 px-3 py-2 text-sm text-gray-700 focus:border-indigo-400 focus:outline-none"
                />
              </div>
              <div>
                <label className="text-xs font-semibold text-gray-500">DSN</label>
                <input
                  type="text"
                  value={dsn}
                  onChange={(e) => setDsn(e.target.value)}
                  placeholder="databend://user:pass@host:port/default"
                  className="mt-1 w-full rounded-lg border border-gray-200 px-3 py-2 text-sm text-gray-700 focus:border-indigo-400 focus:outline-none"
                  required
                />
              </div>
              {error && <p className="text-xs text-red-500">{error}</p>}
              <div className="flex items-center justify-between">
                <span className="text-[11px] uppercase tracking-wide text-gray-400">
                DSN is only stored locally
                </span>
                <button
                  type="submit"
                  className="rounded-full bg-indigo-600 px-4 py-1.5 text-xs font-semibold uppercase tracking-wide text-white shadow-sm hover:bg-indigo-500 disabled:opacity-50"
                  disabled={!dsn.trim()}
                >
                  Add DSN
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default DsnSelector;
