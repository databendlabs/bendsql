import React from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';

import SQLQuery from './SQLQuery';
import ProfileGraphDashboard from './ProfileGraphDashboard';

const App: React.FC = () => {
  const pathname = window.location.pathname;

  // Route to ProfileGraphDashboard for /perf/* paths
  if (pathname.startsWith('/perf/')) {
    return <ProfileGraphDashboard />;
  }

  // Default to SQLQuery for all other paths
  return <SQLQuery />;
};

const container = document.getElementById('root');
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
