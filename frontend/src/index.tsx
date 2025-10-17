import React from 'react';
import ReactDOM from 'react-dom';
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

ReactDOM.render(<App />, document.getElementById('root'));
