import React from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';

import SQLQuery from './components/SQLQuery';

const container = document.getElementById('root');
const root = createRoot(container!);
root.render(<SQLQuery />);
