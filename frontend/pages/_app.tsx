import React from 'react';
import type { AppProps } from 'next/app';
import '../src/index.css';
import '../src/css/ProfileGraphDashboard.css';

function MyApp({ Component, pageProps }: AppProps) {
  return <Component {...pageProps} />;
}

export default MyApp;