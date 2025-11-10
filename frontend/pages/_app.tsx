import React from 'react';
import type { AppProps } from 'next/app';
import Layout from '../src/Layout';
import '../src/index.css';
import '../src/css/ProfileGraphDashboard.css';
import { DsnProvider } from '../src/context/DsnContext';

function MyApp({ Component, pageProps }: AppProps) {
  return (
    <DsnProvider>
      <Layout>
        <Component {...pageProps} />
      </Layout>
    </DsnProvider>
  );
}

export default MyApp;
