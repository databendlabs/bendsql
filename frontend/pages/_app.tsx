import React from 'react';
import type { AppProps } from 'next/app';
import Layout from '../src/Layout';
import '../src/index.css';
import '../src/css/ProfileGraphDashboard.css';

function MyApp({ Component, pageProps }: AppProps) {
  return (
    <Layout>
      <Component {...pageProps} />
    </Layout>
  );
}

export default MyApp;