import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const Notebooks = dynamic(() => import('../src/Notebooks'), {
  ssr: false
});

const NotebooksPage: React.FC = () => {
  return (
    <>
      <Head>
        <title>Databend - Notebooks</title>
      </Head>
      <Notebooks />
    </>
  );
};

export default NotebooksPage;