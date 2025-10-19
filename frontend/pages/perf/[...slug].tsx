import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const PerfQuery = dynamic(() => import('../../src/PerfQuery'), {
  ssr: false
});

const PerfPage: React.FC = () => {
  return (
    <>
      <Head>
        <title>Databend - Performance Analysis</title>
      </Head>
      <PerfQuery />
    </>
  );
};

export default PerfPage;
