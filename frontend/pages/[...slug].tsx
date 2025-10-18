import React from 'react';
import Head from 'next/head';
import SQLQuery from '../src/SQLQuery';

const CatchAllPage: React.FC = () => {
  return (
    <>
      <Head>
        <title>Databend</title>
      </Head>
      <SQLQuery />
    </>
  );
};

export default CatchAllPage;