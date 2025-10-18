import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const ProfileGraphDashboard = dynamic(() => import('../../src/ProfileGraphDashboard'), {
  ssr: false
});

const PerfPage: React.FC = () => {
  return (
    <>
      <Head>
        <title>Databend - Performance</title>
      </Head>
      <ProfileGraphDashboard />
    </>
  );
};

export default PerfPage;
