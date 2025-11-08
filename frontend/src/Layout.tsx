import React from 'react';
import { useRouter } from 'next/router';
import Link from 'next/link';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout: React.FC<LayoutProps> = ({ children }) => {
  const router = useRouter();

  const navItems = [
    { name: 'Query', path: '/', icon: '📊' },
    { name: 'Notebooks', path: '/notebooks', icon: '📓' },
    { name: 'Perf', path: '/perf/0', icon: '⚡' },
  ];

  const isActive = (path: string) => {
    const currentPath = router.asPath.split('?')[0] || '/';
    if (path === '/') {
      return currentPath === '/';
    }
    return currentPath.startsWith(path);
  };

  return (
    <div className="flex h-screen min-h-screen flex-col bg-[#ffe895]">
      <header className="border-b border-yellow-300 bg-yellow-400">
        <div className="flex w-full items-center justify-between gap-4 px-6 py-3">
          <div className="flex items-center gap-4">
            <span className="text-lg font-bold text-gray-900">BendSQL</span>
            <nav className="flex items-center gap-2">
              {navItems.map(item => (
                <Link
                  key={item.path}
                  href={item.path}
                  className={`inline-flex items-center gap-2 rounded-full px-4 py-1.5 text-sm font-semibold transition ${
                    isActive(item.path)
                      ? 'bg-white text-gray-900 shadow'
                      : 'text-gray-900/70 hover:text-gray-900'
                  }`}
                >
                  <span>{item.icon}</span>
                  <span>{item.name}</span>
                </Link>
              ))}
            </nav>
          </div>
        </div>
      </header>
      <main className="flex-1 overflow-hidden bg-[#f9fbff] flex flex-col min-h-0 h-full">
        <div className="flex-1 min-h-0 flex flex-col h-full">
          {children}
        </div>
      </main>
    </div>
  );
};

export default Layout;
