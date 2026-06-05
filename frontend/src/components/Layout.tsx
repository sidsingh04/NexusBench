import type { ReactNode } from 'react';
import './Layout.css';

interface LayoutProps {
  children: ReactNode;
  currentView?: 'dashboard' | 'admin';
  onNavigate?: (view: 'dashboard' | 'admin') => void;
}

export function Layout({ children, currentView = 'dashboard', onNavigate }: LayoutProps) {
  return (
    <div className="layout-container">
      <header className="layout-header glass-panel">
        <div className="header-content">
          <div className="brand">
            <h1 className="text-gradient">NexusBench</h1>
          </div>
          
          <nav className="nav-links">
            <button 
              className={`nav-link ${currentView === 'dashboard' ? 'active' : ''}`}
              onClick={() => onNavigate && onNavigate('dashboard')}
              style={{ background: 'transparent', border: 'none', cursor: 'pointer', fontFamily: 'inherit', fontSize: '1rem' }}
            >
              Dashboard
            </button>
            <button 
              className={`nav-link admin-link ${currentView === 'admin' ? 'active' : ''}`}
              onClick={() => onNavigate && onNavigate('admin')}
              style={{ background: 'transparent', border: 'none', cursor: 'pointer', fontFamily: 'inherit', fontSize: '1rem' }}
            >
              Admin
            </button>
          </nav>
        </div>
      </header>
      
      <main className="layout-main">
        {children}
      </main>
      
      <footer className="layout-footer">
        <p>&copy; 2026 NexusBench Platform. High-Frequency Trading Benchmark.</p>
      </footer>
    </div>
  );
}
