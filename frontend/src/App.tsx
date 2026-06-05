import { useState } from 'react';
import { Layout } from './components/Layout';
import { Dashboard } from './pages/Dashboard';
import { Admin } from './pages/Admin';

function App() {
  const [currentView, setCurrentView] = useState<'dashboard' | 'admin'>('dashboard');

  return (
    <Layout currentView={currentView} onNavigate={setCurrentView}>
      {currentView === 'dashboard' ? <Dashboard /> : <Admin />}
    </Layout>
  )
}

export default App
