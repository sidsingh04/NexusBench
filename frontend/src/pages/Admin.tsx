import { useState, useEffect } from 'react';
import type { FormEvent } from 'react';
import { api, ApiError } from '../api/client';

interface Contest {
  id: string;
  name: string;
  status: string;
  created_at: string;
}

interface LeaderboardEntry {
  rank: number;
  team_name: string;
  final_score: number;
  best_p99_ms: number;
  peak_sustained_tps: number;
  avg_correctness: number;
}

function formatNumber(num: number): string {
  if (num === undefined || num === null) return '0.00';
  return num.toFixed(2);
}

export function Admin() {
  const [adminKey, setAdminKey] = useState<string>('testkey');
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  
  // Create Contest State
  const [contestName, setContestName] = useState<string>('');
  
  // Target Contest ID for operations
  const [targetContestId, setTargetContestId] = useState<string>('');
  
  // Past contests state
  const [pastContests, setPastContests] = useState<Contest[]>([]);
  const [loadingHistory, setLoadingHistory] = useState<boolean>(false);

  // Snapshot state
  const [snapshotEntries, setSnapshotEntries] = useState<LeaderboardEntry[] | null>(null);
  const [viewingContest, setViewingContest] = useState<Contest | null>(null);

  const displayError = (err: unknown) => {
    if (err instanceof ApiError) {
      setError(`[${err.status}] ${err.code}: ${err.message}`);
    } else {
      setError(String(err));
    }
    setSuccess(null);
  };

  const displaySuccess = (msg: string) => {
    setSuccess(msg);
    setError(null);
  };

  const handleCreateContest = async (e: FormEvent) => {
    e.preventDefault();
    try {
      const contest = await api.post<Contest>(
        '/admin/contests', 
        { name: contestName, use_defaults: true },
        adminKey
      );
      displaySuccess(`Contest created! ID: ${contest.id}`);
      setTargetContestId(contest.id);
      setContestName('');
    } catch (err) {
      displayError(err);
    }
  };

  const handleActivateContest = async (e: FormEvent) => {
    e.preventDefault();
    try {
      await api.post(`/admin/contests/${targetContestId}/activate`, null, adminKey);
      displaySuccess(`Contest ${targetContestId} is now ACTIVE.`);
    } catch (err) {
      displayError(err);
    }
  };

  const handleCloseContest = async (e: FormEvent) => {
    e.preventDefault();
    try {
      await api.post(`/admin/contests/${targetContestId}/close`, null, adminKey);
      displaySuccess(`Contest ${targetContestId} closed successfully.`);
      fetchPastContests(); // Refresh history
    } catch (err) {
      displayError(err);
    }
  };
  const fetchPastContests = async () => {
    setLoadingHistory(true);
    try {
      const data = await api.get<{count: number, contests: Contest[]}>('/admin/contests', adminKey);
      const sorted = (data.contests || []).sort((a, b) => 
        new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      );
      setPastContests(sorted);
      setError(null);
    } catch (err) {
      displayError(err);
    } finally {
      setLoadingHistory(false);
    }
  };

  const fetchSnapshot = async (contest: Contest) => {
    try {
      const data = await api.get<{entries: LeaderboardEntry[]}>(`/admin/contests/${contest.id}/leaderboard`, adminKey);
      setSnapshotEntries(data.entries || []);
      setViewingContest(contest);
    } catch (err) {
      displayError(err);
    }
  };

  // Load history on mount if admin key is set
  useEffect(() => {
    if (adminKey) {
      fetchPastContests();
    }
  }, []);

  return (
    <div className="grid-layout" style={{ gap: '2rem' }}>
      {/* Left Column: Controls */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
        
        {/* Auth Panel */}
        <div className="glass-panel" style={{ padding: '2rem' }}>
          <h3 style={{ marginBottom: '1.5rem', color: 'var(--text-primary)' }}>Admin Authentication</h3>
          <div className="form-group">
            <label className="form-label" htmlFor="adminKey">Admin API Key</label>
            <input 
              className="input-field" 
              type="password" 
              id="adminKey" 
              value={adminKey}
              onChange={(e) => setAdminKey(e.target.value)}
              placeholder="Enter Admin Secret" 
            />
          </div>
        </div>

        {/* Global Notifications */}
        {(error || success) && (
          <div className="glass-panel" style={{ padding: '1rem' }}>
            {error && <div style={{ color: '#FCA5A5' }}>{error}</div>}
            {success && <div style={{ color: '#6EE7B7' }}>{success}</div>}
          </div>
        )}

        {/* Create Contest */}
        <div className="glass-panel" style={{ padding: '2rem' }}>
          <h3 style={{ marginBottom: '1.5rem', color: 'var(--text-primary)' }}>Create Contest</h3>
          <form onSubmit={handleCreateContest}>
            <div className="form-group">
              <label className="form-label" htmlFor="contestName">Contest Name</label>
              <input 
                className="input-field" 
                type="text" 
                id="contestName" 
                value={contestName}
                onChange={(e) => setContestName(e.target.value)}
                placeholder="e.g., Summer Hackathon 2026" 
                required
              />
              <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.5rem' }}>
                Profiles (Low/Med/High volatility) will be automatically attached.
              </p>
            </div>
            <button type="submit" className="btn-primary" style={{ width: '100%' }}>Create Draft</button>
          </form>
        </div>

        {/* Manage Lifecycle */}
        <div className="glass-panel" style={{ padding: '2rem' }}>
          <h3 style={{ marginBottom: '1.5rem', color: 'var(--text-primary)' }}>Manage Lifecycle</h3>
          <div className="form-group">
            <label className="form-label" htmlFor="targetContestId">Target Contest ID</label>
            <input 
              className="input-field" 
              type="text" 
              id="targetContestId" 
              value={targetContestId}
              onChange={(e) => setTargetContestId(e.target.value)}
              placeholder="UUID" 
            />
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            <button onClick={handleActivateContest} className="btn-primary" style={{ background: 'var(--accent-success)' }} disabled={!targetContestId}>
              Activate
            </button>
            <button onClick={handleCloseContest} className="btn-primary" style={{ background: 'var(--accent-error)' }} disabled={!targetContestId}>
              Close & Freeze
            </button>
          </div>
        </div>
      </div>

      {/* Right Column: History */}
      <div className="glass-panel" style={{ padding: '2rem' }}>
        <div className="flex-between" style={{ marginBottom: '1.5rem' }}>
          <h3 style={{ color: 'var(--text-primary)' }}>Contest History</h3>
          <button onClick={fetchPastContests} className="btn-secondary" style={{ padding: '0.25rem 0.75rem', fontSize: '0.875rem' }}>
            {loadingHistory ? 'Loading...' : 'Refresh'}
          </button>
        </div>

        <div className="data-table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th>ID (Truncated)</th>
                <th>Name</th>
                <th>Status</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {pastContests.length === 0 ? (
                <tr>
                  <td colSpan={4} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '3rem 1rem' }}>
                    No past contests found.
                  </td>
                </tr>
              ) : (
                pastContests.map((c) => (
                  <tr key={c.id} className="table-row">
                    <td style={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
                      {c.id.substring(0, 8)}...
                      <button 
                        onClick={() => setTargetContestId(c.id)}
                        style={{ marginLeft: '0.5rem', background: 'transparent', border: '1px solid var(--border-light)', color: 'var(--text-secondary)', cursor: 'pointer', borderRadius: '4px', padding: '0 4px' }}
                        title="Select this ID"
                      >
                        Copy
                      </button>
                    </td>
                    <td style={{ fontWeight: 500, color: 'var(--text-primary)' }}>{c.name}</td>
                    <td>
                      <span className={`status-badge ${c.status}`}>{c.status}</span>
                    </td>
                    <td>{new Date(c.created_at).toLocaleDateString()}</td>
                    <td>
                      {c.status === 'closed' && (
                        <button 
                          onClick={() => fetchSnapshot(c)}
                          className="btn-secondary"
                          style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}
                        >
                          View Snapshot
                        </button>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
      {/* Snapshot Modal */}
      {viewingContest && snapshotEntries && (
        <div style={{ position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, background: 'rgba(0,0,0,0.6)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, padding: '2rem' }}>
          <div className="glass-panel" style={{ width: '100%', maxWidth: '800px', maxHeight: '80vh', overflowY: 'auto' }}>
            <div className="flex-between" style={{ marginBottom: '1.5rem' }}>
              <h3 style={{ color: 'var(--text-primary)' }}>Snapshot: {viewingContest.name}</h3>
              <button onClick={() => setViewingContest(null)} className="btn-secondary">Close</button>
            </div>
            <div className="data-table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Team</th>
                    <th>Final Score</th>
                    <th>Best P99 (ms)</th>
                    <th>Peak TPS</th>
                    <th>Avg Correctness</th>
                  </tr>
                </thead>
                <tbody>
                  {snapshotEntries.length === 0 ? (
                    <tr>
                      <td colSpan={6} style={{ textAlign: 'center', padding: '2rem' }}>No submissions found for this contest.</td>
                    </tr>
                  ) : (
                    snapshotEntries.map((entry, idx) => (
                      <tr key={idx} className="table-row">
                        <td><span className="rank-badge">{entry.rank}</span></td>
                        <td style={{ fontWeight: 500, color: 'var(--text-primary)' }}>{entry.team_name}</td>
                        <td style={{ fontWeight: 600, color: 'var(--accent-primary)' }}>{formatNumber(entry.final_score || 0)}</td>
                        <td>{(entry.best_p99_ms || 0).toFixed(2)}</td>
                        <td>{formatNumber(entry.peak_sustained_tps || 0)}</td>
                        <td>{((entry.avg_correctness || 0) * 100).toFixed(1)}%</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
