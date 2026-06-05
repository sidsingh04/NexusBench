import { useEffect, useState, useRef } from 'react';
import { api } from '../api/client';
import type { LeaderboardEntry } from '../types';

interface LeaderboardProps {
  onActiveContestId?: (id: string | null) => void;
}

export function Leaderboard({ onActiveContestId }: LeaderboardProps = {}) {
  const [entries, setEntries] = useState<LeaderboardEntry[]>([]);
  // 'loading' only during the very first REST seed fetch before SSE connects.
  // After that, status reflects the SSE connection state. The table is ALWAYS
  // rendered — no full-component blanking on connect/reconnect.
  const [status, setStatus] = useState<'loading' | 'live' | 'frozen' | 'error'>('loading');
  const [contestId, setContestId] = useState<string | null>(null);
  const [contestName, setContestName] = useState<string | null>(null);

  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const isComponentMounted = useRef<boolean>(true);
  // Ref (not state) so onerror reads the current value synchronously in the
  // same tick that onmessage set it — avoids the stale-closure reconnect loop.
  const isFrozenRef = useRef<boolean>(false);

  useEffect(() => {
    isComponentMounted.current = true;
    isFrozenRef.current = false;
    let reconnectDelay = 1000;

    // ── Seed from REST on mount ───────────────────────────────────────────────
    // Fetch the current leaderboard immediately so the table shows real data
    // while the SSE connection is establishing. This eliminates the "No
    // submissions yet" flash that appeared on every page load or navigation.
    // SSE updates will overwrite this seed once they arrive.
    const seedFromREST = async () => {
      try {
        const data = await api.get<LeaderboardEntry[]>('/leaderboard');
        if (isComponentMounted.current && data && data.length > 0) {
          setEntries(data);
        }
      } catch {
        // Silently ignore — SSE will populate the table when it connects.
      }
    };
    seedFromREST();

    // ── SSE connection ────────────────────────────────────────────────────────
    const connect = () => {
      if (!isComponentMounted.current) return;
      if (isFrozenRef.current) return;

      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      // Do NOT reset entries here — keep the REST seed (or last known entries)
      // visible while the new connection is establishing.
      const eventSource = api.getEventSource('/leaderboard/stream');
      eventSourceRef.current = eventSource;

      eventSource.onopen = () => {
        if (!isComponentMounted.current) return;
        setStatus('live');
        reconnectDelay = 1000;
      };

      eventSource.onmessage = (event) => {
        if (!isComponentMounted.current) return;
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'update' || data.type === 'frozen') {
            if (data.contest_id) {
              setContestId(data.contest_id);
              onActiveContestId?.(data.contest_id);
            }
            if (data.contest_name) setContestName(data.contest_name);

            // Only overwrite entries when the incoming list is non-empty.
            // An empty list from an "update" event means no submissions yet —
            // only apply it if we also have no entries (avoids blanking a
            // populated table with an empty update).
            if (data.entries && data.entries.length > 0) {
              setEntries(data.entries);
            }

            if (data.type === 'frozen') {
              isFrozenRef.current = true;
              setStatus('frozen');
              onActiveContestId?.(null);
              eventSource.close();
            }
          }
        } catch (err) {
          console.error('Failed to parse SSE message', err);
        }
      };

      eventSource.onerror = () => {
        if (!isComponentMounted.current) return;
        eventSource.close();
        // Server intentionally closes the connection after sending "frozen".
        // isFrozenRef is set synchronously in onmessage so it is already true
        // here even though React state hasn't committed yet.
        if (isFrozenRef.current) return;

        if (reconnectTimeoutRef.current) {
          window.clearTimeout(reconnectTimeoutRef.current);
        }
        reconnectTimeoutRef.current = window.setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
        setStatus('error');
      };
    };

    connect();

    return () => {
      isComponentMounted.current = false;
      if (reconnectTimeoutRef.current) {
        window.clearTimeout(reconnectTimeoutRef.current);
      }
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, []);

  const formatScore = (num?: number) =>
    !num ? '0.00' : num.toFixed(2);

  const formatTPS = (num?: number) =>
    !num ? '—' : new Intl.NumberFormat().format(Math.round(num));

  const formatP99 = (num?: number) =>
    !num ? '—' : num.toFixed(2);

  // Status indicator — shown inline in the header, never blanks the table.
  const statusBadge = () => {
    switch (status) {
      case 'loading':
        return (
          <span className="status-badge pending" style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem' }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', border: '2px solid currentColor', borderTopColor: 'transparent', display: 'inline-block', animation: 'spin 0.8s linear infinite' }} />
            Loading
          </span>
        );
      case 'live':
        return <span className="status-badge active">Live</span>;
      case 'frozen':
        return <span className="status-badge closed">Closed</span>;
      case 'error':
        return (
          <span className="status-badge failed" style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem' }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', border: '2px solid currentColor', borderTopColor: 'transparent', display: 'inline-block', animation: 'spin 0.8s linear infinite' }} />
            Reconnecting
          </span>
        );
    }
  };

  return (
    <div className="glass-panel" style={{ padding: '2rem' }}>
      <div className="flex-between" style={{ marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            {contestName || 'Live Leaderboard'}
            {statusBadge()}
          </h2>
          {contestId && (
            <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', marginTop: '0.25rem' }}>
              Contest ID: {contestId}
            </p>
          )}
        </div>
      </div>

      {/* Table is ALWAYS rendered. Connection state is shown in the header only. */}
      <div className="data-table-container">
        <table className="data-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Team</th>
              <th>Final Score</th>
              <th>Best P99 (ms)</th>
              <th>Peak TPS</th>
              <th>Correctness</th>
            </tr>
          </thead>
          <tbody>
            {entries.length === 0 ? (
              <tr>
                <td colSpan={6} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '3rem 1rem' }}>
                  {status === 'loading' ? 'Loading leaderboard…' : 'No submissions yet. Be the first to submit!'}
                </td>
              </tr>
            ) : (
              entries.map((entry) => (
                <tr key={entry.team_name} className="table-row">
                  <td>
                    <span className={`rank-badge ${entry.rank <= 3 ? 'rank-' + entry.rank : ''}`}>
                      {entry.rank}
                    </span>
                  </td>
                  <td style={{ fontWeight: 500, color: 'var(--text-primary)' }}>{entry.team_name}</td>
                  <td style={{ fontWeight: 600, color: 'var(--accent-primary)' }}>{formatScore(entry.final_score)}</td>
                  <td>{formatP99(entry.best_p99_ms)}</td>
                  <td>{formatTPS(entry.peak_sustained_tps)}</td>
                  <td>{((entry.avg_correctness || 0) * 100).toFixed(1)}%</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
