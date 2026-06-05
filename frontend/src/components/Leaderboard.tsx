import { useEffect, useState, useRef } from 'react';
import { api } from '../api/client';
import type { LeaderboardEntry } from '../types';

interface LeaderboardProps {
  /** Called whenever the active contest ID is known from the SSE stream. */
  onActiveContestId?: (id: string | null) => void;
}

export function Leaderboard({ onActiveContestId }: LeaderboardProps = {}) {
  const [entries, setEntries] = useState<LeaderboardEntry[]>([]);
  const [status, setStatus] = useState<'connecting' | 'live' | 'frozen' | 'error'>('connecting');
  const [contestId, setContestId] = useState<string | null>(null);
  const [contestName, setContestName] = useState<string | null>(null);

  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const isComponentMounted = useRef<boolean>(true);
  // Ref (not state) so onerror reads the current value synchronously in the
  // same tick that onmessage set it — avoids the stale-closure reconnect loop
  // that blanked the leaderboard when the server closed the connection after
  // sending the "frozen" event.
  const isFrozenRef = useRef<boolean>(false);

  useEffect(() => {
    isComponentMounted.current = true;
    isFrozenRef.current = false;
    let reconnectDelay = 1000;

    const connect = () => {
      if (!isComponentMounted.current) return;
      if (isFrozenRef.current) return; // never reconnect after contest closes

      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      setStatus('connecting');
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

            // Only replace entries when incoming list is non-empty, or when
            // we have no entries yet. Prevents a late empty-list event from
            // blanking a populated table.
            if (data.entries && data.entries.length > 0) {
              setEntries(data.entries);
            } else if (data.type === 'update') {
              setEntries(prev => prev.length === 0 ? [] : prev);
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

        // Server closes the connection intentionally after "frozen" — do not
        // reconnect. isFrozenRef is set synchronously inside onmessage so it
        // is already true here even though React state hasn't committed yet.
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

  return (
    <div className="glass-panel" style={{ padding: '2rem' }}>
      <div className="flex-between" style={{ marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            {contestName || 'Live Leaderboard'}
            {status === 'live' && <span className="status-badge active">Live</span>}
            {status === 'frozen' && <span className="status-badge closed">Closed</span>}
            {status === 'connecting' && <span className="status-badge pending">Connecting...</span>}
            {status === 'error' && <span className="status-badge failed">Reconnecting...</span>}
          </h2>
          {contestId && (
            <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', marginTop: '0.25rem' }}>
              Contest ID: {contestId}
            </p>
          )}
        </div>
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
              <th>Correctness</th>
            </tr>
          </thead>
          <tbody>
            {entries.length === 0 ? (
              <tr>
                <td colSpan={6} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '3rem 1rem' }}>
                  No submissions yet. Be the first to submit!
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
