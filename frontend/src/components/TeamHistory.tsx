import { useState, useEffect, useRef } from 'react';
import { api } from '../api/client';
import type { Submission, DryRunResult } from '../types';

interface TeamHistoryProps {
  teamName: string | null;
  activeContestId: string | null;
}

const IN_PROGRESS = new Set(['pending', 'building', 'deploying', 'running', 'benchmarking']);

function statusBadge(status: string) {
  let cls = 'status-badge';
  if (status === 'completed') cls += ' completed';
  else if (status === 'failed') cls += ' failed';
  else if (IN_PROGRESS.has(status)) cls += ' benchmarking';
  else cls += ' pending';
  return <span className={cls}>{status}</span>;
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function scoreCell(score: number | undefined | null, status: string) {
  if (status !== 'completed' || typeof score !== 'number') return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  const color = score >= 80 ? 'var(--accent-success)' : score >= 50 ? 'var(--accent-warning)' : 'var(--accent-error)';
  return <span style={{ color, fontWeight: 700 }}>{score.toFixed(2)}</span>;
}

export function TeamHistory({ teamName, activeContestId }: TeamHistoryProps) {
  const [submissions, setSubmissions] = useState<Submission[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [contestLabel, setContestLabel] = useState<string>('');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!teamName) {
      setSubmissions([]);
      setError(null);
      return;
    }

    let cancelled = false;

    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await api.get<{ submissions: Submission[] }>(
          `/teams/${encodeURIComponent(teamName)}/submissions`
        );
        if (cancelled) return;

        const all: Submission[] = data.submissions ?? [];

        // Decide which contest to show: live contest first, else most recent
        let targetId = activeContestId;
        if (!targetId && all.length > 0) {
          const sorted = [...all].sort(
            (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
          );
          targetId = sorted[0].contest_id ?? null;
        }

        const filtered = targetId ? all.filter(s => s.contest_id === targetId) : all;
        filtered.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

        setSubmissions(filtered);
        setContestLabel(activeContestId ? 'Live Contest' : targetId ? 'Last Contest' : '');
      } catch (err: any) {
        if (cancelled) return;
        if (err?.status === 404) {
          setSubmissions([]);
        } else {
          setError(err?.message ?? 'Failed to load submissions');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    load();
    pollRef.current = setInterval(load, 5000);

    return () => {
      cancelled = true;
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [teamName, activeContestId]);

  if (!teamName) return null;

  return (
    <div className="glass-panel" style={{ padding: '1.5rem', marginTop: '2rem' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem' }}>
        <div>
          <h3 style={{ color: 'var(--text-primary)', fontSize: '1rem', fontWeight: 600, marginBottom: '0.2rem' }}>
            My Submissions
          </h3>
          <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>
            Team: <span style={{ color: 'var(--accent-primary)', fontWeight: 600 }}>{teamName}</span>
            {contestLabel && (
              <span style={{
                marginLeft: '0.5rem',
                padding: '0.1rem 0.4rem',
                borderRadius: 'var(--radius-full)',
                background: activeContestId ? 'rgba(16,185,129,0.12)' : 'rgba(100,116,139,0.12)',
                border: `1px solid ${activeContestId ? 'rgba(16,185,129,0.3)' : 'rgba(100,116,139,0.3)'}`,
                color: activeContestId ? 'var(--accent-success)' : 'var(--text-muted)',
                fontSize: '0.72rem', fontWeight: 600,
              }}>
                {contestLabel}
              </span>
            )}
          </span>
        </div>
        {loading && (
          <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <span style={{
              width: '10px', height: '10px',
              border: '2px solid var(--accent-primary)', borderTopColor: 'transparent',
              borderRadius: '50%', display: 'inline-block', animation: 'spin 0.8s linear infinite',
            }} />
            Refreshing
          </span>
        )}
      </div>

      {error && (
        <div style={{ padding: '0.75rem 1rem', borderRadius: 'var(--radius-md)', background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.25)', color: '#FCA5A5', fontSize: '0.875rem', marginBottom: '1rem' }}>
          {error}
        </div>
      )}

      {!loading && submissions.length === 0 && !error && (
        <div style={{ textAlign: 'center', padding: '2rem 0', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
          No submissions found for this team.
        </div>
      )}

      {submissions.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table" style={{ minWidth: '520px' }}>
            <thead>
              <tr>
                <th>#</th>
                <th>Status</th>
                <th>Score</th>
                <th>Language</th>
                <th>Submitted</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {submissions.map((sub, idx) => {
                const isExpanded = expandedId === sub.id;
                // Expand arrow appears when there are benchmark results OR a dry-run result.
                const hasResults =
                  (sub.all_results && sub.all_results.length > 0) ||
                  sub.dry_run_result !== null;
                return (
                  <>
                    <tr
                      key={sub.id}
                      className="table-row"
                      style={{ cursor: hasResults ? 'pointer' : 'default' }}
                      onClick={() => hasResults && setExpandedId(isExpanded ? null : sub.id)}
                    >
                      <td style={{ color: 'var(--text-muted)', fontSize: '0.8rem', fontFamily: 'monospace' }}>
                        {String(idx + 1).padStart(2, '0')}
                      </td>
                      <td>{statusBadge(sub.status)}</td>
                      <td>{scoreCell(sub.final_score, sub.status)}</td>
                      <td style={{ color: 'var(--text-secondary)', fontSize: '0.875rem', textTransform: 'capitalize' }}>{sub.language}</td>
                      <td style={{ color: 'var(--text-muted)', fontSize: '0.8rem', whiteSpace: 'nowrap' }}>{formatDate(sub.created_at)}</td>
                      <td style={{ textAlign: 'right' }}>
                        {hasResults && (
                          <span style={{ color: 'var(--text-muted)', fontSize: '0.75rem', userSelect: 'none' }}>
                            {isExpanded ? '▲' : '▼'}
                          </span>
                        )}
                      </td>
                    </tr>
                    {isExpanded && hasResults && (
                      <tr key={`${sub.id}-detail`} style={{ background: 'rgba(0,0,0,0.18)' }}>
                        <td colSpan={6} style={{ padding: '0.75rem 1rem 1rem 1rem' }}>
                          {sub.all_results && sub.all_results.length > 0
                            ? <ProfileBreakdown results={sub.all_results} />
                            : sub.dry_run_result
                            ? <DryRunBreakdown result={sub.dry_run_result} />
                            : null}
                        </td>
                      </tr>
                    )}
                  </>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Inline per-profile mini breakdown ────────────────────────────────────────

interface ProfileBreakdownProps {
  results: NonNullable<Submission['all_results']>;
}

function ProfileBreakdown({ results }: ProfileBreakdownProps) {
  const order = ['low', 'medium', 'high'];
  const sorted = [...results].sort((a, b) => order.indexOf(a.volatility_label) - order.indexOf(b.volatility_label));
  const profileColor: Record<string, string> = {
    low: 'var(--accent-success)',
    medium: 'var(--accent-warning)',
    high: 'var(--accent-secondary)',
  };

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.625rem' }}>
      {sorted.map(r => {
        const color = profileColor[r.volatility_label] ?? 'var(--accent-primary)';
        const label = r.volatility_label.charAt(0).toUpperCase() + r.volatility_label.slice(1);
        return (
          <div key={r.volatility_label} style={{
            background: 'rgba(0,0,0,0.2)',
            border: `1px solid ${color}28`,
            borderRadius: 'var(--radius-md)',
            padding: '0.625rem 0.75rem',
            position: 'relative',
            overflow: 'hidden',
          }}>
            <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: '2px', background: color }} />
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
              <span style={{ color, fontWeight: 700, fontSize: '0.72rem', textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
              <span style={{ color: 'var(--text-primary)', fontWeight: 700, fontSize: '0.9rem' }}>{(r.run_score || 0).toFixed(2)}</span>
            </div>
            {([
              ['P99', (r.p99_latency_ms || 0) > 0 ? `${(r.p99_latency_ms || 0).toFixed(2)} ms` : '—'],
              ['TPS', (r.sustained_tps || 0) > 0 ? (r.sustained_tps || 0).toLocaleString() : '—'],
              ['Correct', `${((r.correctness_score || 0) * 100).toFixed(1)}%`],
            ] as [string, string][]).map(([k, v]) => (
              <div key={k} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.775rem', padding: '0.2rem 0', borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                <span style={{ color: 'var(--text-muted)' }}>{k}</span>
                <span style={{ color: 'var(--text-primary)' }}>{v}</span>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

// ── DryRunBreakdown: compact pre-flight result for team history ───────────────
// Shows failed scenarios first, then passed. When all_passed, a single header
// line is sufficient — no need to list 21 green checkmarks.

interface DryRunBreakdownProps {
  result: DryRunResult;
}

function DryRunBreakdown({ result }: DryRunBreakdownProps) {
  if (result.all_passed) {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.5rem',
        padding: '0.5rem 0.25rem',
        color: 'var(--accent-success)',
        fontSize: '0.82rem',
        fontWeight: 600,
      }}>
        <span>✓</span>
        <span>Pre-flight Validation — {result.scenarios.length}/{result.scenarios.length} passed</span>
      </div>
    );
  }

  const passedCount = result.scenarios.filter(s => s.passed).length;
  const totalCount = result.scenarios.length;

  // Show failed scenarios first, then passed, sorted by name within each group.
  const failed = result.scenarios.filter(s => !s.passed).sort((a, b) => a.name.localeCompare(b.name));
  const passed = result.scenarios.filter(s => s.passed).sort((a, b) => a.name.localeCompare(b.name));
  const ordered = [...failed, ...passed];

  return (
    <div>
      {/* One-line header */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '0.5rem',
        paddingBottom: '0.4rem',
        borderBottom: '1px solid rgba(255,255,255,0.07)',
      }}>
        <span style={{ color: 'var(--accent-error)', fontSize: '0.82rem', fontWeight: 600 }}>
          ✗ Pre-flight Validation
        </span>
        <span style={{ color: 'var(--accent-error)', fontSize: '0.78rem' }}>
          {totalCount - passedCount}/{totalCount} failed
        </span>
      </div>

      {/* Summary line from backend */}
      {result.fail_summary && (
        <div style={{
          fontSize: '0.75rem',
          color: 'var(--text-muted)',
          fontFamily: 'monospace',
          marginBottom: '0.5rem',
          wordBreak: 'break-word',
        }}>
          {result.fail_summary}
        </div>
      )}

      {/* Scenario list: failed first, then passed */}
      <div style={{ maxHeight: '220px', overflowY: 'auto' }}>
        {ordered.map((sc, i) => (
          <div key={i} style={{
            padding: '0.3rem 0',
            borderBottom: i < ordered.length - 1 ? '1px solid rgba(255,255,255,0.04)' : 'none',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
              <span style={{ color: sc.passed ? 'var(--accent-success)' : 'var(--accent-error)', fontSize: '0.78rem', flexShrink: 0 }}>
                {sc.passed ? '✓' : '✗'}
              </span>
              <span style={{
                color: sc.passed ? 'var(--text-muted)' : 'var(--text-secondary)',
                fontSize: '0.78rem',
                fontWeight: sc.passed ? 400 : 500,
              }}>
                {sc.name}
              </span>
            </div>
            {!sc.passed && sc.reason && (
              <pre style={{
                marginTop: '0.25rem',
                marginLeft: '1.1rem',
                fontSize: '0.68rem',
                color: 'rgba(252,165,165,0.85)',
                background: 'rgba(0,0,0,0.12)',
                borderRadius: 'var(--radius-sm)',
                padding: '0.3rem 0.5rem',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
                border: '1px solid rgba(239,68,68,0.12)',
              }}>
                {sc.reason}
              </pre>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
