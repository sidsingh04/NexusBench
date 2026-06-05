import { useState } from 'react';
import { UploadForm } from '../components/UploadForm';
import { Leaderboard } from '../components/Leaderboard';
import { TeamHistory } from '../components/TeamHistory';

export function Dashboard() {
  const [teamName, setTeamName] = useState<string>('');
  const [teamInput, setTeamInput] = useState<string>('');
  const [activeContestId, setActiveContestId] = useState<string | null>(null);

  const handleTeamLookup = () => {
    const trimmed = teamInput.trim();
    if (trimmed) setTeamName(trimmed);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
      <div className="grid-layout">
        <aside>
          <UploadForm />

          {/* Team history lookup panel — sits naturally below submit form */}
          <div className="glass-panel" style={{ padding: '1.25rem', marginTop: '1.5rem' }}>
            <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '0.75rem', fontWeight: 500 }}>
              View my submission history
            </p>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <input
                className="input-field"
                type="text"
                placeholder="Enter team name…"
                value={teamInput}
                onChange={e => setTeamInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleTeamLookup()}
                style={{ flex: 1, padding: '0.5rem 0.75rem', fontSize: '0.875rem' }}
              />
              <button
                className="btn-secondary"
                style={{ whiteSpace: 'nowrap', padding: '0.5rem 0.875rem', fontSize: '0.875rem' }}
                onClick={handleTeamLookup}
              >
                Look up
              </button>
            </div>
            {teamName && (
              <button
                style={{ marginTop: '0.5rem', background: 'none', border: 'none', color: 'var(--text-muted)', fontSize: '0.75rem', cursor: 'pointer', padding: 0 }}
                onClick={() => { setTeamName(''); setTeamInput(''); }}
              >
                ✕ Clear
              </button>
            )}
          </div>
        </aside>

        <section>
          <Leaderboard onActiveContestId={setActiveContestId} />
        </section>
      </div>

      {/* Team history — full width below the grid */}
      {teamName && (
        <TeamHistory teamName={teamName} activeContestId={activeContestId} />
      )}
    </div>
  );
}
