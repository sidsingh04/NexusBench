import React, { useState, useEffect } from 'react';
import type { ChangeEvent, FormEvent } from 'react';
import { api, ApiError } from '../api/client';
import type { Submission, ValidationResult } from '../types';

const PIPELINE_STEPS = ['pending', 'benchmarking', 'completed'];

export function UploadForm() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [fileName, setFileName] = useState<string>('');
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);

  const [activeSubmission, setActiveSubmission] = useState<Submission | null>(null);
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);
  const [isValidating, setIsValidating] = useState(false);
  const [hasAttemptedValidation, setHasAttemptedValidation] = useState(false);

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFileName(e.target.files[0].name);
    } else {
      setFileName('');
    }
  };

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setActiveSubmission(null);
    setValidationResult(null);
    setHasAttemptedValidation(false);
    setUploadProgress(0);

    const form = e.currentTarget;
    const formData = new FormData(form);

    try {
      const response = await new Promise<Submission>((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/v1/submissions');
        xhr.setRequestHeader('Accept', 'application/json');

        xhr.upload.onprogress = (event) => {
          if (event.lengthComputable) {
            setUploadProgress(Math.round((event.loaded * 100) / event.total));
          }
        };

        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            try {
              resolve(JSON.parse(xhr.responseText));
            } catch {
              reject(new Error('Invalid JSON response'));
            }
          } else {
            try {
              const errData = JSON.parse(xhr.responseText);
              reject(new ApiError(xhr.status, errData.code || 'UPLOAD_FAILED', errData.message || 'Upload failed'));
            } catch {
              reject(new Error(`Upload failed with status ${xhr.status}`));
            }
          }
        };

        xhr.onerror = () => reject(new Error('Network error during upload'));
        xhr.send(formData);
      });

      setActiveSubmission(response);
      form.reset();
      setFileName('');
    } catch (err: any) {
      setError(err.message || 'An unexpected error occurred during upload.');
    } finally {
      setLoading(false);
      setUploadProgress(null);
    }
  };

  // Auto-validate once the submission is pending and deployed
  useEffect(() => {
    if (activeSubmission && activeSubmission.status === 'pending' && !validationResult && !isValidating && !hasAttemptedValidation) {
      setHasAttemptedValidation(true);

      const attemptValidation = async () => {
        setIsValidating(true);
        try {
          await new Promise(r => setTimeout(r, 5000));
          const currentData = await api.get<Submission>(`/submissions/${activeSubmission.id}`);
          if (currentData.status === 'pending') {
            const data = await api.post<ValidationResult>(`/submissions/${activeSubmission.id}/validate`);
            setValidationResult(data);
          }
        } catch (err) {
          console.error('Auto-validation failed:', err);
        } finally {
          setIsValidating(false);
        }
      };

      attemptValidation();
    }
  }, [activeSubmission, validationResult, isValidating, hasAttemptedValidation]);

  // Poll submission status until terminal
  useEffect(() => {
    if (!activeSubmission) return;
    if (activeSubmission.status === 'completed' || activeSubmission.status === 'failed') return;

    const interval = setInterval(async () => {
      try {
        const data = await api.get<Submission>(`/submissions/${activeSubmission.id}`);
        setActiveSubmission(data);
        if (data.status === 'completed' || data.status === 'failed') {
          clearInterval(interval);
        }
      } catch (err) {
        console.error('Failed to poll submission status:', err);
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [activeSubmission?.id, activeSubmission?.status]);

  const renderStepper = () => {
    if (!activeSubmission) return null;
    const currentStatus = activeSubmission.status;
    let activeIndex = 0;

    if (currentStatus === 'completed') activeIndex = 2;
    else if (currentStatus === 'benchmarking') activeIndex = 1;
    else if (currentStatus === 'pending') activeIndex = 0;
    else if (currentStatus === 'failed') activeIndex = -1;

    return (
      <div style={{ margin: '1.5rem 0' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
          {PIPELINE_STEPS.map((step, idx) => (
            <div key={step} style={{
              color: currentStatus === 'failed' ? (idx === 0 ? 'var(--accent-error)' : 'var(--text-muted)') :
                idx <= activeIndex ? 'var(--accent-primary)' : 'var(--text-muted)',
              fontSize: '0.75rem',
              fontWeight: idx === activeIndex ? 600 : 400,
              textTransform: 'capitalize',
            }}>
              {step}
            </div>
          ))}
        </div>
        <div style={{ height: '4px', background: 'var(--bg-secondary)', borderRadius: '2px', overflow: 'hidden', display: 'flex' }}>
          {PIPELINE_STEPS.map((step, idx) => {
            const isFailed = currentStatus === 'failed';
            const isActive = !isFailed && idx <= activeIndex;
            return (
              <div key={step} style={{
                flex: 1,
                background: isFailed ? (idx === 0 ? 'var(--accent-error)' : 'transparent') :
                  isActive ? 'var(--accent-primary)' : 'transparent',
                borderRight: idx < PIPELINE_STEPS.length - 1 ? '1px solid var(--bg-tertiary)' : 'none',
              }} />
            );
          })}
        </div>
        {currentStatus === 'failed' && (
          <div style={{ marginTop: '0.5rem', color: 'var(--accent-error)', fontSize: '0.875rem' }}>
            Submission failed during execution.
          </div>
        )}
      </div>
    );
  };

  const renderResultsCard = () => {
    if (!activeSubmission || activeSubmission.status !== 'completed' || !activeSubmission.all_results || activeSubmission.all_results.length === 0) return null;

    const profileColor: Record<string, string> = {
      low: 'var(--accent-success)',
      medium: 'var(--accent-warning)',
      high: 'var(--accent-secondary)',
    };
    const profileOrder = ['low', 'medium', 'high'];
    const sorted = [...activeSubmission.all_results].sort(
      (a, b) => profileOrder.indexOf(a.volatility_label) - profileOrder.indexOf(b.volatility_label)
    );

    const scoreBarStyle = (score: number, color: string): React.CSSProperties => ({
      height: '6px',
      borderRadius: '3px',
      background: `linear-gradient(90deg, ${color} ${Math.min(score * 100, 100).toFixed(1)}%, rgba(255,255,255,0.08) ${Math.min(score * 100, 100).toFixed(1)}%)`,
      marginTop: '0.375rem',
    });

    const metricRow = (label: string, value: string) => (
      <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0.3rem 0', borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
        <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>{label}</span>
        <span style={{ color: 'var(--text-primary)', fontSize: '0.8rem', fontWeight: 500 }}>{value}</span>
      </div>
    );

    return (
      <div style={{ marginTop: '1.75rem' }}>
        {/* Header row with final score */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <h4 style={{ color: 'var(--text-primary)', fontSize: '0.95rem', fontWeight: 600, letterSpacing: '-0.01em' }}>
            Benchmark Breakdown
          </h4>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>Final Score</span>
            <span style={{
              background: (activeSubmission.final_score || 0) >= 80 ? 'rgba(16,185,129,0.15)' : (activeSubmission.final_score || 0) >= 50 ? 'rgba(245,158,11,0.15)' : 'rgba(239,68,68,0.12)',
              color: (activeSubmission.final_score || 0) >= 80 ? 'var(--accent-success)' : (activeSubmission.final_score || 0) >= 50 ? 'var(--accent-warning)' : 'var(--accent-error)',
              border: `1px solid ${(activeSubmission.final_score || 0) >= 80 ? 'rgba(16,185,129,0.35)' : (activeSubmission.final_score || 0) >= 50 ? 'rgba(245,158,11,0.35)' : 'rgba(239,68,68,0.3)'}`,
              padding: '0.2rem 0.6rem', borderRadius: 'var(--radius-full)', fontWeight: 700, fontSize: '1rem',
            }}>
              {(activeSubmission.final_score || 0).toFixed(2)}
            </span>
          </div>
        </div>

        {/* Per-profile cards: Low / Medium / High */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.75rem' }}>
          {sorted.map(res => {
            const color = profileColor[res.volatility_label] ?? 'var(--accent-primary)';
            const label = res.volatility_label.charAt(0).toUpperCase() + res.volatility_label.slice(1);
            const tps = res.sustained_tps > 0 ? res.sustained_tps.toLocaleString() : '—';
            return (
              <div key={res.volatility_label} style={{
                background: 'rgba(0,0,0,0.25)',
                border: `1px solid ${color}30`,
                borderRadius: 'var(--radius-md)',
                padding: '0.875rem',
                position: 'relative',
                overflow: 'hidden',
              }}>
                {/* Colour accent stripe at top */}
                <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: '2px', background: color, borderRadius: 'var(--radius-md) var(--radius-md) 0 0' }} />

                {/* Profile name + run score */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.75rem' }}>
                  <span style={{ color, fontWeight: 700, fontSize: '0.8rem', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{label}</span>
                  <span style={{ color: 'var(--text-primary)', fontWeight: 700, fontSize: '1rem' }}>{(res.run_score || 0).toFixed(2)}</span>
                </div>

                {/* Score bar */}
                <div title={`Run score: ${(res.run_score || 0).toFixed(2)}`} style={scoreBarStyle(Math.min((res.run_score || 0) / 100, 1), color)} />

                {/* Metrics */}
                <div style={{ marginTop: '0.75rem' }}>
                  {metricRow('P50 Latency', res.p50_latency_ms > 0 ? `${res.p50_latency_ms.toFixed(2)} ms` : '—')}
                  {metricRow('P90 Latency', res.p90_latency_ms > 0 ? `${res.p90_latency_ms.toFixed(2)} ms` : '—')}
                  {metricRow('P99 Latency', res.p99_latency_ms > 0 ? `${res.p99_latency_ms.toFixed(2)} ms` : '—')}
                  {metricRow('Sustained TPS', tps)}
                  {metricRow('Correctness', `${(res.correctness_score * 100).toFixed(1)}%`)}
                </div>
              </div>
            );
          })}
        </div>

        <div style={{ marginTop: '0.75rem', fontSize: '0.75rem', color: 'var(--text-muted)', textAlign: 'center' }}>
          Low · Medium · High volatility profiles · Leaderboard updates within 5 s
        </div>
      </div>
    );
  };

  return (
    <div className="glass-panel" style={{ padding: '2rem' }}>
      <h3 style={{ marginBottom: '1.5rem', color: 'var(--text-primary)' }}>Submit Engine</h3>

      {error && (
        <div style={{ padding: '1rem', marginBottom: '1rem', borderRadius: 'var(--radius-md)', background: 'rgba(239, 68, 68, 0.1)', border: '1px solid var(--accent-error)', color: '#FCA5A5' }}>
          {error}
        </div>
      )}

      {activeSubmission && (
        <div style={{ marginBottom: '2rem', padding: '1.5rem', borderRadius: 'var(--radius-md)', border: '1px solid var(--bg-secondary)', background: 'rgba(255, 255, 255, 0.02)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div>
              <div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: '0.25rem' }}>Submission Tracker</div>
              <div style={{ fontSize: '0.875rem', color: 'var(--text-secondary)' }}>ID: {activeSubmission.id}</div>
              <div style={{ fontSize: '0.875rem', color: 'var(--text-secondary)' }}>Team: {activeSubmission.team_name}</div>
            </div>
            {isValidating && (
              <div style={{ fontSize: '0.875rem', color: 'var(--accent-primary)', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span style={{ display: 'inline-block', width: '12px', height: '12px', border: '2px solid', borderTopColor: 'transparent', borderRadius: '50%', animation: 'spin 1s linear infinite' }}></span>
                Auto-validating...
              </div>
            )}
          </div>

          {renderStepper()}

          {validationResult && (
            <div style={{ marginTop: '1.5rem', padding: '1rem', borderRadius: 'var(--radius-md)', background: validationResult.all_passed ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)', border: `1px solid ${validationResult.all_passed ? 'var(--accent-success)' : 'var(--accent-error)'}` }}>
              <h5 style={{ color: validationResult.all_passed ? 'var(--accent-success)' : 'var(--accent-error)', marginBottom: '0.75rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                {validationResult.all_passed ? '✓ Validation Passed' : '✗ Validation Failed'}
                <span style={{ fontSize: '0.875rem', fontWeight: 'normal', color: 'var(--text-secondary)' }}>
                  ({validationResult.scenarios.filter(s => s.passed).length} / {validationResult.scenarios.length} passed)
                </span>
              </h5>
              <div style={{ maxHeight: '200px', overflowY: 'auto', fontSize: '0.875rem', color: 'var(--text-secondary)' }}>
                {validationResult.scenarios.map((sc, i) => (
                  <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '0.5rem 0', borderBottom: i < validationResult.scenarios.length - 1 ? '1px solid rgba(255,255,255,0.05)' : 'none' }}>
                    <span>{sc.name}</span>
                    <span style={{ color: sc.passed ? 'var(--accent-success)' : 'var(--accent-error)', textAlign: 'right' }}>
                      {sc.passed ? 'Passed' : sc.reason || 'Failed'}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {renderResultsCard()}
        </div>
      )}

      {uploadProgress !== null && (
        <div style={{ marginBottom: '1.5rem', padding: '1rem', borderRadius: 'var(--radius-md)', background: 'rgba(255, 255, 255, 0.02)', border: '1px solid var(--bg-secondary)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.875rem', marginBottom: '0.5rem', color: 'var(--text-primary)' }}>
            <span>Uploading Archive...</span>
            <span style={{ fontWeight: 600 }}>{uploadProgress}%</span>
          </div>
          <div style={{ height: '8px', background: 'var(--bg-tertiary)', borderRadius: '4px', overflow: 'hidden' }}>
            <div style={{ height: '100%', background: 'var(--accent-primary)', width: `${uploadProgress}%`, transition: 'width 0.2s ease' }} />
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit} style={{ opacity: loading ? 0.7 : 1, pointerEvents: loading ? 'none' : 'auto' }}>
        <div className="form-group">
          <label className="form-label" htmlFor="team_name">Team Name</label>
          <input
            className="input-field"
            type="text"
            id="team_name"
            name="team_name"
            required
            placeholder="e.g., Alpha Quant"
          />
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="language">Language</label>
          <select className="input-field" id="language" name="language" required>
            <option value="go">Go</option>
            <option value="rust">Rust</option>
            <option value="cpp">C++</option>
            <option value="python">Python</option>
            <option value="binary">Compiled Binary</option>
          </select>
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="protocol">Protocol</label>
          <select className="input-field" id="protocol" name="protocol" required>
            <option value="rest">REST</option>
            <option value="websocket">WebSocket</option>
          </select>
        </div>

        <div className="form-group">
          <label className="form-label">Engine Archive (.tar.gz)</label>
          <div className="file-input-wrapper">
            <button type="button" className="file-input-button">
              <svg width="24" height="24" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path>
              </svg>
              <span>{fileName ? fileName : 'Click or drag file to upload'}</span>
            </button>
            <input type="file" name="archive" accept=".tar.gz,.tgz" required onChange={handleFileChange} />
          </div>
        </div>

        <button
          type="submit"
          className="btn-primary"
          style={{ width: '100%' }}
          disabled={loading || activeSubmission?.status === 'pending' || activeSubmission?.status === 'benchmarking'}
        >
          {loading ? 'Uploading...' : 'Upload & Benchmark'}
        </button>
      </form>
    </div>
  );
}
