export interface LeaderboardEntry {
  rank: number;
  team_name: string;
  submission_id: string;
  final_score: number;
  low_score: number;
  medium_score: number;
  high_score: number;
  best_p99_ms: number;
  peak_sustained_tps: number;
  avg_correctness: number;
}

export interface Contest {
  id: string;
  name: string;
  status: 'draft' | 'active' | 'closed';
  created_at: string;
}

export interface BenchmarkResults {
  volatility_label: string;
  p50_latency_ms: number;
  p90_latency_ms: number;
  p99_latency_ms: number;
  max_tps: number;
  sustained_tps: number;
  correctness_score: number;
  total_orders: number;
  correct_fills: number;
  incorrect_fills: number;
  run_score: number;
  benchmark_duration: string;
  completed_at: string;
}

// DryRunScenarioResult is one scenario's outcome from the worker-side
// pre-flight validator. Mirrors models.DryRunScenarioResult on the backend.
export interface DryRunScenarioResult {
  name: string;
  passed: boolean;
  reason?: string; // enriched failure string; absent when passed
}

// DryRunResult is the stored output of the worker-side pre-flight validator.
// Non-null only on submissions processed by workers with the pre-flight gate
// enabled. Null for all pre-Phase-7 submissions (omitempty on backend).
export interface DryRunResult {
  all_passed: boolean;
  scenarios: DryRunScenarioResult[];
  ran_at: string;           // ISO 8601
  fail_summary?: string;    // e.g. "3/21 scenarios failed: [...]"; absent when all_passed
}

export interface Submission {
  id: string;
  team_name: string;
  language: string;
  protocol: string;
  status: string;
  status_message?: string;
  contest_id: string;
  all_results: BenchmarkResults[] | null;
  final_score: number;
  created_at: string;
  completed_at: string | null;
  // dry_run_result is populated by the worker-side pre-flight validator.
  // Null for submissions processed before this feature was deployed.
  dry_run_result: DryRunResult | null;
}

export interface ScenarioResult {
  name: string;
  passed: boolean;
  reason?: string;
}

export interface ValidationResult {
  submission_id: string;
  scenarios: ScenarioResult[];
  all_passed: boolean;
  tested_at: string;
}
