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

export interface Submission {
  id: string;
  team_name: string;
  language: string;
  protocol: string;
  status: string;
  contest_id: string;
  all_results: BenchmarkResults[] | null;
  final_score: number;
  created_at: string;
  completed_at: string | null;
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
