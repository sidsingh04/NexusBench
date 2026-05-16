## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- ALWAYS read graphify-out/GRAPH_REPORT.md before reading any source files, running grep/glob searches, or answering codebase questions. The graph is your primary map of the codebase.
- IF graphify-out/wiki/index.md EXISTS, navigate it instead of reading raw files
- For cross-module "how does X relate to Y" questions, prefer `graphify query "<question>"`, `graphify path "<A>" "<B>"`, or `graphify explain "<concept>"` over grep — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

# Claude Engineering Guidelines

## Thinking Rules
- Think step-by-step before coding
- Surface assumptions explicitly
- Explain tradeoffs before major changes
- Clarify ambiguity instead of guessing

## Simplicity Rules
- Prefer simple implementations
- Avoid premature optimization
- Avoid speculative abstractions
- Do not overengineer

## Code Modification Rules
- Make surgical changes only
- Do not refactor unrelated code
- Preserve existing architecture unless requested
- Minimize file modifications

## Reliability Rules
- Prefer correctness over cleverness
- Add validation where useful
- Consider edge cases before implementation
- Avoid hidden concurrency risks

## Benchmarking Rules
- Define measurable goals before optimization
- Prefer benchmark-driven optimization
- Measure before changing performance-critical paths

## Distributed Systems Rules
- Think about latency impact
- Think about failure modes
- Think about backpressure and retries
- Consider concurrency safety
- Avoid blocking hot paths

## Communication Rules
- Explain reasoning before major implementation
- Mention risks and tradeoffs
- Keep explanations concise and technical