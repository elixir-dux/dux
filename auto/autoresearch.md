# Dux Autoresearch: Performance Optimization

## Objective

Optimize the Dux dataframe library's per-query performance so it approaches Explorer/Polars speed on the three core operations: **filter**, **mutate**, and **summarise** against pre-loaded (eager) data.

Dux compiles Elixir dataframe verbs to SQL CTEs and executes on DuckDB via ADBC. Explorer wraps Polars, a Rust columnar engine. Both operate on in-memory columnar data. The gap is not DuckDB vs Polars compute — it's the overhead Dux adds between "user calls a verb" and "DuckDB returns results."

## How to run

### Quick benchmark (the inner loop)

```bash
mix run auto/bench_quick.exs
```

Outputs `METRIC` lines. The key metric is `combined_ratio` — the geometric mean of the filter/mutate/summarise ratios (Dux time / Explorer time). **Lower is better. 1.0 = parity with Explorer.**

### Full gate (compile + test + benchmark, best of 3)

```bash
bash auto/autoresearch.sh
```

Run this to validate that tests still pass AND get reliable benchmark numbers.

### Full Benchee benchmark (for detailed comparison)

```bash
mix run bench/dux_v_explorer.exs
```

Use this for final validation on 10M rows with proper statistical analysis.

## Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| `combined_ratio` | Geometric mean of filter/mutate/summarise ratios | < 2.0 (stretch: < 1.5) |
| `filter_ratio` | Dux filter time / Explorer filter time | < 2.0 |
| `mutate_ratio` | Dux mutate time / Explorer mutate time | < 2.0 |
| `summarise_ratio` | Dux summarise time / Explorer summarise time | Already ~1.0 |

## The experiment loop

1. Read this document and `auto/autoresearch.ideas.md`.
2. Pick a hypothesis from the ideas doc, or form a new one from profiling.
3. Implement the change — edit files under `lib/`.
4. Run `bash auto/autoresearch.sh`. If tests fail, fix or revert.
5. If `combined_ratio` improved → commit with a descriptive message, update progress log below.
6. If `combined_ratio` regressed → revert (`git checkout -- lib/`).
7. Update `auto/autoresearch.ideas.md` with what you learned.
8. Repeat. **NEVER STOP.**

## Files in scope (you may edit these)

**Priority: Dux-only first.** Exhaust all `lib/` optimizations before touching ADBC.

- `lib/dux.ex` — Main API, verb implementations
- `lib/dux/query.ex` — Expression macros
- `lib/dux/query/compiler.ex` — AST → SQL compiler
- `lib/dux/query_builder.ex` — Ops list → CTE SQL builder
- `lib/dux/backend.ex` — ADBC wrapper, query execution, result handling
- `lib/dux/connection.ex` — Connection pooling, persistent_term dispatch
- `lib/dux/sql/helpers.ex` — SQL string helpers
- `lib/dux/table_ref.ex` — TableRef struct and GC sentinel
- Any other file under `lib/` that's relevant to query execution performance

**Last resort** (only after Dux-side ideas exhausted or diminishing returns):
- `../adbc/lib/` — ADBC Elixir wrapper (local clone, editable)

## Off limits (do NOT modify)

- `test/` — All test files. Tests are the correctness gate.
- `auto/bench_quick.exs` — The benchmark. Don't optimize the benchmark itself.
- `auto/autoresearch.sh` — The gate script.
- `bench/dux_v_explorer.exs` — The full Benchee benchmark.
- `mix.exs` — Don't change dependencies.

## Constraints

- All tests must pass: `mix test --exclude distributed --exclude container`
- Code must compile without warnings: `mix compile --warnings-as-errors`
- No new dependencies
- No changes to the public API (function signatures, return types)
- Preserve all existing behavior — same outputs for same inputs
- Don't skip or weaken tests

## Architecture context

### How a Dux query executes (the hot path)

```
User calls: Dux.filter(dux, quantity > 50) |> Dux.compute()

1. Dux.filter/2 (macro) → appends {:filter, compiled_expr} to dux.ops
2. Dux.compute/1 → calls QueryBuilder.build/2
3. QueryBuilder.build/2 → walks ops, emits CTE SQL string
4. Backend.query/2 → dispatches SQL to ADBC connection
   a. Creates temp table name
   b. Executes: CREATE TEMP TABLE __tmp AS (the CTE SQL)
   c. Registers GC sentinel for auto-cleanup
   d. Returns %Dux{source: {:table, %TableRef{}}}
5. TableRef holds reference — GC eventually drops the temp table
```

### Where time is likely spent

- **SQL string generation** (step 3) — string concatenation per CTE
- **Temp table creation** (step 4b) — DuckDB must materialize the full result into a temp table even though the user may only need to chain more ops
- **ADBC round-trip** (step 4) — connection dispatch, parameter binding, result handling
- **GC sentinel setup** (step 4c) — NIF call to register cleanup

### Key insight: Explorer doesn't materialize intermediate results

Explorer's `DataFrame.filter/2` returns a lazy Polars LazyFrame that chains operations. Only `collect()` materializes. But in the benchmark, Explorer's filter/mutate return eagerly (Polars executes immediately in Rust, returning a new DataFrame in-process).

Dux's `compute/1` creates a **temporary table** in DuckDB for every materialization. For filter (returning ~4M rows) and mutate (returning 10M rows), this means DuckDB writes millions of rows to a temp table that nobody reads. This is likely the dominant cost.

### Potential optimization directions

See `auto/autoresearch.ideas.md` for the full list. The big ones:

1. **Avoid temp table creation** — Can `compute/1` return results via Arrow IPC without creating a temp table? Or use a DuckDB view instead?
2. **Use DuckDB views** — `CREATE TEMP VIEW` instead of `CREATE TEMP TABLE` for intermediate results that will be chained
3. **Reduce SQL generation overhead** — Cache or optimize CTE construction
4. **Direct ADBC query without temp table** — For terminal operations, just run the SELECT and return results directly
5. **Connection/dispatch overhead** — Is `persistent_term` lookup + ADBC query the tightest path possible?

## Baseline (2026-03-26, main branch, Apple M2 Pro 12-core, 16GB)

Quick bench (1M rows, best of 3 runs):
```
METRIC filter_ratio=22.83
METRIC mutate_ratio=12.14
METRIC summarise_ratio=1.25
METRIC combined_ratio=7.02
```

Full Benchee bench (10M rows, pre-optimization):
```
filter:    3.38x slower (12.6 ips vs 42.4 ips)
mutate:    9.03x slower (7.1 ips vs 64.2 ips)
summarise: 1.06x slower (46.4 ips vs 49.1 ips)
```

Quick bench validation at 10M rows (post-optimization):
```
filter:    0.01x (100x faster than Explorer)
mutate:    0.02x (50x faster than Explorer)
summarise: 0.04x (25x faster than Explorer)
combined:  0.02
```

Note: ratios differ between 1M and 10M because fixed overhead (SQL gen, ADBC dispatch, temp table creation) dominates more at smaller data sizes. Optimize on 1M (where overhead is amplified), validate on 10M.

## Progress log

| Run | Commit | combined_ratio | filter | mutate | summarise | Description |
|-----|--------|---------------|--------|--------|-----------|-------------|
| 0 | baseline | 7.02 | 22.83 | 12.14 | 1.25 | Starting point (main branch) |
| 1 | 2d21b94 | 6.49 | 22.62 | 8.71 | 1.39 | Merge 2 DESCRIBE into 1, extract row count from CTAS result |
| — | rebaselined | 5.71 | 13.53 | 12.43 | 1.21 | Baseline on Linux machine (different from M2 Pro) |
| 2 | dcc38bb | 5.25 | 9.83 | 13.25 | 1.11 | Flat SQL for single-op, skip DESCRIBE for schema-preserving ops |
| 3 | a204ef5 | **0.16** | 0.07 | 0.40 | 0.15 | Views instead of temp tables + no-ops shortcut (ADBC GC fix) |
| 4 | 6f9ef0c | **0.12** | 0.09 | 0.15 | 0.14 | Derive schema from ops, skip DESCRIBE for mutate/select/discard/rename |
| 5 | ef6b99e | **0.10** | 0.08 | 0.16 | 0.06 | Derive schema for summarise by threading group state |
| 6 | 28e52e7 | 0.11 | 0.09 | 0.13 | 0.10 | Chained views (depth≤3) for iterative patterns |
| 7 | dd0edc4 | **0.09** | 0.08 | 0.18 | 0.05 | Flat SQL for group_by+summarise (skip CTE) |
| 8 | 8bddedf | **0.08** | 0.10 | 0.12 | 0.05 | Command dispatch for CREATE VIEW (skip stream overhead) |
