# Autoresearch: End-to-End Performance (to_rows)

## Objective

Close the ~25% e2e gap between Dux and Explorer when materializing data via `to_rows()`. Pipeline definition (`compute`) is already 10-20x faster — the gap is in reading data back to Elixir.

## Target metric

`e2e_ratio` = Dux to_rows time / Explorer to_rows time. **Target: ≤ 1.0** (parity or better).

## Current state (1M rows, filter returning ~400k rows)

```
filter  to_rows: ratio=~1.02  (at parity with Explorer)
mutate  to_rows: ratio=~1.01  (at parity with Explorer)
summarise to_rows: ratio=~1.42  (DuckDB aggregation overhead)
e2e_combined: ~1.15
```

### Progress log

| Baseline | After batch rows | After ADBC fast decoders |
|----------|-----------------|-------------------------|
| filter 1.26x | 1.09x | **1.02x** |
| mutate 1.25x | 0.95x | **1.01x** |
| summarise 1.72x | 1.09x | **1.42x** |
| combined 1.39x | 1.04x | **1.15x** |

## Where the time goes (Dux filter to_rows, 1M rows)

```
1. compute (view creation):       1ms   (0%)  — already optimized
2. DuckDB query execution:       80ms  (10%)  — irreducible
3. Adbc.Result.to_map:          235ms  (30%)  — ADBC Elixir layer ← target
   └ Column.materialize (NIF):   ?ms          — Arrow binary → Elixir binary
   └ Column.to_list (Elixir):    ?ms          — binary → Elixir list
4. build_rows_from_map:         200ms  (25%)  — Dux Elixir layer ← target
   └ column lists → row maps (400k maps × 5 cols)
5. Other (GC, dispatch):       ~100ms  (13%)
```

## Files in scope

### ADBC Elixir layer (`../adbc/lib/`) — 45% of the gap
- `adbc/lib/adbc/result.ex` — `to_map/1`, `to_columns/1`, `materialize/1`
- `adbc/lib/adbc/column.ex` — `materialize/1`, `to_list/1`, binary decoders

### Dux layer (`lib/`) — 39% of the gap
- `lib/dux/backend.ex` — `build_rows_from_map/1`, `table_to_rows/2`, `table_to_columns/2`

### Off limits
- `adbc/c_src/` — NIF C++ code (last resort)
- `test/` — tests are the correctness gate
- `auto/bench_quick.exs` — compute benchmark (separate concern)

## Ideas (ordered by expected impact)

### HIGH — ADBC Elixir layer
- [x] **Single-batch fast path for to_map**: Skip Enum.zip_with + Enum.flat_map for common single-batch results. ~15-20% improvement on to_rows.
- [ ] **Profile Column.materialize vs Column.to_list**: Measure the split between NIF materialization and Elixir binary decoding. If to_list dominates, focus there.
- [ ] **Batch to_list per type**: The `to_list` function pattern-matches column type then decodes binary. For common types (s64, f64, string), see if the decode functions can be made faster.
- [ ] **Skip to_list for identity types**: For integer and float columns, ADBC's binary format might already be a flat array of native values. Could we pass the binary directly as an Erlang binary term list?

### MEDIUM — Dux layer
- [ ] **Build rows without intermediate column lists**: Instead of to_map → build_rows_from_map, decode Arrow columns directly into row maps in a single pass.
- [ ] **Use :ets or persistent structure for large results**: For 400k+ rows, the list-of-maps result is expensive to build. Could use a different return format.
- [ ] **Parallel column decoding**: Decode each column in a separate process, then zip.

### LOW — NIF level (last resort)
- [ ] **Combined materialize+to_list NIF**: Single NIF call that builds Elixir list directly from Arrow buffers.
- [ ] **NIF row builder**: Build row maps in C/C++, avoiding Elixir allocation overhead entirely.

## How to run

### Quick e2e benchmark
```bash
mix run auto/bench_e2e_check.exs
```

### Full gate (existing — ensures compute perf doesn't regress)
```bash
bash auto/autoresearch.sh
```

## Constraints
- All 411 tests must pass
- compute benchmark (combined_ratio) must not regress
- No changes to public API
- ADBC changes must not break ADBC's own behavior
