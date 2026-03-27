# Autoresearch: Ideas & Dead Ends

## Key insight

Summarise is already at parity (~1x). The bottleneck is filter and mutate, which return **large result sets** (4M and 10M rows respectively). The cost is not DuckDB's query execution — it's what Dux does around it: creating temp tables to store results nobody reads.

## Ideas to try (ordered by expected impact)

**Priority rule**: Exhaust Dux-only (`lib/`) optimizations first. Only modify the ADBC Elixir wrapper (`../adbc/lib/`) after Dux-side ideas are exhausted or hit clear diminishing returns. Mark ADBC-dependent ideas with ⚠️.

### HIGH — Temp table elimination (Dux-only approaches)

- [x] **Skip CTE wrapping for single-op pipelines + inline table source**: Flat SQL without CTE for single-op pipelines, table name used directly instead of subquery. ~8% combined improvement.
- [x] **Schema propagation for schema-preserving ops**: Skip DESCRIBE when ops don't change schema (filter, sort, head, slice, distinct, drop_nil, group_by, ungroup). Part of the ~8% improvement above.
- [ ] **Defer DESCRIBE to lazy access**: compute/1 currently calls `table_schema/2` (DESCRIBE) eagerly for schema-changing ops. If names/dtypes were fetched lazily on first access, compute/1 saves one round-trip. (But struct field access can't be intercepted in Elixir, so this would need a different struct design.)

### HIGH — Temp table elimination (⚠️ requires ADBC changes, try last)

- [x] **Use views instead of temp tables in compute/1**: Views for non-PIVOT pipelines on materialized table sources. ADBC GC handler modified to DROP VIEW for `__dux_v_` prefixed names. TableRef.deps keeps source tables alive. No-ops compute shortcut. **combined_ratio: 5.71 → 0.16 (97% reduction)**.
- [ ] **Direct query mode for compute/1**: Instead of `CREATE TEMP TABLE __tmp AS (SELECT ...)`, just execute the SELECT and hold a reference to the result. (Likely unnecessary now that views work.)
- [ ] **Lazy view chain**: Keep everything as views until terminal materialization. (Partially implemented — first-level views work, chained views fall back to tables to prevent dependency cascade.)

### MEDIUM — Query dispatch overhead (Dux-only)

- [x] **Eliminate double DESCRIBE in compute/1**: `table_names` and `table_dtypes` each called DESCRIBE separately. Combined into single `table_schema/2` call. ~7.5% improvement combined with row count extraction below.
- [x] **Extract row count from CTAS result**: DuckDB's CREATE TABLE AS returns a "Count" column with the inserted row count. `query_with_count/2` captures this, eliminating the separate `SELECT count(*)` query.
- [ ] **Simplify SQL generation for single-op pipelines**: filter on a table source doesn't need CTEs — it could be `SELECT * FROM table WHERE condition` directly instead of a CTE chain.
- [ ] **Batch parameter binding**: Check if parameter binding has per-parameter overhead.

### LOW — Micro-optimizations (Dux-only)

- [x] **IO list instead of string concat for SQL building**: Profiled at 0μs — SQL build is negligible. Not worth changing.
- [x] **Cache compiled SQL for identical pipelines**: Not needed — SQL build is 0μs.
- [x] **Reduce GC sentinel overhead**: Profiled at 0μs — negligible.
- [x] **Pool dispatch**: Profiled as negligible (persistent_term lookup is essentially free).
- [x] **Chained views (depth ≤ 3)**: Views can now chain up to 3 levels before falling back to CTAS. Helps iterative algorithms.
- [x] **Command dispatch for DDL**: Added `Adbc.Connection.execute/2` that uses command dispatch (no stream) for CREATE VIEW. Saves stream setup/teardown overhead.
- [ ] **Skip view creation entirely (lazy SQL source)**: Return {:sql, ...} from compute instead of creating a DuckDB view. Zero DuckDB overhead but defers errors to data access. Would require changes to to_rows/to_columns. Diminishing returns territory.
- [ ] **NIF-level view creation**: Bypass GenServer/ADBC and use DuckDB C API directly. Major undertaking for marginal gain.

### NEW — End-to-end (compute + read data) optimization

The `compute()` benchmark (combined_ratio=0.08) measures pipeline definition, not data materialization. When data is read via `to_rows()`, Dux is ~1.5x slower than Explorer. Profiling at 1M rows (filter, 400k result rows):

```
DuckDB query execution:     80ms  (10%)  — fast, not the bottleneck
Adbc.Result.to_map:        235ms  (30%)  — Arrow → Elixir column lists
Row transposition:         213ms  (27%)  — column lists → row maps
Other overhead:            261ms  (33%)  — normalize_value, GC, etc.
TOTAL:                     790ms
Explorer equivalent:       521ms
```

Ideas (updated after batch normalize fix — to_rows now 1.13x, to_columns at parity 0.98x):
- [x] **Skip normalize_value for known safe types**: Batch per-column check. to_rows 1.52x → 1.13x, to_columns 1.24x → 0.98x.
- [ ] **⚠️ Optimize Adbc.Result.to_map for single-batch** (ADBC change): Current path wraps in Enum.zip_with + Enum.flat_map even for the common single-batch case. Skip the wrappers and process directly.
- [ ] **⚠️ Combined materialize+to_list in ADBC** (ADBC change): Two-step path (NIF materialize → Elixir to_list) could be a single NIF that builds the Elixir list directly from Arrow buffers, skipping intermediate binary allocation.
- [ ] **⚠️ Direct row building from materialized columns** (ADBC change): Build row maps directly from Adbc.Column structs without going through to_list + Map.new transposition.
- [ ] **Encourage to_columns over to_rows**: to_columns is already at parity. Document as the fast path.

## Dead ends (tried and failed)

- **Views instead of temp tables (full replacement)**: Attempted to use `CREATE TEMP VIEW` instead of `CREATE TEMP TABLE` in compute/1. Views are near-instant to create since they don't materialize data. However, two blocking issues:
  1. DuckDB doesn't support data-driven PIVOT in views ("PIVOT statements with pivot elements extracted from the data cannot be used in views").
  2. Views are never dropped by the GC mechanism (`adbc_delete_on_gc_new` runs `DROP TABLE IF EXISTS` which is a no-op for views). Accumulated views across test runs cause DuckDB OOM errors.
  A partial approach (views only for non-PIVOT, non-chained ops) was attempted but still caused OOM in graph algorithm tests. The fundamental issue is that `DROP VIEW IF EXISTS` cleanup requires changes to the ADBC NIF layer.

## Key learnings

- The ADBC `adbc_delete_on_gc_new` NIF mechanism only generates `DROP TABLE IF EXISTS` — it cannot clean up views. Any view-based optimization needs a different cleanup strategy.
- DuckDB's CTAS (`CREATE TABLE AS`) returns the row count in a "Count" column in the result set. This can be extracted to avoid a separate `SELECT count(*)` query.
- The `table_names` and `table_dtypes` functions in Backend each call `DESCRIBE` independently — combining them into one call halves the metadata query overhead.
- The dominant cost in compute/1 is still the CTAS materialization itself (creating the temp table), not the metadata queries. Eliminating metadata overhead gives ~7.5% improvement, but reaching Explorer parity requires avoiding materialization entirely.
- Views eliminate CTAS cost entirely. CREATE TEMP VIEW is ~0.7ms vs ~50ms for CTAS at 500k rows. The key is keeping source tables alive via deps in TableRef, and falling back to tables for chained views (to prevent infinite dependency chains in iterative algorithms like PageRank).
- No-ops compute shortcut (`compute()` on a table with no ops) is critical — without it, `from_list |> compute` creates a needless view, and subsequent operations see deps!=[] and fall back to CTAS.
- ADBC GC handler uses name prefix convention (`__dux_v_` → DROP VIEW) to avoid NIF changes.
- At combined_ratio=0.10, remaining overhead is ~430μs per compute. View creation via ADBC GenServer is ~377μs (87%). Only ~55μs of pure Dux overhead remains. Further gains require reducing ADBC round-trip or DuckDB statement overhead.
- Schema derivation eliminates DESCRIBE for filter (preserved), mutate (append names), select/discard (subset), rename (remap), summarise (groups+aggs). Only joins, pivots, and json_unnest still need DESCRIBE.
