# Benchmark: ADBC Migration (v0.1.1 NIF → ADBC)

Machine: Apple M-series (arm64), macOS
Date: 2026-03-23

## Results

| Operation | NIF (v0.1.1) | ADBC | Change |
|-----------|-------------|------|--------|
| from_query(10K) → compute | 0.10ms | 1.18ms | 12x slower |
| from_list(100) → compute | 3.41ms | 6.04ms | 1.8x slower |
| to_rows (1K) | 3.69ms | 6.78ms | 1.8x slower |
| join (small) → compute | 4.24ms | 6.39ms | 1.5x slower |
| **from_list(10K) → compute** | **3675ms** | **5.81ms** | **633x faster** |
| **full pipeline → to_rows** | **625ms** | **4.88ms** | **128x faster** |
| **group_by + summarise** | **655ms** | **5.81ms** | **113x faster** |
| **filter + mutate → compute** | **3143ms** | **7.29ms** | **431x faster** |
| **to_columns (10K)** | **3150ms** | **8.08ms** | **390x faster** |
| distributed(2) vs local | — | 1.94x | — |

## Analysis

**ADBC is slower for pure SQL queries** (12x for `from_query`) because the NIF kept data
in Arrow RecordBatches in Rust memory (no temp table creation), while ADBC ingests into
a temp table (query + materialize + ingest).

**ADBC is massively faster for `from_list` operations** (100-633x) because `from_list` in
v0.1.1 generated a SQL `UNION ALL` for each row — a catastrophically expensive approach
for >500 rows. ADBC uses `Adbc.Connection.ingest` which bypasses SQL entirely, going
directly from Elixir data → Arrow columnar → DuckDB temp table.

The NIF's `from_list` bottleneck affected every benchmark that used it (full pipeline,
group_by, filter, to_columns) since the data source was always `from_list(medium_list())`.
