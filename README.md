# Dux

[![CI](https://github.com/elixir-dux/dux/actions/workflows/ci.yml/badge.svg)](https://github.com/elixir-dux/dux/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/hex.pm-docs-green.svg?style=flat)](https://hexdocs.pm/dux)
[![Hex.pm](https://img.shields.io/hexpm/v/dux.svg)](https://hex.pm/packages/dux)

> **Note:** Dux is under active development and not yet production ready. APIs may change between releases.

**DuckDB-native dataframes for Elixir.**

Dux gives you a [dplyr](https://dplyr.tidyverse.org)-style verb API backed by DuckDB's analytical engine, with built-in distributed execution across the BEAM. Pipelines are lazy, operations compile to SQL, and DuckDB handles columnar execution, vectorised aggregation, and predicate pushdown.

```elixir
require Dux

Dux.from_parquet("s3://data/sales/**/*.parquet")
|> Dux.filter(amount > 100 and region == "US")
|> Dux.mutate(revenue: price * quantity)
|> Dux.group_by(:product)
|> Dux.summarise(total: sum(revenue), orders: count(product))
|> Dux.sort_by(desc: :total)
|> Dux.to_rows()
```

## Performance

Dux pipelines compile to SQL and execute inside DuckDB — no data crosses into Elixir until you materialise. On a 10M-row dataset (Apple M3 Max, 36GB):

| Operation | Dux | Explorer (Polars) | Ratio |
|-----------|-----|-------------------|-------|
| Filter (10M rows) | 41ms | 13ms | 3.1x |
| Mutate (10M rows) | ~40ms | ~14ms | ~3x |
| Group + Summarise | ~12ms | ~21ms | **0.6x** |
| Memory per compute | 5-10 KB | 5-10 KB | ~same |

Dux is within 3x of Polars for single-node operations and **faster for aggregations** (DuckDB's columnar engine). The gap narrows further at scale — Dux can distribute across machines while Polars is single-node.

## Design

Dux is the successor to [Explorer](https://github.com/elixir-explorer/explorer). That means it borrows its verb design from dplyr and the tidyverse — constrained, composable operations that each do one thing well. If you've used `dplyr::filter()`, `mutate()`, `group_by() |> summarise()`, the Dux API will feel familiar.

Where Dux diverges from Explorer:

- **The module IS the dataframe.** `Dux.filter(df, ...)` not `Dux.DataFrame.filter(df, ...)`. No Series API — all operations are dataframe-level.
- **DuckDB is the only engine.** No pluggable backends, no abstraction tax. Full access to DuckDB's SQL functions, window functions, recursive CTEs, and 50+ extensions.
- **Lazy by default.** Operations accumulate as an AST in `%Dux{}`. When you materialise (`compute/1`, `to_rows/1`), the whole pipeline compiles to a chain of SQL CTEs and DuckDB optimises end-to-end.
- **Distributed on the BEAM.** `%Dux{}` is plain data — ship it to any BEAM node, compile to SQL there, execute against that node's local DuckDB. No function serialisation, no cluster manager, no heavyweight RPC.

## Installation

```elixir
def deps do
  [{:dux, "~> 0.3.0"}]
end
```

Dux is a pure Elixir project. The DuckDB engine is provided via [ADBC](https://github.com/elixir-explorer/adbc) — a precompiled driver downloaded automatically at compile time. No Rust or C++ compilation needed.

## Getting Started

```elixir
require Dux

# Built-in datasets — no files needed
Dux.Datasets.flights()
|> Dux.filter(distance > 1000)
|> Dux.group_by(:origin)
|> Dux.summarise(avg_delay: avg(arr_delay), n: count(flight))
|> Dux.sort_by(desc: :avg_delay)
|> Dux.head(5)
|> Dux.to_rows()
```

Every verb (`filter`, `mutate`, `group_by`, `summarise`, etc.) takes Elixir expressions via macros. Bare identifiers become column names. `^` interpolates Elixir values safely as parameter bindings:

```elixir
min_amount = 500
Dux.filter(df, amount > ^min_amount and status == "active")
```

All DuckDB functions work inside expressions — `year()`, `lower()`, `coalesce()`, `regexp_matches()`, and [hundreds more](https://duckdb.org/docs/sql/functions/overview). `cond` maps to `CASE WHEN`, `in` maps to `IN`:

```elixir
Dux.mutate(df,
  tier: cond do
    amount > 1000 -> "gold"
    amount > 100 -> "silver"
    true -> "bronze"
  end
)

Dux.filter(df, status in ["active", "pending"])
```

The `_with` variants accept raw DuckDB SQL for window functions and other constructs the macro doesn't cover:

```elixir
Dux.mutate_with(df, rank: "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)")
```

## IO

Read and write CSV, Parquet, NDJSON, Excel, and database tables:

```elixir
df = Dux.from_parquet("s3://bucket/data/**/*.parquet")
df = Dux.from_csv("data.csv", delimiter: "\t")
df = Dux.from_excel("sales.xlsx", sheet: "Q1")

Dux.to_parquet(df, "output/", partition_by: [:year, :month])
Dux.to_excel(df, "report.xlsx")
Dux.insert_into(df, "pg.public.events", create: true)
```

Cross-source queries via DuckDB's ATTACH — Postgres, MySQL, SQLite, Iceberg, Delta, DuckLake:

```elixir
Dux.attach(:warehouse, "host=db.internal dbname=analytics", type: :postgres)
customers = Dux.from_attached(:warehouse, "public.customers")

Dux.from_parquet("s3://lake/orders/*.parquet")
|> Dux.join(customers, on: :customer_id)
|> Dux.group_by(:region)
|> Dux.summarise(revenue: sum(amount))
|> Dux.to_rows()
```

## Distributed Execution

Mark a pipeline for distributed execution with `distribute/2`. The same verbs work — Dux handles partitioning, fan-out, and merge automatically:

```elixir
workers = Dux.Remote.Worker.list()

Dux.from_parquet("s3://lake/events/**/*.parquet")
|> Dux.distribute(workers)
|> Dux.filter(year == 2024)
|> Dux.group_by(:region)
|> Dux.summarise(total: sum(revenue))
|> Dux.to_rows()
```

Under the hood: the Coordinator partitions files across workers (size-balanced, with Hive partition pruning), each worker compiles and executes SQL against its local DuckDB, and the Merger re-aggregates results. Workers read from and write to storage directly — no data funnels through the coordinator.

Distributed writes work the same way:

```elixir
Dux.from_parquet("s3://input/**/*.parquet")
|> Dux.distribute(workers)
|> Dux.filter(status == "active")
|> Dux.to_parquet("s3://output/", partition_by: :year)
```

Attach Postgres and distribute reads with `partition_by:`:

```elixir
Dux.from_attached(:pg, "public.orders", partition_by: :id)
|> Dux.distribute(workers)
|> Dux.insert_into("pg.public.summary", create: true)
```

See the [Distributed Execution](https://hexdocs.pm/dux/distributed.html) guide for the full architecture — aggregate rewrites, broadcast vs shuffle joins, streaming merge, and fault tolerance.

## Graph Analytics

A graph is two dataframes. All algorithms return `%Dux{}` — pipe into any verb:

```elixir
graph = Dux.Graph.new(vertices: users, edges: follows)

graph |> Dux.Graph.pagerank() |> Dux.sort_by(desc: :rank) |> Dux.head(10)
graph |> Dux.Graph.shortest_paths(start_node)
graph |> Dux.Graph.connected_components()
```

## Livebook

Add [`kino_dux`](https://github.com/elixir-dux/kino_dux) for rich rendering and smart cells in Livebook:

```elixir
Mix.install([
  {:dux, "~> 0.3.0"},
  {:kino_dux, "~> 0.1"}
])
```

Lazy pipelines render with source provenance, operations, and generated SQL. Computed results become interactive data tables.

## Guides

- [Getting Started](https://hexdocs.pm/dux/getting-started.html) — core concepts, expressions, pipelines
- [Data IO](https://hexdocs.pm/dux/data-io.html) — CSV, Parquet, Excel, NDJSON, database writes
- [Transformations](https://hexdocs.pm/dux/transformations.html) — filter, mutate, window functions
- [Joins & Reshape](https://hexdocs.pm/dux/joins-and-reshape.html) — join types, ASOF joins, pivots
- [Distributed Execution](https://hexdocs.pm/dux/distributed.html) — architecture, partitioning, distributed IO
- [FLAME Clusters](https://hexdocs.pm/dux/flame-clusters.html) — ad-hoc Spark-like clusters with Fly.io
- [Graph Analytics](https://hexdocs.pm/dux/graph-analytics.html) — PageRank, shortest paths, components
- [Cheatsheet](https://hexdocs.pm/dux/cheatsheet.html) — quick reference for all verbs

## License

Dual-licensed under Apache 2.0 and MIT. See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT).

## Links

- [HexDocs](https://hexdocs.pm/dux)
- [Hex.pm](https://hex.pm/packages/dux)
- [GitHub](https://github.com/elixir-dux/dux)
- [Changelog](CHANGELOG.md)
