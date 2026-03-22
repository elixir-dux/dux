# Dux

**DuckDB-native DataFrames for Elixir.**

Dux is a dataframe library where DuckDB is the execution engine and the BEAM is the distributed runtime. Pipelines are lazy, operations compile to SQL CTEs, and DuckDB handles all the heavy lifting.

```elixir
require Dux

Dux.from_parquet("s3://data/sales/**/*.parquet")
|> Dux.filter(amount > 100 and region == ^selected_region)
|> Dux.mutate(revenue: price * quantity)
|> Dux.group_by(:product)
|> Dux.summarise(total: sum(revenue), orders: count(product))
|> Dux.sort_by(desc: :total)
|> Dux.to_parquet("results.parquet", compression: :zstd)
```

## Why Dux?

- **The module IS the dataframe.** `Dux.filter(df, ...)` — no `Dux.DataFrame`, no `Dux.Series`. Just verbs that pipe.
- **Everything is lazy.** Operations accumulate until `compute/1`. DuckDB optimizes the full pipeline.
- **DuckDB-only.** No pluggable backends, no abstraction tax. Full access to DuckDB extensions, window functions, recursive CTEs.
- **Elixir expressions compile to SQL.** `Dux.filter(df, x > ^min_val)` becomes `WHERE x > $1` with parameter bindings. SQL injection safe by construction.
- **Distributed.** Ship `%Dux{}` structs to any BEAM node, compile to SQL there, execute against that node's local DuckDB. Fan out with the Coordinator, merge results.
- **Graph analytics.** `Dux.Graph` — a graph is two dataframes (vertices + edges). PageRank, shortest paths, connected components as verb compositions.
- **Nx interop.** Numeric columns become tensors via `Nx.LazyContainer`. Zero-copy where possible.

## Installation

```elixir
def deps do
  [
    {:dux, "~> 0.1.1"}
  ]
end
```

Precompiled NIF binaries are available for macOS (arm64, x86_64), Linux (gnu, musl), and Windows. No Rust or DuckDB compilation needed.

To force a local build (requires Rust toolchain):

```bash
DUX_BUILD=true mix deps.compile dux --force
```

## Quick start

```elixir
require Dux

# Read data
df = Dux.from_csv("sales.csv")

# Transform
result =
  df
  |> Dux.filter(amount > 100)
  |> Dux.mutate(tax: amount * 0.08)
  |> Dux.group_by(:region)
  |> Dux.summarise(total: sum(amount), avg_tax: avg(tax))
  |> Dux.sort_by(desc: :total)
  |> Dux.to_rows()

# result is a list of maps:
# [%{"region" => "US", "total" => 15000, "avg_tax" => 120.0}, ...]
```

## Verbs

All operations are verbs on `%Dux{}` structs:

| Verb             | Description                                                |
| ---------------- | ---------------------------------------------------------- |
| `filter/2`       | Filter rows (macro: `filter(df, x > 10)`)                  |
| `mutate/2`       | Add/replace columns (macro: `mutate(df, y: x * 2)`)        |
| `select/2`       | Keep columns                                               |
| `discard/2`      | Drop columns                                               |
| `sort_by/2`      | Sort rows (asc/desc)                                       |
| `group_by/2`     | Group for aggregation                                      |
| `summarise/2`    | Aggregate (macro: `summarise(df, total: sum(x))`)          |
| `join/3`         | Inner, left, right, cross, anti, semi joins                |
| `head/2`         | First N rows                                               |
| `slice/3`        | Offset + limit                                             |
| `distinct/1`     | Deduplicate                                                |
| `drop_nil/2`     | Remove rows with nil values                                |
| `rename/2`       | Rename columns                                             |
| `pivot_wider/4`  | Long → wide (DuckDB PIVOT)                                 |
| `pivot_longer/3` | Wide → long (DuckDB UNPIVOT)                               |
| `concat_rows/1`  | UNION ALL                                                  |
| `compute/1`      | Execute the pipeline                                       |
| `to_rows/1`      | Execute and return list of maps (`atom_keys: true` option) |
| `to_columns/1`   | Execute and return column map                              |
| `peek/2`         | Print formatted table preview                              |
| `n_rows/1`       | Count rows                                                 |
| `sql_preview/2`  | Show generated SQL (`pretty: true` option)                 |

The `_with` variants (`filter_with/2`, `mutate_with/2`, `summarise_with/2`) accept raw SQL strings for programmatic use.

## IO

DuckDB handles all file formats and remote access natively:

```elixir
# Read
Dux.from_csv("data.csv", delimiter: "\t")
Dux.from_parquet("data/**/*.parquet")
Dux.from_ndjson("events.ndjson")
Dux.from_query("SELECT * FROM read_parquet('s3://bucket/data.parquet')")

# Write
Dux.to_csv(df, "output.csv")
Dux.to_parquet(df, "output.parquet", compression: :zstd)
Dux.to_ndjson(df, "output.ndjson")
```

S3, HTTP, Postgres, MySQL, SQLite — all via DuckDB extensions. No separate libraries needed.

## Distributed queries

Dux distributes analytical workloads across a BEAM cluster:

```elixir
# Workers auto-register via :pg
workers = Dux.Remote.Worker.list()

# Mark for distributed, then use the same verbs
Dux.from_parquet("data/**/*.parquet")
|> Dux.distribute(workers)
|> Dux.filter(amount > 100)
|> Dux.group_by(:region)
|> Dux.summarise(total: sum(amount))
|> Dux.to_rows()
```

No function serialization — `%Dux{}` is plain data. Ship it anywhere, compile to SQL there. No cluster manager — just `libcluster` + `:pg`. No heavyweight RPC — just `:erpc.multicall`.

## Graph analytics

```elixir
graph = Dux.Graph.new(vertices: users, edges: follows)

# All algorithms are verb compositions
graph |> Dux.Graph.pagerank() |> Dux.sort_by(desc: :rank) |> Dux.head(10)
graph |> Dux.Graph.shortest_paths(start_node)
graph |> Dux.Graph.connected_components()
graph |> Dux.Graph.triangle_count()

# Distribute graph across workers
graph |> Dux.Graph.distribute(workers) |> Dux.Graph.pagerank()
```

## Nx interop

Numeric columns become tensors:

```elixir
tensor = Dux.to_tensor(df, :price)
# #Nx.Tensor<f64[1000] [...]>
```

`Dux` implements `Nx.LazyContainer` for use in `defn`.

## Raw SQL escape hatch

For anything the macro doesn't support — window functions, CASE WHEN, PIVOT, CTEs — use the `_with` variants with raw DuckDB SQL:

```elixir
# Window functions
Dux.mutate_with(df, rank: "ROW_NUMBER() OVER (PARTITION BY \"dept\" ORDER BY \"salary\" DESC)")

# CASE WHEN
Dux.mutate_with(df, tier: "CASE WHEN amount > 1000 THEN 'high' ELSE 'low' END")

# Pivot
Dux.from_query("PIVOT sales ON product USING SUM(amount) GROUP BY region")

# Any DuckDB SQL
Dux.from_query("SELECT * FROM read_parquet('s3://bucket/data.parquet') WHERE year = 2025")
```

## License

Dual-licensed under Apache 2.0 and MIT. See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT).

## Links

- [Documentation](https://hexdocs.pm/dux)
- [GitHub](https://github.com/elixir-dux/dux)
- [Changelog](CHANGELOG.md)
