# Dux Usage Rules

DuckDB-native dataframes for Elixir. A `%Dux{}` is a lazy pipeline: verbs append
operations to a struct, and the whole pipeline compiles to one chain of SQL CTEs
that DuckDB executes when you materialize. The module *is* the dataframe —
`Dux.filter(df, ...)`, not `Dux.DataFrame.filter/2`. There is no Series API.

## Setup

```elixir
{:dux, "~> 0.3"}
```

No configuration needed — the DuckDB connection starts with the `:dux` application.
`require Dux` in any module using the macro verbs (`filter/2`, `mutate/2`,
`summarise/2`); `use Dux` is a shorthand that does the same.

## Core rules

- **Build lazily, materialize once.** Verbs are pure struct updates. Nothing hits
  DuckDB until `compute/1`, `to_rows/1`, `to_columns/1`, `n_rows/1`, `peek/1`, or
  `to_tensor/2`. Don't `compute/1` between verbs to "check" intermediate state —
  use `sql_preview/2` instead.
- **Interpolate Elixir values with `^`.** Bare identifiers are column names, so an
  un-pinned variable compiles to a column reference and fails at bind time.

  ```elixir
  min = 500
  Dux.filter(df, amount > ^min)      # parameter binding — correct and injection-safe
  Dux.filter(df, amount > min)       # Binder Error: column "min" not found
  ```

- **Reach for `_with` variants for raw SQL.** `filter_with/2`, `mutate_with/2`,
  `summarise_with/2` take DuckDB SQL strings for anything the macro doesn't cover.
  Never string-interpolate user input into them — use `^` in the macro form instead.
- **Column lists must be lists.** `select/2`, `discard/2`, `drop_nil/2` take a list;
  `Dux.select(df, :name)` raises. `group_by/2` and `sort_by/2` accept a bare column.
- **Results use string keys** unless you pass `atom_keys: true` to `to_rows/2`.

## Expressions

Inside `filter/2`, `mutate/2`, and `summarise/2`:

```elixir
Dux.filter(df, status in ["active", "pending"] and not archived)
Dux.mutate(df, full: first <> " " <> last, y: year(created_at), safe: coalesce(n, 0))
Dux.mutate(df, tier: cond do
  amount > 1000 -> "gold"
  amount > 100 -> "silver"
  true -> "bronze"
end)
```

- **Every DuckDB function works** — calls pass through unchanged. Don't look for a
  Dux wrapper; `regexp_matches/2`, `date_trunc/2`, `list_extract/2`, `cast/2` and
  the rest of DuckDB's library are already available.
- `cond` compiles to `CASE WHEN`, `if/else` to a two-branch `CASE`, `in` to `IN`,
  `<>` to `||`.
- Later assignments in one `mutate/2` can reference earlier ones:
  `Dux.mutate(df, y: x + 1, z: y * 2)`.
- **Window functions use `over/2`** inside `mutate/2`, not `_with`:

  ```elixir
  Dux.mutate(df,
    rank: over(row_number(), partition_by: :dept, order_by: [desc: :salary]),
    moving_avg: over(avg(x), order_by: :date, frame: {:rows, -2, :current})
  )
  ```

## Aggregation

```elixir
df
|> Dux.group_by(:region)
|> Dux.summarise(total: sum(amount), n: count(), avg_price: avg(price))
```

- Aggregations: `sum`, `avg`/`mean`, `min`, `max`, `count`, `count_distinct`,
  `std`, `variance`. `count()` with no argument is `COUNT(*)`.
- `summarise/2` without a preceding `group_by/2` aggregates the whole frame to one row.
- `summarise/2` consumes the grouping — no `ungroup/1` needed afterwards. Use
  `ungroup/1` only to drop groups you set but never aggregated.
- Filter *after* `summarise/2` to filter on aggregate results (a `HAVING`).

## IO

```elixir
Dux.from_csv("data.csv", delimiter: "\t")
Dux.from_parquet("s3://bucket/**/*.parquet")
Dux.to_parquet(df, "out/", partition_by: [:year, :month])
Dux.insert_into(df, "pg.public.events", create: true)
```

- Cross-source queries go through `attach/3` + `from_attached/3` (Postgres, MySQL,
  SQLite, Iceberg, Delta, DuckLake). Join attached tables and files directly — no
  need to load either side into Elixir.
- Register cloud credentials once with `create_secret/2`, not per-read.
- `from_query/1` is the escape hatch for any SQL DuckDB accepts; `exec/1` is for
  DDL/DML (`SET`, `INSTALL`) and returns no dataframe.

## Distributed

```elixir
workers = Dux.Remote.Worker.list()

Dux.from_parquet("s3://lake/events/**/*.parquet")
|> Dux.distribute(workers)
|> Dux.group_by(:region)
|> Dux.summarise(total: sum(revenue))
|> Dux.to_rows()
```

- `distribute/2` only marks the pipeline — the same verbs work unchanged.
  Partitioning, fan-out, aggregate rewrites, and merge are automatic.
- `collect/1` brings distributed results back as a local `%Dux{}`;
  `to_rows/1` and `to_columns/1` already collect. `local/1` clears the workers.
- Workers read and write storage directly. Keep the distributed source a
  file glob or a `from_attached(..., partition_by: ...)` table — a materialized
  local table has to be shipped through the coordinator.
- `Dux.Flame.spin_up/2` provisions ephemeral workers on a FLAME pool.

## Common mistakes

- Forgetting `require Dux` — the macro verbs fail to compile with `undefined variable`
  errors on your column names, plus a warning that `Dux.filter/2` is a macro.
- Forgetting `^` on an Elixir variable — see above; this is the most common error.
- Calling `Dux.compute/1` repeatedly inside a pipeline instead of once at the end.
- Building SQL by string interpolation in a `_with` verb instead of pinning values.
- `to_tensor/2` needs the optional `:nx` dep, raises on columns containing nulls
  (filter them first), and does not support boolean or decimal columns.
- Assuming row order without `sort_by/2` — DuckDB does not guarantee it.

## Testing

`Dux.Datasets` ships CC0 datasets — `penguins/0`, `gapminder/0`, `flights/0`,
`airlines/0`, `airports/0`, `planes/0`, `karate_club/0` — so tests and examples
need no fixture files. `Dux.from_list/1` is the fastest way to build a small frame
inline. Assert on `to_rows/2` output, sorting with `sort_by/2` first so the
comparison is deterministic.
