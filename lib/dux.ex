defmodule Dux do
  @moduledoc """
  DuckDB-native dataframe library for Elixir.

  Dux gives you a dataframe API that compiles to SQL and executes on DuckDB.
  Pipelines are lazy — operations accumulate as an AST until you materialize.
  DuckDB handles all the heavy lifting: columnar execution, parallel scans,
  predicate pushdown, and vectorized aggregation.

  This module contains the core dataframe verbs. For graph analytics, see
  `Dux.Graph`. For distributed execution, see `Dux.Remote`. For embedded
  datasets, see `Dux.Datasets`.

  ## Quick start

      require Dux

      Dux.Datasets.penguins()
      |> Dux.filter(species == "Gentoo" and body_mass_g > 5000)
      |> Dux.group_by(:island)
      |> Dux.summarise(count: count(species), avg_mass: avg(body_mass_g))
      |> Dux.to_rows()

  ## How it works

  1. **Build** — each verb (`filter`, `mutate`, `group_by`, etc.) appends an
     operation to the `%Dux{}` struct. No computation happens.

  2. **Compile** — when you call `compute/1`, `to_rows/1`, or `to_columns/1`,
     the operation list compiles to a chain of SQL CTEs.

  3. **Execute** — DuckDB runs the SQL. Results land in a temporary table
     that's automatically cleaned up when garbage collected.

  Use `sql_preview/1` to see the generated SQL at any point:

      iex> require Dux
      iex> Dux.from_query("SELECT * FROM range(10) t(x)")
      ...> |> Dux.filter(x > 5)
      ...> |> Dux.sql_preview()
      ...> |> String.contains?("WHERE")
      true

  ## Expression syntax

  Verbs like `filter/2`, `mutate/2`, and `summarise/2` are macros that capture
  Elixir expressions. Bare identifiers become column names. Use `^` to
  interpolate Elixir values safely (as parameter bindings, not string
  interpolation):

      iex> require Dux
      iex> threshold = 3
      iex> Dux.from_query("SELECT * FROM range(1, 6) t(x)")
      ...> |> Dux.filter(x > ^threshold)
      ...> |> Dux.to_columns()
      %{"x" => [4, 5]}

  Every expression verb has a `_with` variant that accepts raw DuckDB SQL
  strings for full access to DuckDB's function library:

      iex> Dux.from_query("SELECT * FROM range(1, 6) t(x)")
      ...> |> Dux.filter_with("x > 3")
      ...> |> Dux.to_columns()
      %{"x" => [4, 5]}

  ## Distribution

  Mark a pipeline for distributed execution across BEAM nodes with
  `distribute/2`. The same verbs work — Dux partitions, fans out, and
  merges automatically. See `Dux.Remote` for details.

      workers = Dux.Remote.Worker.list()

      Dux.from_parquet("s3://data/**/*.parquet")
      |> Dux.distribute(workers)
      |> Dux.group_by(:region)
      |> Dux.summarise(total: sum(amount))
      |> Dux.to_rows()

  ## Embedded datasets

  `Dux.Datasets` ships with CC0 datasets for learning and testing:
  penguins, gapminder, nycflights13 (flights, airlines, airports, planes).
  """

  defstruct [:source, :remote, :workers, ops: [], names: [], dtypes: %{}, groups: []]

  @type source ::
          {:parquet, String.t()}
          | {:csv, String.t(), keyword()}
          | {:ndjson, String.t(), keyword()}
          | {:table, reference()}
          | {:sql, String.t()}

  @type t :: %__MODULE__{
          source: source(),
          ops: [tuple()],
          names: [String.t()],
          dtypes: %{String.t() => atom() | tuple()},
          groups: [String.t()]
        }

  # ---------------------------------------------------------------------------
  # Constructors
  # ---------------------------------------------------------------------------

  @doc group: :constructors
  @doc """
  Create a Dux from a raw SQL query.

  This is the most flexible constructor — anything DuckDB can query, you can use.

      iex> df = Dux.from_query("SELECT 1 AS x, 2 AS y")
      iex> Dux.to_rows(df)
      [%{"x" => 1, "y" => 2}]

      iex> df = Dux.from_query("SELECT * FROM range(3) t(n)")
      iex> Dux.to_columns(df)
      %{"n" => [0, 1, 2]}
  """
  def from_query(sql) when is_binary(sql) do
    %Dux{source: {:sql, sql}}
  end

  @doc group: :constructors
  @doc """
  Create a Dux from a list of maps.

  Each map is a row. Keys become column names. Both atom and string keys are supported.

      iex> df = Dux.from_list([%{name: "Alice", age: 30}, %{name: "Bob", age: 25}])
      iex> Dux.to_columns(df)
      %{"age" => [30, 25], "name" => ["Alice", "Bob"]}
  """
  def from_list(rows) when is_list(rows) do
    %Dux{source: {:list, rows}}
  end

  # ---------------------------------------------------------------------------
  # Distribution
  # ---------------------------------------------------------------------------

  @doc group: :distribution
  @doc """
  Mark a Dux for distributed execution across the given workers.

  All subsequent operations will automatically use the Coordinator to fan out
  work across the workers. Use `collect/1` to bring distributed results back
  to a local `%Dux{}`, or `to_rows/1` / `to_columns/1` to materialize directly.

  ## Examples

      workers = Dux.Remote.Worker.list()

      Dux.from_parquet("data/**/*.parquet")
      |> Dux.distribute(workers)
      |> Dux.filter(amount > 100)
      |> Dux.group_by(:region)
      |> Dux.summarise(total: sum(amount))
      |> Dux.to_rows()
  """
  def distribute(%Dux{} = dux, workers) when is_list(workers) do
    %{dux | workers: workers}
  end

  @doc group: :distribution
  @doc """
  Return to local execution, removing any distributed workers from the pipeline.

  This is the inverse of `distribute/2`. After calling `local/1`, all subsequent
  operations execute on the local node's DuckDB instance.

  ## Examples

      iex> df = Dux.from_list([%{x: 1}]) |> Dux.distribute([:fake])
      iex> df.workers
      [:fake]
      iex> Dux.local(df).workers
      nil
  """
  def local(%Dux{} = dux) do
    %{dux | workers: nil}
  end

  # ---------------------------------------------------------------------------
  # IO — reading
  # ---------------------------------------------------------------------------

  @doc group: :constructors
  @doc """
  Read a CSV file into a lazy Dux pipeline.

  All options are passed through to DuckDB's `read_csv()`.

  ## Options

    * `:delimiter` - field delimiter (default: `","`)
    * `:header` - whether the file has a header row (default: `true`)
    * `:null_padding` - pad missing columns with NULL (default: `false`)
    * `:skip` - number of rows to skip at the start
    * `:columns` - list of column names or indices to read
    * `:types` - map of column name to DuckDB type string
    * `:auto_detect` - auto-detect types (default: `true`)

  ## Examples

      iex> path = Path.join(Application.app_dir(:dux, "priv/datasets"), "airlines.csv")
      iex> Dux.from_csv(path) |> Dux.to_rows() |> length()
      16

      df = Dux.from_csv("data/sales.csv", delimiter: "\\t", skip: 1)
  """
  def from_csv(path, opts \\ []) when is_binary(path) do
    %Dux{source: {:csv, path, opts}}
  end

  @doc group: :constructors
  @doc """
  Read a Parquet file or glob pattern into a lazy Dux pipeline.

  Supports local files, globs, and remote URLs (S3, HTTP) when the
  appropriate DuckDB extension is loaded (httpfs).

  ## Examples

      df = Dux.from_parquet("data/sales.parquet")
      df = Dux.from_parquet("data/**/*.parquet")
      df = Dux.from_parquet("s3://bucket/data/*.parquet")

  Write a Parquet file and read it back:

      iex> path = Path.join(System.tmp_dir!(), "dux_doctest_#{:erlang.unique_integer([:positive])}.parquet")
      iex> Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}]) |> Dux.to_parquet(path)
      iex> Dux.from_parquet(path) |> Dux.to_columns()
      %{"x" => [1, 2, 3]}
  """
  def from_parquet(path, opts \\ []) when is_binary(path) do
    %Dux{source: {:parquet, path, opts}}
  end

  @doc group: :constructors
  @doc """
  Read a newline-delimited JSON (NDJSON) file into a lazy Dux pipeline.

  Each line in the file must be a valid JSON object. This is a common format
  for log files, streaming exports, and data interchange.

  ## Examples

      df = Dux.from_ndjson("events.ndjson")

  NDJSON files look like this (one JSON object per line):

      {"name": "Alice", "age": 30}
      {"name": "Bob", "age": 25}

  Write an NDJSON file and read it back:

      iex> path = Path.join(System.tmp_dir!(), "dux_doctest_#{:erlang.unique_integer([:positive])}.ndjson")
      iex> Dux.from_list([%{x: 1}, %{x: 2}]) |> Dux.to_ndjson(path)
      iex> Dux.from_ndjson(path) |> Dux.to_columns()
      %{"x" => [1, 2]}
  """
  def from_ndjson(path, opts \\ []) when is_binary(path) do
    %Dux{source: {:ndjson, path, opts}}
  end

  # ---------------------------------------------------------------------------
  # IO — writing
  # ---------------------------------------------------------------------------

  @doc group: :io
  @doc """
  Write a Dux to a CSV file. Triggers computation.

  Returns `:ok` on success. The file is written atomically by DuckDB's
  `COPY ... TO` statement.

  ## Options

    * `:delimiter` - field delimiter (default: `","`)
    * `:header` - write header row (default: `true`)

  ## Examples

      Dux.from_query("SELECT * FROM range(10) t(x)")
      |> Dux.to_csv("/tmp/output.csv")

      # With custom delimiter
      Dux.from_list([%{name: "Alice", age: 30}])
      |> Dux.to_csv("/tmp/output.tsv", delimiter: "\t")
  """
  def to_csv(%Dux{} = dux, path, opts \\ []) when is_binary(path) do
    write_copy(dux, path, "CSV", opts)
  end

  @doc group: :io
  @doc """
  Write a Dux to a Parquet file. Triggers computation.

  ## Options

    * `:compression` - compression codec: `:snappy` (default), `:zstd`, `:gzip`, `:none`
    * `:row_group_size` - rows per row group

  ## Examples

      Dux.from_query("SELECT * FROM range(10) t(x)")
      |> Dux.to_parquet("/tmp/output.parquet")

      Dux.from_query("SELECT * FROM range(10) t(x)")
      |> Dux.to_parquet("/tmp/output.parquet", compression: :zstd)
  """
  def to_parquet(%Dux{} = dux, path, opts \\ []) when is_binary(path) do
    write_copy(dux, path, "PARQUET", opts)
  end

  @doc group: :io
  @doc """
  Write a Dux to a newline-delimited JSON (NDJSON) file. Triggers computation.

  Each row becomes a single JSON object on its own line. Returns `:ok` on success.

  ## Examples

      Dux.from_query("SELECT * FROM range(10) t(x)")
      |> Dux.to_ndjson("/tmp/output.ndjson")

      # The resulting file contains one JSON object per line:
      # {"x":0}
      # {"x":1}
      # ...
  """
  def to_ndjson(%Dux{} = dux, path, opts \\ []) when is_binary(path) do
    write_copy(dux, path, "JSON", opts)
  end

  # ---------------------------------------------------------------------------
  # Selection verbs
  # ---------------------------------------------------------------------------

  @doc group: :transforms
  @doc """
  Keep only the named columns.

      iex> Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
      ...> |> Dux.select([:a, :b])
      ...> |> Dux.to_rows()
      [%{"a" => 1, "b" => 2}]
  """
  def select(%Dux{ops: ops} = dux, columns) when is_list(columns) do
    cols = Enum.map(columns, &to_col_name/1)
    %{dux | ops: ops ++ [{:select, cols}]}
  end

  @doc group: :transforms
  @doc """
  Drop the named columns.

      iex> Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
      ...> |> Dux.discard([:c])
      ...> |> Dux.to_rows()
      [%{"a" => 1, "b" => 2}]
  """
  def discard(%Dux{ops: ops} = dux, columns) when is_list(columns) do
    cols = Enum.map(columns, &to_col_name/1)
    %{dux | ops: ops ++ [{:discard, cols}]}
  end

  # ---------------------------------------------------------------------------
  # Filtering verbs
  # ---------------------------------------------------------------------------

  @doc group: :transforms
  @doc """
  Filter rows matching a condition.

  This is a macro — bare identifiers become column names, `^` interpolates
  Elixir values. Requires `require Dux`.

      iex> require Dux
      iex> Dux.from_query("SELECT * FROM range(1, 6) t(x)")
      ...> |> Dux.filter(x > 3)
      ...> |> Dux.to_columns()
      %{"x" => [4, 5]}

      iex> require Dux
      iex> threshold = 7
      iex> Dux.from_query("SELECT * FROM range(1, 11) t(x)")
      ...> |> Dux.filter(x >= ^threshold)
      ...> |> Dux.to_columns()
      %{"x" => [7, 8, 9, 10]}

  For raw SQL strings, use `filter_with/2`.
  """
  defmacro filter(dux, expr) do
    if is_binary(expr) do
      quote do
        Dux.filter_with(unquote(dux), unquote(expr))
      end
    else
      {ast, pins} = Dux.Query.traverse_public(expr, [])

      quote do
        pins = unquote(Enum.reverse(pins))
        compiled = Dux.Query.Compiler.to_sql(unquote(Macro.escape(ast)), pins)
        Dux.filter_with(unquote(dux), compiled)
      end
    end
  end

  @doc group: :transforms
  @doc """
  Filter rows using a raw SQL expression string or compiled `{sql, params}`.

      iex> Dux.from_query("SELECT * FROM range(1, 6) t(x)")
      ...> |> Dux.filter_with("x > 3")
      ...> |> Dux.to_columns()
      %{"x" => [4, 5]}

      iex> Dux.from_query("SELECT * FROM range(1, 11) t(x)")
      ...> |> Dux.filter_with("x % 2 = 0")
      ...> |> Dux.to_columns()
      %{"x" => [2, 4, 6, 8, 10]}
  """
  def filter_with(%Dux{ops: ops} = dux, expr) when is_binary(expr) do
    %{dux | ops: ops ++ [{:filter, expr}]}
  end

  def filter_with(%Dux{ops: ops} = dux, {sql, params})
      when is_binary(sql) and is_list(params) do
    %{dux | ops: ops ++ [{:filter, inline_params(sql, params)}]}
  end

  @doc group: :sorting
  @doc """
  Take the first `n` rows, defaulting to 10 if not specified.

  In IEx, the result is automatically displayed via the Inspect protocol.
  Use `peek/2` for an explicit table preview.

  ## Examples

      iex> Dux.from_query("SELECT * FROM range(100) t(x)")
      ...> |> Dux.head(3)
      ...> |> Dux.to_columns()
      %{"x" => [0, 1, 2]}

      iex> Dux.from_query("SELECT * FROM range(100) t(x)")
      ...> |> Dux.head()
      ...> |> Dux.to_columns()
      ...> |> Map.get("x")
      ...> |> length()
      10
  """
  def head(dux, n \\ 10)

  def head(%Dux{ops: ops} = dux, n) when is_integer(n) and n >= 0 do
    %{dux | ops: ops ++ [{:head, n}]}
  end

  @doc group: :sorting
  @doc """
  Skip `offset` rows and take `length` rows.

      iex> Dux.from_query("SELECT * FROM range(10) t(x)")
      ...> |> Dux.slice(3, 4)
      ...> |> Dux.to_columns()
      %{"x" => [3, 4, 5, 6]}
  """
  def slice(%Dux{ops: ops} = dux, offset, length)
      when is_integer(offset) and is_integer(length) do
    %{dux | ops: ops ++ [{:slice, offset, length}]}
  end

  @doc group: :sorting
  @doc """
  Keep distinct rows, optionally by specific columns.

  Row ordering is **not** guaranteed after `distinct/1` — use `sort_by/2`
  if you need deterministic output order. When called with columns,
  which row is kept for each distinct group is also non-deterministic.

      iex> result = Dux.from_list([%{x: 1, y: "a"}, %{x: 1, y: "b"}, %{x: 2, y: "c"}])
      ...> |> Dux.distinct([:x])
      ...> |> Dux.sort_by(:x)
      ...> |> Dux.to_columns()
      iex> result["x"]
      [1, 2]

      iex> Dux.from_list([%{x: 1}, %{x: 1}, %{x: 2}])
      ...> |> Dux.distinct()
      ...> |> Dux.sort_by(:x)
      ...> |> Dux.to_columns()
      %{"x" => [1, 2]}
  """
  def distinct(%Dux{ops: ops} = dux, columns \\ nil) do
    cols = if columns, do: Enum.map(columns, &to_col_name/1), else: nil
    %{dux | ops: ops ++ [{:distinct, cols}]}
  end

  @doc group: :transforms
  @doc """
  Drop rows where any of the given columns are nil.

      iex> Dux.from_query("SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 3")
      ...> |> Dux.drop_nil([:x])
      ...> |> Dux.to_columns()
      %{"x" => [1, 3]}
  """
  def drop_nil(%Dux{ops: ops} = dux, columns) when is_list(columns) do
    cols = Enum.map(columns, &to_col_name/1)
    %{dux | ops: ops ++ [{:drop_nil, cols}]}
  end

  # ---------------------------------------------------------------------------
  # Transformation verbs
  # ---------------------------------------------------------------------------

  @doc group: :transforms
  @doc """
  Add or replace columns using expressions.

  This is a macro — bare identifiers in expressions become column names,
  `^` interpolates Elixir values. Requires `require Dux`.

      iex> require Dux
      iex> Dux.from_query("SELECT 1 AS x, 2 AS y")
      ...> |> Dux.mutate(z: x + y, w: x * 10)
      ...> |> Dux.to_rows()
      [%{"w" => 10, "x" => 1, "y" => 2, "z" => 3}]

      iex> require Dux
      iex> factor = 5
      iex> Dux.from_query("SELECT 10 AS x")
      ...> |> Dux.mutate(scaled: x * ^factor)
      ...> |> Dux.to_rows()
      [%{"scaled" => 50, "x" => 10}]

  For raw SQL strings, use `mutate_with/2`.
  """
  defmacro mutate(dux, pairs) do
    compiled_pairs = compile_keyword_exprs(pairs)

    quote do
      Dux.mutate_with(unquote(dux), unquote(compiled_pairs))
    end
  end

  @doc group: :transforms
  @doc """
  Add or replace columns using raw SQL expression strings or compiled tuples.

      iex> Dux.from_query("SELECT 1 AS x, 2 AS y")
      ...> |> Dux.mutate_with(z: "x + y", w: "x * 10")
      ...> |> Dux.to_rows()
      [%{"w" => 10, "x" => 1, "y" => 2, "z" => 3}]
  """
  def mutate_with(%Dux{ops: ops} = dux, exprs) when is_list(exprs) do
    assignments =
      Enum.map(exprs, fn {name, expr} ->
        {to_col_name(name), resolve_expr(expr)}
      end)

    %{dux | ops: ops ++ [{:mutate, assignments}]}
  end

  @doc group: :transforms
  @doc """
  Rename columns.

  Accepts a keyword list of `old_name: :new_name` or a map.

      iex> Dux.from_query("SELECT 1 AS x, 2 AS y")
      ...> |> Dux.rename(x: :a, y: :b)
      ...> |> Dux.to_rows()
      [%{"a" => 1, "b" => 2}]
  """
  def rename(%Dux{ops: ops} = dux, mapping) when is_list(mapping) or is_map(mapping) do
    pairs =
      Enum.map(mapping, fn {old, new} ->
        {to_col_name(old), to_col_name(new)}
      end)

    %{dux | ops: ops ++ [{:rename, pairs}]}
  end

  # ---------------------------------------------------------------------------
  # Sorting
  # ---------------------------------------------------------------------------

  @doc group: :sorting
  @doc """
  Sort rows by columns.

  Accepts a column name (ascending) or keyword list with `:asc`/`:desc`.

      iex> Dux.from_list([%{x: 3}, %{x: 1}, %{x: 2}])
      ...> |> Dux.sort_by(:x)
      ...> |> Dux.to_columns()
      %{"x" => [1, 2, 3]}

      iex> Dux.from_list([%{x: 3}, %{x: 1}, %{x: 2}])
      ...> |> Dux.sort_by(desc: :x)
      ...> |> Dux.to_columns()
      %{"x" => [3, 2, 1]}
  """
  def sort_by(%Dux{ops: ops} = dux, columns) do
    sort_spec = normalize_sort(columns)
    %{dux | ops: ops ++ [{:sort_by, sort_spec}]}
  end

  # ---------------------------------------------------------------------------
  # Grouping & Aggregation
  # ---------------------------------------------------------------------------

  @doc group: :aggregation
  @doc """
  Group by columns for subsequent aggregation.

      iex> require Dux
      iex> Dux.from_list([%{g: "a", v: 1}, %{g: "a", v: 2}, %{g: "b", v: 3}])
      ...> |> Dux.group_by(:g)
      ...> |> Dux.summarise(total: sum(v))
      ...> |> Dux.sort_by(:g)
      ...> |> Dux.to_rows()
      [%{"g" => "a", "total" => 3}, %{"g" => "b", "total" => 3}]
  """
  def group_by(%Dux{ops: ops} = dux, columns) do
    cols =
      columns
      |> List.wrap()
      |> Enum.map(&to_col_name/1)

    %{dux | ops: ops ++ [{:group_by, cols}]}
  end

  @doc group: :aggregation
  @doc """
  Clear any active grouping set by `group_by/2`.

  This removes all group columns so subsequent operations apply to the
  full dataframe rather than per-group. The ungroup is tracked as an
  operation in the pipeline and takes effect when compiled to SQL.

  ## Examples

      iex> df = Dux.from_list([%{g: "a", x: 1}]) |> Dux.group_by(:g) |> Dux.ungroup()
      iex> {:ungroup} in df.ops
      true
  """
  def ungroup(%Dux{ops: ops} = dux) do
    %{dux | ops: ops ++ [{:ungroup}]}
  end

  @doc group: :aggregation
  @doc """
  Aggregate grouped data using expressions.

  This is a macro — function calls like `sum(col)`, `count(col)`, `avg(col)`
  compile to DuckDB SQL aggregations. Requires `require Dux`.

      iex> require Dux
      iex> Dux.from_list([
      ...>   %{region: "US", sales: 100},
      ...>   %{region: "US", sales: 200},
      ...>   %{region: "EU", sales: 150}
      ...> ])
      ...> |> Dux.group_by(:region)
      ...> |> Dux.summarise(total: sum(sales), n: count(sales))
      ...> |> Dux.sort_by(:region)
      ...> |> Dux.to_rows()
      [%{"n" => 1, "region" => "EU", "total" => 150}, %{"n" => 2, "region" => "US", "total" => 300}]

  For raw SQL strings, use `summarise_with/2`.
  """
  defmacro summarise(dux, pairs) do
    compiled_pairs = compile_keyword_exprs(pairs)

    quote do
      Dux.summarise_with(unquote(dux), unquote(compiled_pairs))
    end
  end

  @doc group: :aggregation
  @doc """
  Aggregate grouped data using raw SQL expression strings or compiled tuples.

      iex> Dux.from_list([%{g: "a", v: 1}, %{g: "a", v: 2}, %{g: "b", v: 3}])
      ...> |> Dux.group_by(:g)
      ...> |> Dux.summarise_with(total: "SUM(v)")
      ...> |> Dux.sort_by(:g)
      ...> |> Dux.to_rows()
      [%{"g" => "a", "total" => 3}, %{"g" => "b", "total" => 3}]
  """
  def summarise_with(%Dux{ops: ops} = dux, aggs) when is_list(aggs) do
    assignments =
      Enum.map(aggs, fn {name, expr} ->
        {to_col_name(name), resolve_expr(expr)}
      end)

    %{dux | ops: ops ++ [{:summarise, assignments}]}
  end

  # ---------------------------------------------------------------------------
  # Joins
  # ---------------------------------------------------------------------------

  @doc group: :joins
  @doc """
  Join two dataframes.

  Options:
  - `:on` — column name(s) to join on (required for most join types)
  - `:how` — join type: `:inner` (default), `:left`, `:right`, `:cross`, `:anti`, `:semi`
  - `:suffix` — suffix for duplicate column names (default: `"_right"`)

      iex> left = Dux.from_list([%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}])
      iex> right = Dux.from_list([%{id: 1, score: 95}, %{id: 2, score: 87}])
      iex> left
      ...> |> Dux.join(right, on: :id)
      ...> |> Dux.sort_by(:id)
      ...> |> Dux.to_rows()
      [%{"id" => 1, "name" => "Alice", "score" => 95}, %{"id" => 2, "name" => "Bob", "score" => 87}]
  """
  def join(%Dux{ops: ops} = left, %Dux{} = right, opts \\ []) do
    how = Keyword.get(opts, :how, :inner)
    on = Keyword.get(opts, :on)
    suffix = Keyword.get(opts, :suffix, "_right")

    on_cols =
      case on do
        nil ->
          nil

        col when is_atom(col) or is_binary(col) ->
          [{to_col_name(col), to_col_name(col)}]

        cols when is_list(cols) ->
          Enum.map(cols, fn
            {left_col, right_col} -> {to_col_name(left_col), to_col_name(right_col)}
            col -> {to_col_name(col), to_col_name(col)}
          end)
      end

    %{left | ops: ops ++ [{:join, right, how, on_cols, suffix}]}
  end

  # ---------------------------------------------------------------------------
  # Reshape
  # ---------------------------------------------------------------------------

  @doc group: :reshape
  @doc """
  Pivot from long to wide format (PIVOT).

  Takes values from `values_from` column and spreads them into new columns
  named by the `names_from` column, aggregated with `agg_func`.

  ## Options

    * `:agg` — aggregation function (default: `"SUM"`)

  ## Examples

      iex> Dux.from_list([
      ...>   %{region: "US", product: "Widget", sales: 100},
      ...>   %{region: "US", product: "Gadget", sales: 200},
      ...>   %{region: "EU", product: "Widget", sales: 150}
      ...> ])
      ...> |> Dux.pivot_wider(:product, :sales)
      ...> |> Dux.sort_by(:region)
      ...> |> Dux.to_rows()
      [%{"Gadget" => nil, "Widget" => 150, "region" => "EU"}, %{"Gadget" => 200, "Widget" => 100, "region" => "US"}]
  """
  def pivot_wider(%Dux{ops: ops} = dux, names_from, values_from, opts \\ []) do
    agg = Keyword.get(opts, :agg, "SUM")
    names_col = to_col_name(names_from)
    values_col = to_col_name(values_from)
    %{dux | ops: ops ++ [{:pivot_wider, names_col, values_col, agg}]}
  end

  @doc group: :reshape
  @doc """
  Unpivot from wide to long format (UNPIVOT).

  Takes multiple columns and stacks them into two columns: one for the
  original column name and one for the value.

  ## Examples

      iex> Dux.from_list([
      ...>   %{region: "US", q1: 100, q2: 200},
      ...>   %{region: "EU", q1: 150, q2: 250}
      ...> ])
      ...> |> Dux.pivot_longer([:q1, :q2], names_to: "quarter", values_to: "sales")
      ...> |> Dux.sort_by([:region, :quarter])
      ...> |> Dux.to_rows()
      [%{"quarter" => "q1", "region" => "EU", "sales" => 150}, %{"quarter" => "q2", "region" => "EU", "sales" => 250}, %{"quarter" => "q1", "region" => "US", "sales" => 100}, %{"quarter" => "q2", "region" => "US", "sales" => 200}]
  """
  def pivot_longer(%Dux{ops: ops} = dux, columns, opts \\ []) do
    cols = Enum.map(columns, &to_col_name/1)
    names_to = Keyword.get(opts, :names_to, "name")
    values_to = Keyword.get(opts, :values_to, "value")
    %{dux | ops: ops ++ [{:pivot_longer, cols, names_to, values_to}]}
  end

  # ---------------------------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------------------------

  @doc group: :joins
  @doc """
  Concatenate rows from multiple dataframes (UNION ALL).

      iex> a = Dux.from_list([%{x: 1}])
      iex> b = Dux.from_list([%{x: 2}])
      iex> c = Dux.from_list([%{x: 3}])
      iex> Dux.concat_rows([a, b, c])
      ...> |> Dux.to_columns()
      %{"x" => [1, 2, 3]}
  """
  def concat_rows([first | rest]) do
    %{first | ops: first.ops ++ [{:concat_rows, rest}]}
  end

  # ---------------------------------------------------------------------------
  # Materialization
  # ---------------------------------------------------------------------------

  @doc group: :materialization
  @doc """
  Compile the pipeline to SQL and execute against DuckDB.

  Returns a new `%Dux{}` with `source: {:table, ref}` and empty ops.
  The ref is a NIF ResourceArc — when it's GC'd, the temp table is dropped.

  ## Options (distributed only)

    * `:broadcast_threshold` — max IPC size in bytes for broadcast joins
      (default: 256MB). Right sides larger than this trigger shuffle joins.

  ## Examples

      iex> df = Dux.from_query("SELECT 1 AS x") |> Dux.compute()
      iex> df.ops
      []
      iex> match?({:table, _}, df.source)
      true
  """
  def compute(dux, opts \\ [])

  def compute(%Dux{workers: workers, source: {:table, _}, ops: []} = dux, _opts)
      when is_list(workers) and workers != [] do
    dux
  end

  def compute(%Dux{workers: workers} = dux, opts) when is_list(workers) and workers != [] do
    meta = %{n_ops: length(dux.ops), distributed: true}

    :telemetry.span([:dux, :query], meta, fn ->
      coordinator_opts = [workers: workers] ++ Keyword.take(opts, [:broadcast_threshold])
      # credo:disable-for-next-line Credo.Check.Design.AliasUsage
      result = Dux.Remote.Coordinator.execute(dux, coordinator_opts)
      result = %{result | workers: workers}
      {:table, table_ref} = result.source
      conn = Dux.Connection.get_conn()
      {result, Map.put(meta, :n_rows, Dux.Backend.table_n_rows(conn, table_ref))}
    end)
  end

  def compute(%Dux{} = dux, _opts) do
    meta = %{n_ops: length(dux.ops), distributed: false}

    :telemetry.span([:dux, :query], meta, fn ->
      conn = Dux.Connection.get_conn()

      source_ref = extract_source_ref(dux)
      Process.put(:dux_compute_ref, source_ref)

      {sql, source_setup} = Dux.QueryBuilder.build(dux, conn)

      Enum.each(source_setup, fn setup_sql ->
        Dux.Backend.execute(conn, setup_sql)
      end)

      table_ref = Dux.Backend.query(conn, sql)
      names = Dux.Backend.table_names(conn, table_ref)
      dtypes = Dux.Backend.table_dtypes(conn, table_ref) |> Map.new()
      result = %Dux{source: {:table, table_ref}, names: names, dtypes: dtypes}

      Process.delete(:dux_compute_ref)
      {:table, table_ref} = result.source
      {result, Map.put(meta, :n_rows, Dux.Backend.table_n_rows(conn, table_ref))}
    end)
  end

  @doc group: :distribution
  @doc """
  Collect distributed results back to a local `%Dux{}`.

  For distributed pipelines, this brings results back to the calling node.
  For local pipelines, this is equivalent to `compute/1`.

  ## Examples

      workers = Dux.Remote.Worker.list()

      Dux.from_parquet("data/**/*.parquet")
      |> Dux.distribute(workers)
      |> Dux.filter(amount > 100)
      |> Dux.collect()
      # => local %Dux{} with no workers
  """
  def collect(%Dux{} = dux) do
    computed = compute(dux)
    %{computed | workers: nil}
  end

  @doc group: :materialization
  @doc """
  Compute and return results as a list of maps.

  Automatically collects from distributed if needed.

  ## Options

    * `:atom_keys` - use atom keys instead of string keys (default: `false`)

  ## Examples

      iex> Dux.from_query("SELECT 1 AS x, 'hello' AS y")
      ...> |> Dux.to_rows()
      [%{"x" => 1, "y" => "hello"}]

      iex> Dux.from_query("SELECT 1 AS x, 'hello' AS y")
      ...> |> Dux.to_rows(atom_keys: true)
      [%{x: 1, y: "hello"}]
  """
  def to_rows(%Dux{} = dux, opts \\ []) do
    computed = compute(dux)
    {:table, table_ref} = computed.source
    conn = Dux.Connection.get_conn()
    rows = Dux.Backend.table_to_rows(conn, table_ref)

    if Keyword.get(opts, :atom_keys, false) do
      Enum.map(rows, &atomize_keys/1)
    else
      rows
    end
  end

  @doc group: :materialization
  @doc """
  Compute and return results as a map of column_name => [values].

  Automatically collects from distributed if needed.

  ## Options

    * `:atom_keys` - use atom keys instead of string keys (default: `false`)

  ## Examples

      iex> Dux.from_query("SELECT * FROM range(3) t(x)")
      ...> |> Dux.to_columns()
      %{"x" => [0, 1, 2]}

      iex> Dux.from_query("SELECT * FROM range(3) t(x)")
      ...> |> Dux.to_columns(atom_keys: true)
      %{x: [0, 1, 2]}
  """
  def to_columns(%Dux{} = dux, opts \\ []) do
    computed = compute(dux)
    {:table, table_ref} = computed.source
    conn = Dux.Connection.get_conn()
    columns = Dux.Backend.table_to_columns(conn, table_ref)

    if Keyword.get(opts, :atom_keys, false) do
      atomize_keys(columns)
    else
      columns
    end
  end

  @doc group: :materialization
  @doc """
  Return the SQL that would be generated, without executing.

  ## Options

    * `:pretty` - format with indentation (default: `false`)

  ## Examples

      iex> sql = Dux.from_query("SELECT * FROM t")
      ...> |> Dux.filter_with("x > 10")
      ...> |> Dux.head(5)
      ...> |> Dux.sql_preview()
      iex> sql =~ "WHERE"
      true
      iex> sql =~ "LIMIT"
      true
  """
  def sql_preview(%Dux{} = dux, opts \\ []) do
    conn = Dux.Connection.get_conn()
    {sql, _setup} = Dux.QueryBuilder.build(dux, conn)

    if Keyword.get(opts, :pretty, false) do
      pretty_sql(sql)
    else
      sql
    end
  end

  @doc group: :materialization
  @doc """
  Return the number of rows. Triggers computation.

      iex> Dux.from_query("SELECT * FROM range(42) t(x)")
      ...> |> Dux.n_rows()
      42
  """
  def n_rows(%Dux{} = dux) do
    computed = compute(dux)
    {:table, ref} = computed.source
    conn = Dux.Connection.get_conn()
    Dux.Backend.table_n_rows(conn, ref)
  end

  # ---------------------------------------------------------------------------
  # Nx interop
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(Nx) do
    @doc group: :materialization
    @doc """
    Convert a single numeric column to an Nx tensor.

    Triggers computation. The column must have a numeric DuckDB type (integer,
    float, or decimal). Boolean columns are converted to `:u8`. Non-numeric
    columns raise `ArgumentError`.

    Requires `Nx` to be available as a dependency.

    ## Examples

        df = Dux.from_list([%{x: 1.0, y: 2.0}, %{x: 3.0, y: 4.0}])
        Dux.to_tensor(df, :x)
        #=> #Nx.Tensor<f64[2] [1.0, 3.0]>
    """
    def to_tensor(%Dux{} = dux, column) do
      col_name = to_col_name(column)
      computed = compute(dux)
      {:table, ref} = computed.source
      columns = Dux.Backend.table_to_columns(Dux.Connection.get_conn(), ref)

      values = Map.fetch!(columns, col_name)
      dtype = Map.get(computed.dtypes, col_name)

      nx_type =
        case dtype do
          {:s, n} ->
            String.to_atom("s#{n}")

          {:u, n} ->
            String.to_atom("u#{n}")

          {:f, n} ->
            String.to_atom("f#{n}")

          :boolean ->
            :u8

          {:decimal, _, _} ->
            :f64

          other ->
            raise ArgumentError, "column #{col_name} has non-numeric type: #{inspect(other)}"
        end

      Nx.tensor(values, type: nx_type)
    end
  end

  @doc group: :materialization
  @doc """
  Print a formatted preview of the data. Triggers computation.

  Shows the first `limit` rows (default 5) as a formatted table with
  a shape summary.

  ## Options

    * `:limit` - number of rows to show (default: `5`)

  Returns `:ok`.
  """
  def peek(%Dux{} = dux, opts \\ []) do
    limit = Keyword.get(opts, :limit, 5)

    computed = dux |> head(limit) |> compute()
    {:table, ref} = computed.source

    names = Dux.Backend.table_names(Dux.Connection.get_conn(), ref)
    columns = Dux.Backend.table_to_columns(Dux.Connection.get_conn(), ref)
    total_rows = n_rows(dux)

    # Calculate column widths
    col_data =
      Enum.map(names, fn name ->
        values = Map.get(columns, name, [])
        cells = Enum.map(values, &cell_to_string/1)

        width =
          max(String.length(name), cells |> Enum.map(&String.length/1) |> Enum.max(fn -> 0 end))

        width = min(width, 30)
        {name, cells, width}
      end)

    # Header
    separator =
      "+-" <> Enum.map_join(col_data, "-+-", fn {_, _, w} -> String.duplicate("-", w) end) <> "-+"

    header =
      "| " <>
        Enum.map_join(col_data, " | ", fn {name, _, w} -> String.pad_trailing(name, w) end) <>
        " |"

    # Rows
    n_preview = length(Map.get(columns, hd(names), []))

    rows =
      for i <- 0..(n_preview - 1) do
        "| " <>
          Enum.map_join(col_data, " | ", fn {_, cells, w} ->
            cell = Enum.at(cells, i, "")
            pad_cell(cell, w)
          end) <> " |"
      end

    output =
      [separator, header, separator | rows] ++
        [separator, "#{total_rows} rows × #{length(names)} columns"]

    IO.puts(Enum.join(output, "\n"))
    :ok
  end

  defp pad_cell(cell, width) do
    truncated =
      if String.length(cell) > width,
        do: String.slice(cell, 0, width - 1) <> "…",
        else: cell

    String.pad_trailing(truncated, width)
  end

  defp cell_to_string(nil), do: ""
  defp cell_to_string(v) when is_binary(v), do: v
  defp cell_to_string(v) when is_float(v), do: Float.to_string(v)
  defp cell_to_string(v), do: Kernel.inspect(v)

  # ---------------------------------------------------------------------------
  # Macro helpers (compile-time)
  # ---------------------------------------------------------------------------

  @doc false
  defmacro __using__(_opts) do
    quote do
      require Dux
    end
  end

  # Compile a keyword list where values are expressions.
  # Returns AST that evaluates to a keyword list of {name, {sql, params}}.
  @doc false
  def compile_keyword_exprs(pairs) when is_list(pairs) do
    Enum.map(pairs, fn {name, expr} ->
      if is_binary(expr) do
        # Raw SQL string — pass through
        {name, expr}
      else
        {ast, pins} = Dux.Query.traverse_public(expr, [])

        # credo:disable-for-next-line Credo.Check.Design.AliasUsage
        compiled =
          quote do
            (fn ->
               pins = unquote(Enum.reverse(pins))
               Dux.Query.Compiler.to_sql(unquote(Macro.escape(ast)), pins)
             end).()
          end

        {name, compiled}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  # Extract any NIF resource refs from the source to keep them alive
  # across function calls that reference temp tables by name.
  defp extract_source_ref(%Dux{source: {:table, ref}}), do: ref

  defp extract_source_ref(%Dux{ops: ops} = dux) do
    # Also check for join ops that hold a right-side Dux with a table ref
    join_refs =
      Enum.flat_map(ops, fn
        {:join, %Dux{source: {:table, ref}}, _, _, _} -> [ref]
        _ -> []
      end)

    case dux.source do
      {:table, ref} -> [ref | join_refs]
      _ -> join_refs
    end
  end

  # Force the compiler to keep a value alive until this point.

  defp pretty_sql(sql) do
    # Split CTEs and format each one
    case String.split(sql, "\n", trim: true) do
      ["WITH", cte_line | rest] ->
        ctes_and_final = [String.trim(cte_line) | Enum.map(rest, &String.trim/1)]

        formatted =
          Enum.map(ctes_and_final, fn line ->
            line
            |> String.replace(~r/\)\s*$/, ")")
            |> format_sql_line()
          end)

        "WITH\n" <> Enum.join(formatted, "\n")

      _ ->
        format_sql_line(sql)
    end
  end

  defp format_sql_line(line) do
    line
    |> String.replace(" FROM ", "\n    FROM ")
    |> String.replace(" WHERE ", "\n    WHERE ")
    |> String.replace(" GROUP BY ", "\n    GROUP BY ")
    |> String.replace(" ORDER BY ", "\n    ORDER BY ")
    |> String.replace(" LIMIT ", "\n    LIMIT ")
    |> String.replace(" INNER JOIN ", "\n    INNER JOIN ")
    |> String.replace(" LEFT JOIN ", "\n    LEFT JOIN ")
    |> String.replace(" USING ", "\n      USING ")
    |> String.replace(" ON ", "\n      ON ")
  end

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {String.to_atom(k), v} end)
  end

  defp to_col_name(name) when is_atom(name), do: Atom.to_string(name)
  defp to_col_name(name) when is_binary(name), do: name

  defp resolve_expr(expr) when is_binary(expr), do: expr

  defp resolve_expr({sql, params}) when is_binary(sql) and is_list(params),
    do: inline_params(sql, params)

  defp inline_params(sql, []), do: sql

  defp inline_params(sql, params) do
    params
    |> Enum.with_index(1)
    |> Enum.reduce(sql, fn {value, idx}, sql ->
      String.replace(sql, "$#{idx}", encode_param(value))
    end)
  end

  defp encode_param(v) when is_integer(v), do: Integer.to_string(v)
  defp encode_param(v) when is_float(v), do: Float.to_string(v)
  defp encode_param(v) when is_binary(v), do: "'#{String.replace(v, "'", "''")}'"
  defp encode_param(true), do: "true"
  defp encode_param(false), do: "false"
  defp encode_param(nil), do: "NULL"

  defp write_copy(%Dux{} = dux, path, format, opts) do
    fmt_atom = format |> String.downcase() |> String.to_atom()
    meta = %{format: fmt_atom, path: path}

    :telemetry.span([:dux, :io, :write], meta, fn ->
      conn = Dux.Connection.get_conn()
      Process.put(:dux_write_ref, extract_source_ref(dux))
      {query_sql, source_setup} = Dux.QueryBuilder.build(dux, conn)

      Enum.each(source_setup, fn setup_sql ->
        Dux.Backend.execute(conn, setup_sql)
      end)

      copy_opts = build_copy_options(format, opts)
      escaped_path = String.replace(path, "'", "''")
      sql = "COPY (#{query_sql}) TO '#{escaped_path}' (#{copy_opts})"

      case Adbc.Connection.query(conn, sql) do
        {:ok, _} -> :ok
        {:error, err} -> raise ArgumentError, "DuckDB write failed: #{Exception.message(err)}"
      end

      Process.delete(:dux_write_ref)
      {:ok, meta}
    end)
  end

  defp build_copy_options("CSV", opts) do
    parts = ["FORMAT CSV"]
    parts = if Keyword.get(opts, :header, true), do: parts ++ ["HEADER"], else: parts

    parts =
      case Keyword.get(opts, :delimiter) do
        nil -> parts
        d -> parts ++ ["DELIMITER '#{d}'"]
      end

    Enum.join(parts, ", ")
  end

  defp build_copy_options("PARQUET", opts) do
    parts = ["FORMAT PARQUET"]

    parts =
      case Keyword.get(opts, :compression) do
        nil -> parts
        c -> parts ++ ["COMPRESSION #{String.upcase(to_string(c))}"]
      end

    parts =
      case Keyword.get(opts, :row_group_size) do
        nil -> parts
        n -> parts ++ ["ROW_GROUP_SIZE #{n}"]
      end

    Enum.join(parts, ", ")
  end

  defp build_copy_options("JSON", _opts) do
    "FORMAT JSON"
  end

  defp normalize_sort(col) when is_atom(col), do: [{:asc, to_col_name(col)}]
  defp normalize_sort(col) when is_binary(col), do: [{:asc, col}]

  defp normalize_sort(specs) when is_list(specs) do
    Enum.map(specs, fn
      {:asc, col} -> {:asc, to_col_name(col)}
      {:desc, col} -> {:desc, to_col_name(col)}
      col when is_atom(col) -> {:asc, to_col_name(col)}
      col when is_binary(col) -> {:asc, col}
    end)
  end
end
