defmodule Dux do
  @moduledoc """
  DuckDB-native dataframe library for Elixir.

  The `Dux` module IS the dataframe. All operations are verbs on `%Dux{}` structs.
  Pipelines are lazy — operations accumulate until `compute/1` compiles them to
  SQL CTEs and executes against DuckDB.

  ## Creating data

      iex> df = Dux.from_list([%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}])
      iex> Dux.collect(df)
      [%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}]

  ## Piping through verbs

      iex> Dux.from_query("SELECT * FROM range(1, 6) t(x)")
      ...> |> Dux.filter("x > 2")
      ...> |> Dux.mutate(doubled: "x * 2")
      ...> |> Dux.to_columns()
      %{"doubled" => [6, 8, 10], "x" => [3, 4, 5]}

  ## Lazy by default

  Operations accumulate — nothing hits DuckDB until you call `compute/1`,
  `collect/1`, or `to_columns/1`. This lets DuckDB optimize the full pipeline.

      iex> df = Dux.from_query("SELECT 1 AS x")
      ...> |> Dux.filter("x > 0")
      ...> |> Dux.mutate(y: "x + 1")
      iex> is_list(df.ops)
      true
      iex> length(df.ops)
      2
  """

  defstruct [:source, ops: [], names: [], dtypes: %{}, groups: []]

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

  @doc """
  Create a Dux from a raw SQL query.

  This is the most flexible constructor — anything DuckDB can query, you can use.

      iex> df = Dux.from_query("SELECT 1 AS x, 2 AS y")
      iex> Dux.collect(df)
      [%{"x" => 1, "y" => 2}]

      iex> df = Dux.from_query("SELECT * FROM range(3) t(n)")
      iex> Dux.to_columns(df)
      %{"n" => [0, 1, 2]}
  """
  def from_query(sql) when is_binary(sql) do
    %Dux{source: {:sql, sql}}
  end

  @doc """
  Create a Dux from a list of maps.

  Each map is a row. Keys become column names.

      iex> df = Dux.from_list([%{"name" => "Alice", "age" => 30}, %{"name" => "Bob", "age" => 25}])
      iex> Dux.to_columns(df)
      %{"age" => [30, 25], "name" => ["Alice", "Bob"]}
  """
  def from_list(rows) when is_list(rows) do
    %Dux{source: {:list, rows}}
  end

  # ---------------------------------------------------------------------------
  # Selection verbs
  # ---------------------------------------------------------------------------

  @doc """
  Keep only the named columns.

      iex> Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
      ...> |> Dux.select([:a, :b])
      ...> |> Dux.collect()
      [%{"a" => 1, "b" => 2}]
  """
  def select(%Dux{ops: ops} = dux, columns) when is_list(columns) do
    cols = Enum.map(columns, &to_col_name/1)
    %{dux | ops: ops ++ [{:select, cols}]}
  end

  @doc """
  Drop the named columns.

      iex> Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
      ...> |> Dux.discard([:c])
      ...> |> Dux.collect()
      [%{"a" => 1, "b" => 2}]
  """
  def discard(%Dux{ops: ops} = dux, columns) when is_list(columns) do
    cols = Enum.map(columns, &to_col_name/1)
    %{dux | ops: ops ++ [{:discard, cols}]}
  end

  # ---------------------------------------------------------------------------
  # Filtering verbs
  # ---------------------------------------------------------------------------

  @doc """
  Filter rows matching a condition.

  Accepts a SQL expression string. The Query macro (brief 1c) will add
  Elixir expression syntax.

      iex> Dux.from_query("SELECT * FROM range(1, 6) t(x)")
      ...> |> Dux.filter("x > 3")
      ...> |> Dux.to_columns()
      %{"x" => [4, 5]}

      iex> Dux.from_query("SELECT * FROM range(1, 11) t(x)")
      ...> |> Dux.filter("x % 2 = 0")
      ...> |> Dux.to_columns()
      %{"x" => [2, 4, 6, 8, 10]}
  """
  def filter(%Dux{ops: ops} = dux, expr) when is_binary(expr) do
    %{dux | ops: ops ++ [{:filter, expr}]}
  end

  @doc """
  Take the first `n` rows.

      iex> Dux.from_query("SELECT * FROM range(100) t(x)")
      ...> |> Dux.head(3)
      ...> |> Dux.to_columns()
      %{"x" => [0, 1, 2]}
  """
  def head(%Dux{ops: ops} = dux, n) when is_integer(n) and n >= 0 do
    %{dux | ops: ops ++ [{:head, n}]}
  end

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

  @doc """
  Keep distinct rows, optionally by specific columns.

      iex> result = Dux.from_list([%{"x" => 1, "y" => "a"}, %{"x" => 1, "y" => "b"}, %{"x" => 2, "y" => "c"}])
      ...> |> Dux.distinct([:x])
      ...> |> Dux.to_columns()
      iex> result["x"]
      [1, 2]

      iex> Dux.from_list([%{"x" => 1}, %{"x" => 1}, %{"x" => 2}])
      ...> |> Dux.distinct()
      ...> |> Dux.to_columns()
      %{"x" => [1, 2]}
  """
  def distinct(%Dux{ops: ops} = dux, columns \\ nil) do
    cols = if columns, do: Enum.map(columns, &to_col_name/1), else: nil
    %{dux | ops: ops ++ [{:distinct, cols}]}
  end

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

  @doc """
  Add or replace columns using SQL expressions.

  Accepts a keyword list of `column_name: "sql_expression"`.

      iex> Dux.from_query("SELECT 1 AS x, 2 AS y")
      ...> |> Dux.mutate(z: "x + y", w: "x * 10")
      ...> |> Dux.collect()
      [%{"w" => 10, "x" => 1, "y" => 2, "z" => 3}]
  """
  def mutate(%Dux{ops: ops} = dux, exprs) when is_list(exprs) do
    assignments =
      Enum.map(exprs, fn {name, expr} ->
        {to_col_name(name), expr}
      end)

    %{dux | ops: ops ++ [{:mutate, assignments}]}
  end

  @doc """
  Rename columns.

  Accepts a keyword list of `old_name: :new_name` or a map.

      iex> Dux.from_query("SELECT 1 AS x, 2 AS y")
      ...> |> Dux.rename(x: :a, y: :b)
      ...> |> Dux.collect()
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

  @doc """
  Sort rows by columns.

  Accepts a column name (ascending) or keyword list with `:asc`/`:desc`.

      iex> Dux.from_list([%{"x" => 3}, %{"x" => 1}, %{"x" => 2}])
      ...> |> Dux.sort_by(:x)
      ...> |> Dux.to_columns()
      %{"x" => [1, 2, 3]}

      iex> Dux.from_list([%{"x" => 3}, %{"x" => 1}, %{"x" => 2}])
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

  @doc """
  Group by columns for subsequent aggregation.

      iex> Dux.from_list([%{"g" => "a", "v" => 1}, %{"g" => "a", "v" => 2}, %{"g" => "b", "v" => 3}])
      ...> |> Dux.group_by(:g)
      ...> |> Dux.summarise(total: "SUM(v)")
      ...> |> Dux.sort_by(:g)
      ...> |> Dux.collect()
      [%{"g" => "a", "total" => 3}, %{"g" => "b", "total" => 3}]
  """
  def group_by(%Dux{ops: ops} = dux, columns) do
    cols =
      columns
      |> List.wrap()
      |> Enum.map(&to_col_name/1)

    %{dux | ops: ops ++ [{:group_by, cols}]}
  end

  @doc """
  Clear any active grouping.
  """
  def ungroup(%Dux{ops: ops} = dux) do
    %{dux | ops: ops ++ [{:ungroup}]}
  end

  @doc """
  Aggregate grouped data using SQL expressions.

  Requires a prior `group_by/2`.

      iex> Dux.from_list([
      ...>   %{"region" => "US", "sales" => 100},
      ...>   %{"region" => "US", "sales" => 200},
      ...>   %{"region" => "EU", "sales" => 150}
      ...> ])
      ...> |> Dux.group_by(:region)
      ...> |> Dux.summarise(total: "SUM(sales)", n: "COUNT(*)")
      ...> |> Dux.sort_by(:region)
      ...> |> Dux.collect()
      [%{"n" => 1, "region" => "EU", "total" => 150}, %{"n" => 2, "region" => "US", "total" => 300}]
  """
  def summarise(%Dux{ops: ops} = dux, aggs) when is_list(aggs) do
    assignments =
      Enum.map(aggs, fn {name, expr} ->
        {to_col_name(name), expr}
      end)

    %{dux | ops: ops ++ [{:summarise, assignments}]}
  end

  # ---------------------------------------------------------------------------
  # Joins
  # ---------------------------------------------------------------------------

  @doc """
  Join two dataframes.

  Options:
  - `:on` — column name(s) to join on (required for most join types)
  - `:how` — join type: `:inner` (default), `:left`, `:right`, `:cross`, `:anti`, `:semi`
  - `:suffix` — suffix for duplicate column names (default: `"_right"`)

      iex> left = Dux.from_list([%{"id" => 1, "name" => "Alice"}, %{"id" => 2, "name" => "Bob"}])
      iex> right = Dux.from_list([%{"id" => 1, "score" => 95}, %{"id" => 2, "score" => 87}])
      iex> left
      ...> |> Dux.join(right, on: :id)
      ...> |> Dux.sort_by(:id)
      ...> |> Dux.collect()
      [%{"id" => 1, "name" => "Alice", "score" => 95}, %{"id" => 2, "name" => "Bob", "score" => 87}]
  """
  def join(%Dux{ops: ops} = left, %Dux{} = right, opts \\ []) do
    how = Keyword.get(opts, :how, :inner)
    on = Keyword.get(opts, :on)
    suffix = Keyword.get(opts, :suffix, "_right")

    on_cols =
      case on do
        nil -> nil
        col when is_atom(col) or is_binary(col) -> [to_col_name(col)]
        cols when is_list(cols) -> Enum.map(cols, &to_col_name/1)
      end

    %{left | ops: ops ++ [{:join, right, how, on_cols, suffix}]}
  end

  # ---------------------------------------------------------------------------
  # Concatenation
  # ---------------------------------------------------------------------------

  @doc """
  Concatenate rows from multiple dataframes (UNION ALL).

      iex> a = Dux.from_list([%{"x" => 1}])
      iex> b = Dux.from_list([%{"x" => 2}])
      iex> c = Dux.from_list([%{"x" => 3}])
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

  @doc """
  Compile the pipeline to SQL and execute against DuckDB.

  Returns a new `%Dux{}` with `source: {:table, ref}` and empty ops.
  The ref is a NIF ResourceArc — when it's GC'd, the temp table is dropped.

      iex> df = Dux.from_query("SELECT 1 AS x") |> Dux.compute()
      iex> df.ops
      []
      iex> match?({:table, _}, df.source)
      true
  """
  def compute(%Dux{} = dux) do
    db = Dux.Connection.get_db()
    {sql, source_setup} = Dux.QueryBuilder.build(dux, db)

    Enum.each(source_setup, fn setup_sql ->
      Dux.Native.db_execute(db, setup_sql)
    end)

    table_ref = Dux.Native.df_query(db, sql)
    names = Dux.Native.table_names(table_ref)
    dtypes = table_ref |> Dux.Native.table_dtypes() |> Map.new()

    %Dux{source: {:table, table_ref}, names: names, dtypes: dtypes}
  end

  @doc """
  Compute and return results as a list of maps.

      iex> Dux.from_query("SELECT 1 AS x, 'hello' AS y")
      ...> |> Dux.collect()
      [%{"x" => 1, "y" => "hello"}]
  """
  def collect(%Dux{} = dux) do
    computed = compute(dux)
    {:table, ref} = computed.source
    Dux.Native.table_to_rows(ref)
  end

  @doc """
  Compute and return results as a map of column_name => [values].

      iex> Dux.from_query("SELECT * FROM range(3) t(x)")
      ...> |> Dux.to_columns()
      %{"x" => [0, 1, 2]}
  """
  def to_columns(%Dux{} = dux) do
    computed = compute(dux)
    {:table, ref} = computed.source
    Dux.Native.table_to_columns(ref)
  end

  @doc """
  Return the SQL that would be generated, without executing.

      iex> sql = Dux.from_query("SELECT * FROM t")
      ...> |> Dux.filter("x > 10")
      ...> |> Dux.head(5)
      ...> |> Dux.sql_preview()
      iex> sql =~ "WHERE"
      true
      iex> sql =~ "LIMIT"
      true
  """
  def sql_preview(%Dux{} = dux) do
    db = Dux.Connection.get_db()
    {sql, _setup} = Dux.QueryBuilder.build(dux, db)
    sql
  end

  @doc """
  Return the number of rows. Triggers computation.

      iex> Dux.from_query("SELECT * FROM range(42) t(x)")
      ...> |> Dux.n_rows()
      42
  """
  def n_rows(%Dux{} = dux) do
    computed = compute(dux)
    {:table, ref} = computed.source
    Dux.Native.table_n_rows(ref)
  end

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  defp to_col_name(name) when is_atom(name), do: Atom.to_string(name)
  defp to_col_name(name) when is_binary(name), do: name

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
