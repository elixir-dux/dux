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

  import Dux.SQL.Helpers, only: [qi: 1]

  defstruct [:source, :remote, :workers, :meta, ops: [], names: [], dtypes: %{}, groups: []]

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

  @doc group: :io
  @doc """
  Read an Excel file (.xlsx) into a lazy Dux pipeline.

  Uses DuckDB's `read_xlsx` function (available in DuckDB 1.5+).

  ## Options

    * `:sheet` — sheet name (default: first sheet)
    * `:range` — cell range, e.g. `"A1:F1000"` (default: auto-detect)
    * `:header` — whether the first row is a header (default: `true`)
    * `:all_varchar` — read all columns as VARCHAR (default: `false`).
      Useful when type inference fails on mixed-type columns.
    * `:ignore_errors` — replace type-cast failures with NULL instead of
      raising (default: `true`). DuckDB infers column types from the first
      data row only — if a column has NULL or a number in row 1 but strings
      later, this prevents hard failures.
    * `:empty_as_varchar` — infer empty cells as VARCHAR instead of DOUBLE
      (default: `true`)
    * `:stop_at_empty` — stop reading at the first empty row (default: `true`)

  ## Examples

      df = Dux.from_excel("sales.xlsx")
      df = Dux.from_excel("data.xlsx", sheet: "Q1 2024", range: "A1:F100")
      df = Dux.from_excel("messy.xlsx", all_varchar: true)
  """
  @excel_read_defaults [
    sheet: nil,
    range: nil,
    header: true,
    all_varchar: false,
    ignore_errors: true,
    empty_as_varchar: true,
    stop_at_empty: nil
  ]

  def from_excel(path, opts \\ []) when is_binary(path) do
    opts = Keyword.validate!(opts, @excel_read_defaults)
    escaped = String.replace(path, "'", "''")
    read_opts = excel_read_options(opts)
    %Dux{source: {:sql, "SELECT * FROM read_xlsx('#{escaped}'#{read_opts})"}}
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
    * `:partition_by` - column(s) for Hive-style partitioned output.
      Pass an atom, string, or list. Writes to a directory tree:
      `path/col=value/data_0.parquet`.

  ## Examples

      Dux.from_query("SELECT * FROM range(10) t(x)")
      |> Dux.to_parquet("/tmp/output.parquet")

      Dux.from_query("SELECT * FROM range(10) t(x)")
      |> Dux.to_parquet("/tmp/output.parquet", compression: :zstd)

      # Hive-partitioned output
      Dux.from_parquet("events.parquet")
      |> Dux.to_parquet("/tmp/events/", partition_by: [:year, :month])
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

  @doc group: :io
  @doc """
  Write a Dux to an Excel file (.xlsx). Triggers computation.

  Requires the DuckDB `excel` extension (auto-installed, needs explicit LOAD).

  ## Examples

      Dux.from_list([%{name: "Alice", age: 30}, %{name: "Bob", age: 25}])
      |> Dux.to_excel("/tmp/output.xlsx")
  """
  def to_excel(%Dux{} = dux, path) when is_binary(path) do
    write_copy(dux, path, "XLSX", [])
  end

  @doc group: :io
  @doc """
  Insert a Dux pipeline's results into a table. Triggers computation.

  The target can be any table DuckDB can write to — a local table, or a
  table in an attached database (Postgres, DuckLake, etc.). The pipeline
  is compiled to SQL and executed as `INSERT INTO target SELECT ...`.

  When workers are set (`Dux.distribute/2`), each worker ATTACHes the
  target database independently and inserts its partition in parallel.
  Per-worker transactions — not atomic across workers.

  ## Options

    * `:create` — create the target table if it doesn't exist (default: `false`).
      Uses `CREATE TABLE ... AS SELECT ...` instead of `INSERT INTO`.

  ## Examples

      # Insert into an attached Postgres table
      Dux.attach(:pg, "host=... dbname=analytics", type: :postgres, read_only: false)

      Dux.from_parquet("s3://bucket/raw/*.parquet")
      |> Dux.filter(col("status") == "active")
      |> Dux.insert_into("pg.public.users")

      # Distributed insert — each worker writes its partition to Postgres
      Dux.from_parquet("s3://input/**/*.parquet")
      |> Dux.distribute(workers)
      |> Dux.insert_into("pg.public.events", create: true)

      # Insert into a local DuckDB table
      Dux.from_query("SELECT 1 AS x")
      |> Dux.insert_into("my_table", create: true)
  """
  def insert_into(dux, table, opts \\ [])

  def insert_into(%Dux{workers: workers} = dux, table, opts)
      when is_binary(table) and is_list(workers) and workers != [] do
    distributed_insert_into(dux, table, opts)
  end

  def insert_into(%Dux{} = dux, table, opts) when is_binary(table) do
    create? = Keyword.get(opts, :create, false)
    meta = %{table: table, create: create?}

    :telemetry.span([:dux, :io, :write], meta, fn ->
      conn = Dux.Connection.get_conn()
      Process.put(:dux_write_ref, extract_source_ref(dux))
      {query_sql, source_setup} = Dux.QueryBuilder.build(dux, conn)

      Enum.each(source_setup, fn setup_sql ->
        Dux.Backend.execute(conn, setup_sql)
      end)

      sql =
        if create? do
          "CREATE TABLE #{table} AS #{query_sql}"
        else
          "INSERT INTO #{table} #{query_sql}"
        end

      case Adbc.Connection.query(conn, sql) do
        {:ok, _} -> :ok
        {:error, err} -> raise ArgumentError, "DuckDB insert failed: #{Exception.message(err)}"
      end

      Process.delete(:dux_write_ref)
      {:ok, meta}
    end)
  end

  defp distributed_insert_into(%Dux{workers: workers} = dux, table, opts) do
    alias Dux.Remote.{Partitioner, Worker}
    require Logger

    create? = Keyword.get(opts, :create, false)
    n_workers = length(workers)
    meta = %{table: table, create: create?, n_workers: n_workers, distributed: true}

    :telemetry.span([:dux, :distributed, :write], meta, fn ->
      # Resolve the target database's connection info for worker ATTACH
      setup_sqls = resolve_insert_target_setup(table)

      # Resolve source for distribution (e.g., attached with partition_by →
      # distributed_scan) then partition across workers
      pipeline = %{dux | workers: nil}
      # credo:disable-for-next-line Credo.Check.Design.AliasUsage
      pipeline = Dux.Remote.Coordinator.resolve_source(pipeline)
      assignments = Partitioner.assign(pipeline, workers)

      results = fan_out_inserts(assignments, table, setup_sqls, create?, n_workers)
      files = handle_write_results(results, n_workers, table)
      {:ok, Map.merge(meta, %{files: files, n_files: length(files)})}
    end)
  end

  # Fan out INSERT INTO to workers. If create: true, the first worker creates
  # the table sequentially before the rest start inserting in parallel.
  defp fan_out_inserts(assignments, table, setup_sqls, create?, n_workers) do
    alias Dux.Remote.Worker

    if create? do
      [{first_worker, first_pipeline} | rest] = assignments
      first = Worker.insert_into(first_worker, first_pipeline, table, setup_sqls, true)

      rest_results =
        rest
        |> Task.async_stream(
          fn {worker, wp} ->
            {worker, Worker.insert_into(worker, wp, table, setup_sqls, false)}
          end,
          max_concurrency: n_workers,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, r} -> r end)

      [{first_worker, first} | rest_results]
    else
      assignments
      |> Task.async_stream(
        fn {worker, wp} ->
          {worker, Worker.insert_into(worker, wp, table, setup_sqls, false)}
        end,
        max_concurrency: n_workers,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, r} -> r end)
    end
  end

  # Resolve the target table's database to INSTALL/LOAD/ATTACH setup SQL
  # for workers. The table name format is "db_name.schema.table" or "db_name.table".
  defp resolve_insert_target_setup(table) do
    case String.split(table, ".", parts: 2) do
      [db_name, _rest] -> build_attach_setup(db_name)
      _ -> []
    end
  end

  defp build_attach_setup(db_name) do
    conn = Dux.Connection.get_conn()
    sql = "SELECT type, path FROM duckdb_databases() WHERE database_name = '#{db_name}'"

    case Adbc.Connection.query(conn, sql) do
      {:ok, result} ->
        materialized = Adbc.Result.materialize(result)
        columns = List.flatten(materialized.data)
        attach_setup_from_columns(columns, db_name)

      {:error, _} ->
        []
    end
  end

  defp attach_setup_from_columns(columns, db_name) do
    type = extract_col_value(columns, "type")
    path = extract_col_value(columns, "path")

    if type && path && type != "duckdb" do
      escaped = String.replace(path, "'", "''")

      [
        "INSTALL #{type}; LOAD #{type};",
        "ATTACH '#{escaped}' AS #{db_name} (TYPE #{type})"
      ]
    else
      []
    end
  end

  defp extract_col_value(columns, name) do
    Enum.find_value(columns, fn col ->
      if col.field.name == name, do: hd(Adbc.Column.to_list(col))
    end)
  end

  # ---------------------------------------------------------------------------
  # Cross-source (ATTACH)
  # ---------------------------------------------------------------------------

  @doc group: :io
  @doc """
  Attach an external database to the DuckDB connection.

  DuckDB can query Postgres, MySQL, SQLite, and lakehouse formats
  (Iceberg, Delta, DuckLake) as if they were local tables. Filter
  pushdown is automatic — DuckDB sends filtered queries to the remote
  database, transferring only matching rows.

  ## Options

    * `:type` — database type (required): `:postgres`, `:mysql`, `:sqlite`,
      `:iceberg`, `:delta`, `:ducklake`, `:duckdb`
    * `:read_only` — attach as read-only (default: `true`)

  ## Examples

      Dux.attach(:warehouse, "postgresql://user:pass@host/db", type: :postgres)
      Dux.attach(:lake, "s3://bucket/iceberg-table/", type: :iceberg)
      Dux.attach(:local, "other.duckdb", type: :duckdb, read_only: false)
  """
  def attach(name, connection_string, opts \\ []) do
    type = Keyword.fetch!(opts, :type)
    read_only = Keyword.get(opts, :read_only, true)
    conn = Dux.Connection.get_conn()

    ro = if read_only, do: ", READ_ONLY", else: ""
    escaped = String.replace(connection_string, "'", "''")
    sql = "ATTACH '#{escaped}' AS #{to_string(name)} (TYPE #{type}#{ro})"

    Adbc.Connection.query!(conn, sql)
    :ok
  end

  @doc group: :io
  @doc """
  Detach a previously attached database.

  ## Examples

      Dux.detach(:warehouse)
  """
  def detach(name) do
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, "DETACH #{to_string(name)}")
    :ok
  end

  @doc group: :io
  @doc """
  List all attached databases.

  Returns a list of maps with `:name`, `:type`, and `:path` keys.

  ## Examples

      Dux.list_attached()
      #=> [%{name: "memory", type: "duckdb", path: "", read_only: false}, ...]
  """
  def list_attached do
    conn = Dux.Connection.get_conn()

    ref =
      Dux.Backend.query(conn, "SELECT database_name AS name, type, path FROM duckdb_databases()")

    Dux.Backend.table_to_rows(conn, ref)
  end

  @doc group: :io
  @doc """
  Create a lazy `%Dux{}` referencing a table in an attached database.

  The table name can include the schema (e.g., `"public.customers"`).

  ## Options

    * `:version` — snapshot/version number for time-travel (Iceberg, Delta, DuckLake)
    * `:as_of` — timestamp for time-travel
    * `:partition_by` — column to hash-partition on for distributed reads.
      When set and the pipeline is distributed, each worker ATTACHes the
      database independently and reads a disjoint hash partition. Without
      this, attached sources are read on the coordinator only.

  ## Examples

      customers = Dux.from_attached(:warehouse, "public.customers")
      events = Dux.from_attached(:lake, "default.click_events")

      # Time travel
      Dux.from_attached(:lake, "events", version: 5)

      # Distributed reads from Postgres (each worker reads 1/N)
      Dux.from_attached(:pg, "public.orders", partition_by: :id)
      |> Dux.distribute(workers)
      |> Dux.to_rows()
  """
  def from_attached(db_name, table_name, opts \\ []) do
    version = Keyword.get(opts, :version)
    as_of = Keyword.get(opts, :as_of)
    partition_by = Keyword.get(opts, :partition_by)

    source =
      cond do
        partition_by -> {:attached, db_name, table_name, partition_by: partition_by}
        version -> {:attached, db_name, table_name, version: version}
        as_of -> {:attached, db_name, table_name, as_of: as_of}
        true -> {:attached, db_name, table_name}
      end

    %Dux{source: source, ops: [], names: [], dtypes: %{}, groups: []}
  end

  @doc group: :io
  @doc """
  Create a DuckDB secret for accessing remote services.

  Wraps DuckDB's Secrets Manager. Secrets are scoped to specific
  URL prefixes so different credentials can be used for different
  buckets or databases.

  ## Options

    * `:type` — secret type (required): `:s3`, `:gcs`, `:azure`, `:postgres`, `:mysql`
    * `:key_id` — access key ID (S3/GCS)
    * `:secret` — secret access key (S3/GCS)
    * `:region` — AWS region (S3)
    * `:provider` — credential provider: `:credential_chain` (use IAM/env)
    * `:scope` — URL prefix scope (e.g., `"s3://my-bucket/"`)
    * `:host`, `:user`, `:password` — database connection options
    * `:password_env` — read password from environment variable at runtime

  ## Examples

      Dux.create_secret(:my_s3, type: :s3,
        key_id: "AKIA...",
        secret: "...",
        region: "us-east-1"
      )

      # Use IAM role / environment credentials
      Dux.create_secret(:my_s3, type: :s3, provider: :credential_chain)

      # Scoped to a specific bucket
      Dux.create_secret(:prod, type: :s3,
        scope: "s3://prod-bucket/",
        provider: :credential_chain
      )
  """
  def create_secret(name, opts) do
    type = Keyword.fetch!(opts, :type)
    conn = Dux.Connection.get_conn()

    params =
      opts
      |> Keyword.delete(:type)
      |> Keyword.delete(:password_env)
      |> Enum.map(fn {k, v} -> secret_param(k, v) end)
      |> Enum.reject(&is_nil/1)

    # Handle password from env var
    params =
      case Keyword.get(opts, :password_env) do
        nil -> params
        env_var -> params ++ ["PASSWORD '#{escape(System.get_env(env_var) || "")}'"]
      end

    type_param = "TYPE #{type}"
    all_params = [type_param | params] |> Enum.join(", ")

    Adbc.Connection.query!(conn, "CREATE SECRET #{to_string(name)} (#{all_params})")
    :ok
  end

  @doc group: :io
  @doc """
  Drop a previously created secret.

  ## Examples

      Dux.drop_secret(:my_s3)
  """
  def drop_secret(name) do
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, "DROP SECRET #{to_string(name)}")
    :ok
  end

  @doc group: :io
  @doc """
  Execute raw SQL against the DuckDB connection.

  Use this for DDL and statements that aren't queries — `SET`, `INSTALL`,
  `LOAD`, `CREATE SECRET`, `CREATE TABLE`, etc. These can't go through
  `from_query/1` because it wraps SQL in `SELECT * FROM (...)`.

  Returns `:ok` on success.

  ## Examples

      Dux.exec("INSTALL httpfs; LOAD httpfs;")
      Dux.exec("SET s3_region = 'us-east-1'")
      Dux.exec("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-east-1')")

  """
  def exec(sql) when is_binary(sql) do
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, sql)
    :ok
  end

  defp secret_param(:key_id, v), do: "KEY_ID '#{escape(v)}'"
  defp secret_param(:secret, v), do: "SECRET '#{escape(v)}'"
  defp secret_param(:region, v), do: "REGION '#{escape(v)}'"
  defp secret_param(:scope, v), do: "SCOPE '#{escape(v)}'"
  defp secret_param(:provider, :credential_chain), do: "PROVIDER credential_chain"
  defp secret_param(:provider, v), do: "PROVIDER '#{escape(to_string(v))}'"
  defp secret_param(:host, v), do: "HOST '#{escape(v)}'"
  defp secret_param(:user, v), do: "USER '#{escape(v)}'"
  defp secret_param(:password, v), do: "PASSWORD '#{escape(v)}'"
  defp secret_param(_, _), do: nil

  defp escape(str), do: String.replace(to_string(str), "'", "''")

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

  @doc group: :joins
  @doc """
  ASOF join — match each left row to the nearest right row satisfying an inequality.

  Useful for time series alignment: match each trade to the most recent quote,
  each event to the closest preceding snapshot, etc.

  ## Options

    * `:on` — equality column(s) to match on (atom, string, or list)
    * `:by` — `{column, operator}` specifying the inequality condition.
      Operators: `:>=`, `:>`, `:<=`, `:<`
    * `:how` — `:inner` (default) or `:left` (preserve unmatched left rows)
    * `:suffix` — suffix for duplicate column names (default: `"_right"`)

  ## Examples

      trades = Dux.from_list([
        %{symbol: "AAPL", timestamp: 10, price: 150.0},
        %{symbol: "AAPL", timestamp: 20, price: 152.0}
      ])
      quotes = Dux.from_list([
        %{symbol: "AAPL", timestamp: 5, bid: 149.0},
        %{symbol: "AAPL", timestamp: 15, bid: 151.0}
      ])

      # Match each trade to the most recent quote
      Dux.asof_join(trades, quotes, on: :symbol, by: {:timestamp, :>=})
      |> Dux.to_rows()
  """
  def asof_join(%Dux{ops: ops} = left, %Dux{} = right, opts) do
    on = Keyword.get(opts, :on)
    by = Keyword.fetch!(opts, :by)
    how = Keyword.get(opts, :how, :inner)
    suffix = Keyword.get(opts, :suffix, "_right")

    {by_col, by_op} = by

    unless by_op in [:>=, :>, :<=, :<] do
      raise ArgumentError,
            "asof_join :by operator must be one of :>=, :>, :<=, :< — got #{inspect(by_op)}"
    end

    on_cols =
      case on do
        nil ->
          []

        col when is_atom(col) or is_binary(col) ->
          [{to_col_name(col), to_col_name(col)}]

        cols when is_list(cols) ->
          Enum.map(cols, fn
            {l, r} -> {to_col_name(l), to_col_name(r)}
            col -> {to_col_name(col), to_col_name(col)}
          end)
      end

    by_normalized = {to_col_name(by_col), by_op}

    %{left | ops: ops ++ [{:asof_join, right, how, on_cols, by_normalized, suffix}]}
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
    Convert a single column to an Nx tensor via zero-copy from Arrow buffers.

    Triggers computation. Supports integer, float, date, time, timestamp,
    duration, and dictionary-encoded columns. Columns with nulls raise
    `ArgumentError` — filter them first. Boolean and decimal columns are not
    supported (booleans are bit-packed, decimals need conversion).

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
      conn = Dux.Connection.get_conn()
      raw_columns = Dux.Backend.table_to_raw_columns(conn, ref)

      case Map.fetch(raw_columns, col_name) do
        {:ok, %Adbc.Column{} = col} ->
          column_to_tensor(col)

        :error ->
          raise ArgumentError, "column #{inspect(col_name)} not found"
      end
    end

    @doc false
    def column_to_tensor(%Adbc.Column{} = col) do
      case nx_type_for(col.field.type) do
        nil ->
          hint =
            case col.field.type do
              :boolean ->
                ". Hint: cast to integer at the query level with mutate_with(flag: \"CAST(col AS UTINYINT)\")"

              _ ->
                ""
            end

          raise ArgumentError,
                "column #{inspect(col.field.name)} has non-numeric type: #{inspect(col.field.type)}#{hint}"

        nx_type ->
          if has_actual_nulls?(col) do
            raise ArgumentError,
                  "column #{inspect(col.field.name)} contains nulls; filter them before converting to tensor"
          end

          binary = Adbc.Column.to_binary(col)

          Nx.from_binary(binary, nx_type, backend: Nx.BinaryBackend)
          |> Nx.reshape({col.size})
      end
    end

    # Check if a column actually contains null values.
    # Adbc.Column.has_validity? only checks if a validity bitmap exists —
    # DuckDB always includes one even for non-nullable columns.
    # We scan the bitmap directly to avoid materializing the column.
    defp has_actual_nulls?(%Adbc.Column{size: 0}), do: false

    defp has_actual_nulls?(%Adbc.Column{size: size, data: %Adbc.BufferData{validity: nil}})
         when size > 0,
         do: false

    defp has_actual_nulls?(%Adbc.Column{size: size, data: %Adbc.BufferData{validity: validity}}) do
      # All-ones bitmap means no nulls. Check full bytes then remainder bits.
      full_bytes = div(size, 8)
      remainder = rem(size, 8)

      has_null_in_full =
        if full_bytes > 0 do
          :binary.part(validity, 0, full_bytes) != :binary.copy(<<255>>, full_bytes)
        else
          false
        end

      has_null_in_tail =
        if remainder > 0 do
          mask = Bitwise.bsl(1, remainder) - 1
          Bitwise.band(:binary.at(validity, full_bytes), mask) != mask
        else
          false
        end

      has_null_in_full or has_null_in_tail
    end

    defp has_actual_nulls?(%Adbc.Column{
           data: %Adbc.DictionaryData{key: key_data},
           field: %{type: {:dictionary, key_field, _}}
         }) do
      has_actual_nulls?(%Adbc.Column{field: key_field, data: key_data, size: key_data.size})
    end

    defp has_actual_nulls?(%Adbc.Column{} = col) do
      Adbc.Column.has_validity?(col)
    end

    defp nx_type_for(:s8), do: :s8
    defp nx_type_for(:s16), do: :s16
    defp nx_type_for(:s32), do: :s32
    defp nx_type_for(:s64), do: :s64
    defp nx_type_for(:u8), do: :u8
    defp nx_type_for(:u16), do: :u16
    defp nx_type_for(:u32), do: :u32
    defp nx_type_for(:u64), do: :u64
    defp nx_type_for(:f16), do: :f16
    defp nx_type_for(:f32), do: :f32
    defp nx_type_for(:f64), do: :f64
    # Date/time stored as fixed-size integers in Arrow buffers
    defp nx_type_for(:date32), do: :s32
    defp nx_type_for(:date64), do: :s64
    defp nx_type_for({:time32, _}), do: :s32
    defp nx_type_for({:time64, _}), do: :s64
    defp nx_type_for({:timestamp, _, _}), do: :s64
    defp nx_type_for({:duration, _}), do: :s64
    # Month intervals are single s32 values
    defp nx_type_for({:interval, :month}), do: :s32
    # Dictionary-encoded: use the key type
    defp nx_type_for({:dictionary, %Adbc.Field{type: key_type}, _}), do: nx_type_for(key_type)
    defp nx_type_for(_), do: nil
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

  defp write_copy(%Dux{workers: workers} = dux, path, format, opts)
       when is_list(workers) and workers != [] do
    distributed_write(dux, path, format, opts)
  end

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

      # XLSX format requires explicit extension loading
      if format == "XLSX", do: Dux.Backend.execute(conn, "INSTALL excel; LOAD excel;")

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

  defp distributed_write(%Dux{workers: workers} = dux, base_path, format, opts) do
    alias Dux.Remote.{Partitioner, Worker}
    require Logger

    fmt_atom = format |> String.downcase() |> String.to_atom()
    ext = format_extension(format)
    copy_opts = build_copy_options(format, opts)
    n_workers = length(workers)
    meta = %{format: fmt_atom, path: base_path, n_workers: n_workers}

    :telemetry.span([:dux, :distributed, :write], meta, fn ->
      # Ensure output directory exists for local paths
      ensure_output_dir(base_path)

      # Warn if output directory is non-empty
      warn_if_non_empty(base_path)

      # Partition the source across workers
      pipeline = %{dux | workers: nil}
      assignments = Partitioner.assign(pipeline, workers)

      # Fan out: each worker writes its partition to a unique file
      results = fan_out_writes(assignments, base_path, ext, copy_opts, n_workers)
      files = handle_write_results(results, n_workers, base_path)

      {:ok, Map.merge(meta, %{files: files, n_files: length(files)})}
    end)
  end

  defp fan_out_writes(assignments, base_path, ext, copy_opts, n_workers) do
    alias Dux.Remote.Worker
    partitioned? = String.contains?(copy_opts, "PARTITION_BY")

    assignments
    |> Enum.with_index()
    |> Task.async_stream(
      fn {{worker, worker_pipeline}, idx} ->
        unique = :erlang.unique_integer([:positive])

        {write_path, worker_copy_opts} =
          if partitioned? do
            # PARTITION_BY writes to a directory. Each worker gets its own
            # subdirectory to avoid races on concurrent directory creation.
            # The Hive partition dirs nest under each worker's subdir.
            # Readers use **/*.parquet to find all files across worker dirs.
            worker_dir = Path.join(base_path, "__w#{idx}")
            {worker_dir, copy_opts}
          else
            {Path.join(base_path, "part_#{idx}_#{unique}.#{ext}"), copy_opts}
          end

        {worker, Worker.write(worker, worker_pipeline, write_path, worker_copy_opts)}
      end,
      max_concurrency: n_workers,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp handle_write_results(results, n_workers, base_path) do
    require Logger

    {successes, failures} =
      Enum.split_with(results, fn {_w, result} -> match?({:ok, _}, result) end)

    if successes == [] and failures != [] do
      reasons = Enum.map(failures, fn {_w, {:error, r}} -> r end)
      raise ArgumentError, "all workers failed distributed write: #{inspect(reasons)}"
    end

    if failures != [] do
      Logger.warning(
        "#{length(failures)} of #{n_workers} workers failed during distributed write to #{base_path}"
      )
    end

    Enum.map(successes, fn {_w, {:ok, path}} -> path end)
  end

  defp ensure_output_dir(path) do
    unless String.starts_with?(path, "s3://") or String.starts_with?(path, "http") do
      File.mkdir_p(path)
    end
  end

  defp format_extension("CSV"), do: "csv"
  defp format_extension("PARQUET"), do: "parquet"
  defp format_extension("JSON"), do: "ndjson"
  defp format_extension("XLSX"), do: "xlsx"

  defp warn_if_non_empty(path) do
    require Logger

    cond do
      String.starts_with?(path, "s3://") or String.starts_with?(path, "http") ->
        # Skip check for remote paths — glob would be expensive
        :ok

      File.dir?(path) ->
        case File.ls(path) do
          {:ok, entries} when entries != [] ->
            Logger.warning(
              "distributed write target #{path} is not empty (#{length(entries)} existing entries)"
            )

          _ ->
            :ok
        end

      true ->
        :ok
    end
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

    parts =
      case Keyword.get(opts, :partition_by) do
        nil ->
          parts

        cols when is_list(cols) ->
          col_list = Enum.map_join(cols, ", ", &qi/1)
          parts ++ ["PARTITION_BY (#{col_list})"]

        col ->
          parts ++ ["PARTITION_BY (#{qi(col)})"]
      end

    Enum.join(parts, ", ")
  end

  defp build_copy_options("JSON", _opts) do
    "FORMAT JSON"
  end

  defp build_copy_options("XLSX", _opts) do
    "FORMAT XLSX, HEADER true"
  end

  defp excel_read_options(opts) do
    parts =
      [
        excel_opt(opts, :sheet, fn s -> "sheet = '#{String.replace(to_string(s), "'", "''")}'" end),
        excel_opt(opts, :range, fn r -> "range = '#{r}'" end),
        excel_opt(opts, :header, fn v -> "header = #{v}" end),
        excel_opt(opts, :all_varchar, fn
          true -> "all_varchar = true"
          _ -> nil
        end),
        excel_opt(opts, :ignore_errors, fn
          true -> "ignore_errors = true"
          _ -> nil
        end),
        excel_opt(opts, :empty_as_varchar, fn
          true -> "empty_as_varchar = true"
          _ -> nil
        end),
        excel_opt(opts, :stop_at_empty, fn v -> "stop_at_empty = #{v}" end)
      ]
      |> Enum.reject(&is_nil/1)

    if parts == [], do: "", else: ", " <> Enum.join(parts, ", ")
  end

  defp excel_opt(opts, key, formatter) do
    case Keyword.get(opts, key) do
      nil -> nil
      val -> formatter.(val)
    end
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
