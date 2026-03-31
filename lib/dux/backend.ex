defmodule Dux.Backend do
  @moduledoc false

  # Pure Elixir backend wrapping ADBC for DuckDB access.
  # Replaces the Rust NIF layer (Dux.Native).

  alias Dux.TableRef

  import Dux.SQL.Helpers, only: [qi: 1]

  # ---------------------------------------------------------------------------
  # Database lifecycle
  # ---------------------------------------------------------------------------

  @doc false
  def open(opts \\ []) do
    db_opts =
      case Keyword.get(opts, :path) do
        nil -> []
        path -> [path: path]
      end

    {:ok, db} = Adbc.Database.start_link([driver: :duckdb] ++ db_opts)
    {:ok, conn} = Adbc.Connection.start_link(database: db)
    {db, conn}
  end

  @doc false
  def execute(conn, sql) do
    case Adbc.Connection.query(conn, sql) do
      {:ok, _} -> :ok
      {:error, %Adbc.Error{} = err} -> raise ArgumentError, "DuckDB query failed: #{err.message}"
      {:error, err} -> raise ArgumentError, "DuckDB query failed: #{Exception.message(err)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Query → TableRef
  # ---------------------------------------------------------------------------

  @doc false
  def query(conn, sql) do
    # Data stays in DuckDB — no Elixir materialization.
    # GC ref via Adbc.Nif.adbc_delete_on_gc_new/2 auto-drops the table
    # when the TableRef is garbage collected (same mechanism ADBC uses
    # internally for Adbc.Connection.ingest!/2 results).
    name = "__dux_#{:erlang.unique_integer([:positive])}"

    case Adbc.Connection.query(conn, "CREATE TEMPORARY TABLE #{qi(name)} AS (#{sql})") do
      {:ok, _} ->
        :ok

      {:error, %Adbc.Error{} = err} ->
        raise ArgumentError, "DuckDB query failed: #{err.message}"

      {:error, err} ->
        raise ArgumentError, "DuckDB query failed: #{Exception.message(err)}"
    end

    gc_ref = Adbc.Nif.adbc_execute_on_gc_new(conn, "DROP TABLE IF EXISTS #{qi(name)}")
    %TableRef{name: name, gc_ref: gc_ref, node: node()}
  end

  @doc false
  def query_view(conn, sql, deps \\ []) do
    # Like query/2 but creates a VIEW — near-instant since no data is copied.
    # The deps list keeps source TableRefs alive so GC doesn't drop tables
    # that the view references.
    name = "__dux_v_#{:erlang.unique_integer([:positive])}"

    case Adbc.Connection.query(conn, "CREATE TEMPORARY VIEW #{qi(name)} AS (#{sql})") do
      {:ok, _} ->
        :ok

      {:error, %Adbc.Error{} = err} ->
        raise ArgumentError, "DuckDB query failed: #{err.message}"

      {:error, err} ->
        raise ArgumentError, "DuckDB query failed: #{Exception.message(err)}"
    end

    gc_ref = Adbc.Nif.adbc_execute_on_gc_new(conn, "DROP VIEW IF EXISTS #{qi(name)}")
    %TableRef{name: name, gc_ref: gc_ref, node: node(), deps: deps}
  end

  # ---------------------------------------------------------------------------
  # Metadata
  # ---------------------------------------------------------------------------

  @doc false
  def normalize_count(n), do: normalize_value(n)

  @doc false
  def table_names(conn, %TableRef{name: name}) do
    {names, _types} = describe_table(conn, name)
    names
  end

  @doc false
  def table_dtypes(conn, %TableRef{name: name}) do
    {names, types} = describe_table(conn, name)

    Enum.zip(names, types)
    |> Enum.map(fn {col_name, duckdb_type} ->
      {col_name, duckdb_type_string_to_dtype(duckdb_type)}
    end)
  end

  @doc false
  def table_schema(conn, %TableRef{name: name}) do
    {names, types} = describe_table(conn, name)

    dtypes =
      Enum.zip(names, types)
      |> Enum.map(fn {col_name, duckdb_type} ->
        {col_name, duckdb_type_string_to_dtype(duckdb_type)}
      end)
      |> Map.new()

    {names, dtypes}
  end

  @doc false
  def table_n_rows(conn, %TableRef{name: name}) do
    result = Adbc.Connection.query!(conn, "SELECT count(*) AS n FROM #{qi(name)}")
    %{"n" => [n]} = Adbc.Result.to_map(result)
    normalize_value(n)
  end

  # Query DESCRIBE to get column names and DuckDB type strings.
  # ADBC returns empty column lists for LIMIT 0 queries, so DESCRIBE
  # is the reliable way to get schema info.
  defp describe_table(conn, name) do
    result = Adbc.Connection.query!(conn, "DESCRIBE #{qi(name)}")
    map = Adbc.Result.to_map(result)
    {map["column_name"] || [], map["column_type"] || []}
  end

  # ---------------------------------------------------------------------------
  # Data extraction
  # ---------------------------------------------------------------------------

  @doc false
  def table_to_columns(conn, %TableRef{} = ref) do
    result = Adbc.Connection.query!(conn, "SELECT * FROM #{qi(ref.name)}")
    map = Adbc.Result.to_map(result)

    if map == %{} do
      # Empty result — ADBC strips columns. Recover schema from DESCRIBE.
      names = table_names(conn, ref)
      Map.new(names, fn name -> {name, []} end)
    else
      Map.new(map, fn {k, vs} -> {k, maybe_normalize(vs)} end)
    end
  end

  @doc false
  def table_to_raw_columns(conn, %TableRef{} = ref) do
    result = Adbc.Connection.query!(conn, "SELECT * FROM #{qi(ref.name)}")
    materialized = Adbc.Result.materialize(result)

    case flatten_batches(materialized.data) do
      [] -> %{}
      :multi_batch -> table_to_raw_columns_multi(materialized.data)
      columns -> Map.new(columns, fn col -> {col.field.name, col} end)
    end
  end

  # Multi-batch: concatenate column buffers across batches by materializing to lists
  # and rebuilding. Rare path — only for very large results.
  defp table_to_raw_columns_multi(batches) do
    batches
    |> Enum.zip_with(fn columns_at_pos ->
      [first | _] = columns_at_pos
      name = first.field.name
      values = Enum.flat_map(columns_at_pos, &Adbc.Column.to_list/1)
      {name, Adbc.Column.new(values, name: name)}
    end)
    |> Map.new()
  end

  @doc false
  def table_to_rows(conn, %TableRef{} = ref) do
    result = Adbc.Connection.query!(conn, "SELECT * FROM #{qi(ref.name)}")
    map = Adbc.Result.to_map(result)

    if map == %{} do
      []
    else
      build_rows_from_map(map)
    end
  end

  defp build_rows_from_map(map) do
    col_names = Map.keys(map)

    columns =
      Enum.map(col_names, fn col ->
        maybe_normalize(Map.fetch!(map, col))
      end)

    Enum.zip_with(columns, fn values ->
      Enum.zip(col_names, values) |> Map.new()
    end)
  end

  # Skip normalize_value for columns that don't contain Decimals.
  # ADBC returns Decimal structs for DuckDB aggregation results (SUM, COUNT)
  # and DECIMAL types, but most columns (integers, floats, strings) don't
  # need normalization. Checking the first non-nil value avoids iterating
  # 100K+ values through normalize_value/1 when it's a no-op.
  defp maybe_normalize([]), do: []

  defp maybe_normalize(values) do
    if needs_normalize?(values) do
      Enum.map(values, &normalize_value/1)
    else
      values
    end
  end

  defp needs_normalize?([%Decimal{} | _]), do: true
  defp needs_normalize?([nil | rest]), do: needs_normalize?(rest)
  defp needs_normalize?(_), do: false

  # ---------------------------------------------------------------------------
  # Arrow IPC serialization (for distribution)
  # ---------------------------------------------------------------------------

  @doc false
  def table_to_ipc(conn, %TableRef{} = ref) do
    # Use query_pointer to serialize directly to IPC without materializing
    # into Elixir memory. Data goes DuckDB → IPC binary, zero Elixir heap.
    # ADBC 0.11+ handles empty results correctly (preserves schema in IPC).
    {:ok, ipc} =
      Adbc.Connection.query_pointer(conn, "SELECT * FROM #{qi(ref.name)}", fn stream ->
        Adbc.StreamResult.to_ipc_stream(stream)
      end)

    ipc
  end

  @doc false
  def table_from_ipc(conn, <<0::32>>) do
    # Empty sentinel — no columns
    name = "__dux_#{:erlang.unique_integer([:positive])}"
    Adbc.Connection.query!(conn, "CREATE TEMPORARY TABLE #{qi(name)} AS SELECT 1 WHERE false")
    gc_ref = Adbc.Nif.adbc_execute_on_gc_new(conn, "DROP TABLE IF EXISTS #{qi(name)}")
    %TableRef{name: name, gc_ref: gc_ref, node: node()}
  end

  def table_from_ipc(conn, <<"DUX_EMPTY"::binary, ipc::binary>>) do
    # Empty table with schema — ingest the dummy row via zero-copy stream, then delete it
    stream = Adbc.StreamResult.from_ipc_stream!(ipc)
    ingest_result = Adbc.Connection.ingest!(conn, stream)
    # Delete the dummy row
    Adbc.Connection.query!(conn, "DELETE FROM #{qi(ingest_result.table)} WHERE true")

    %TableRef{
      name: ingest_result.table,
      gc_ref: ingest_result,
      node: node()
    }
  end

  def table_from_ipc(conn, binary) when is_binary(binary) do
    # Zero-copy path: load IPC stream and ingest directly without materializing.
    # Falls back to materialized path if ingest fails (e.g. special column names).
    stream = Adbc.StreamResult.from_ipc_stream!(binary)
    ingest_result = Adbc.Connection.ingest!(conn, stream)

    %TableRef{
      name: ingest_result.table,
      gc_ref: ingest_result,
      node: node()
    }
  rescue
    _ ->
      # ADBC ingest doesn't quote column names in DDL, so special chars fail.
      # Fall back to materialize + safe-rename ingest.
      result = Adbc.Result.from_ipc_stream!(binary)
      materialized = Adbc.Result.materialize(result)
      columns = flatten_batches(materialized.data)

      if columns in [[], :multi_batch] do
        name = "__dux_#{:erlang.unique_integer([:positive])}"

        Adbc.Connection.query!(
          conn,
          "CREATE TEMPORARY TABLE #{qi(name)} AS SELECT 1 WHERE false"
        )

        gc_ref = Adbc.Nif.adbc_execute_on_gc_new(conn, "DROP TABLE IF EXISTS #{qi(name)}")
        %TableRef{name: name, gc_ref: gc_ref, node: node()}
      else
        ingest_result = ingest_safe(conn, columns)
        %TableRef{name: ingest_result.table, gc_ref: ingest_result, node: node()}
      end
  end

  @sql_reserved ~w(
    add all alter and as between by case check column constraint create cross
    database default delete desc distinct drop else end except exists false
    fetch first for foreign from full group having if in index inner insert
    into is join key left like limit natural not null offset on or order outer
    primary references right select set table then to true union unique update
    using values view when where with
  )

  @doc false
  def sql_reserved_words, do: @sql_reserved

  # Ingest data that has special column names by renaming to safe names,
  # ingesting, then renaming back via CREATE TABLE AS SELECT.
  defp ingest_safe(conn, columns) do
    safe_columns =
      columns
      |> Enum.with_index()
      |> Enum.map(fn {col, i} ->
        %{col | field: %{col.field | name: "__col_#{i}"}}
      end)

    ingest_result = Adbc.Connection.ingest!(conn, safe_columns)

    original_names = Enum.map(columns, & &1.field.name)

    select_cols =
      original_names
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {orig, i} -> "\"__col_#{i}\" AS #{qi(orig)}" end)

    final_name = "__dux_#{:erlang.unique_integer([:positive])}"

    Adbc.Connection.query!(
      conn,
      "CREATE TEMPORARY TABLE #{qi(final_name)} AS SELECT #{select_cols} FROM #{qi(ingest_result.table)}"
    )

    %Adbc.IngestResult{
      ref: ingest_result.ref,
      table: final_name,
      num_rows: ingest_result.num_rows
    }
  end

  # ---------------------------------------------------------------------------
  # Batch helpers
  # ---------------------------------------------------------------------------

  # ADBC results contain a list of record batches (each a list of columns).
  # For single-batch results (common), return the batch directly.
  # For empty or multi-batch results, return [] (callers handle this via SQL fallback).
  defp flatten_batches([single_batch]) when is_list(single_batch), do: single_batch
  defp flatten_batches([_ | _]), do: :multi_batch
  defp flatten_batches([]), do: []

  # ---------------------------------------------------------------------------
  # Value normalization
  # ---------------------------------------------------------------------------

  # ADBC returns Decimal structs for DuckDB integer aggregations (SUM, COUNT)
  # and some numeric types. Normalize to plain Elixir types.
  defp normalize_value(%Decimal{} = d) do
    if Decimal.integer?(d) do
      Decimal.to_integer(d)
    else
      Decimal.to_float(d)
    end
  end

  defp normalize_value(%Date{} = d), do: d
  defp normalize_value(%Time{} = t), do: t
  defp normalize_value(%NaiveDateTime{} = dt), do: dt
  defp normalize_value(%DateTime{} = dt), do: dt
  defp normalize_value(v), do: v

  # ---------------------------------------------------------------------------
  # Type mapping: DuckDB SQL strings → Dux dtype atoms
  # ---------------------------------------------------------------------------

  defp duckdb_type_string_to_dtype("TINYINT"), do: {:s, 8}
  defp duckdb_type_string_to_dtype("SMALLINT"), do: {:s, 16}
  defp duckdb_type_string_to_dtype("INTEGER"), do: {:s, 32}
  defp duckdb_type_string_to_dtype("BIGINT"), do: {:s, 64}
  defp duckdb_type_string_to_dtype("HUGEINT"), do: {:s, 128}
  defp duckdb_type_string_to_dtype("UTINYINT"), do: {:u, 8}
  defp duckdb_type_string_to_dtype("USMALLINT"), do: {:u, 16}
  defp duckdb_type_string_to_dtype("UINTEGER"), do: {:u, 32}
  defp duckdb_type_string_to_dtype("UBIGINT"), do: {:u, 64}
  defp duckdb_type_string_to_dtype("FLOAT"), do: {:f, 32}
  defp duckdb_type_string_to_dtype("DOUBLE"), do: {:f, 64}
  defp duckdb_type_string_to_dtype("BOOLEAN"), do: :boolean
  defp duckdb_type_string_to_dtype("VARCHAR"), do: :string
  defp duckdb_type_string_to_dtype("BLOB"), do: :binary
  defp duckdb_type_string_to_dtype("DATE"), do: :date
  defp duckdb_type_string_to_dtype("TIME"), do: :time
  defp duckdb_type_string_to_dtype("TIMESTAMP"), do: {:naive_datetime, :microsecond}

  defp duckdb_type_string_to_dtype("TIMESTAMP WITH TIME ZONE"),
    do: {:datetime, :microsecond, "UTC"}

  defp duckdb_type_string_to_dtype("INTERVAL"), do: {:duration, :microsecond}

  defp duckdb_type_string_to_dtype("DECIMAL" <> rest) do
    case Regex.run(~r/\((\d+),\s*(\d+)\)/, rest) do
      [_, p, s] -> {:decimal, String.to_integer(p), String.to_integer(s)}
      _ -> {:decimal, 18, 3}
    end
  end

  defp duckdb_type_string_to_dtype(other) do
    if String.ends_with?(other, "[]") do
      inner = String.slice(other, 0..-3//1)
      {:list, duckdb_type_string_to_dtype(inner)}
    else
      {:unknown, other}
    end
  end

  # Map ADBC column types (from result.data) to Dux dtype atoms.
  # Used for IPC deserialization where we get ADBC types, not SQL strings.
  @doc false
  def adbc_type_to_dtype(:s8), do: {:s, 8}
  def adbc_type_to_dtype(:s16), do: {:s, 16}
  def adbc_type_to_dtype(:s32), do: {:s, 32}
  def adbc_type_to_dtype(:s64), do: {:s, 64}
  def adbc_type_to_dtype(:u8), do: {:u, 8}
  def adbc_type_to_dtype(:u16), do: {:u, 16}
  def adbc_type_to_dtype(:u32), do: {:u, 32}
  def adbc_type_to_dtype(:u64), do: {:u, 64}
  def adbc_type_to_dtype(:f16), do: {:f, 16}
  def adbc_type_to_dtype(:f32), do: {:f, 32}
  def adbc_type_to_dtype(:f64), do: {:f, 64}
  def adbc_type_to_dtype(:boolean), do: :boolean
  def adbc_type_to_dtype(:string), do: :string
  def adbc_type_to_dtype(:large_string), do: :string
  def adbc_type_to_dtype(:binary), do: :binary
  def adbc_type_to_dtype(:large_binary), do: :binary
  def adbc_type_to_dtype(:date32), do: :date
  def adbc_type_to_dtype(:date64), do: :date
  def adbc_type_to_dtype(:time32), do: :time
  def adbc_type_to_dtype(:time64), do: :time
  def adbc_type_to_dtype(:null), do: :null
  def adbc_type_to_dtype({:decimal, precision, scale}), do: {:decimal, precision, scale}
  def adbc_type_to_dtype({:timestamp, unit, nil}), do: {:naive_datetime, unit}
  def adbc_type_to_dtype({:timestamp, unit, tz}), do: {:datetime, unit, tz}
  def adbc_type_to_dtype({:duration, unit}), do: {:duration, unit}
  def adbc_type_to_dtype({:list, inner}), do: {:list, adbc_type_to_dtype(inner)}

  def adbc_type_to_dtype({:struct, fields}) do
    {:struct, Map.new(fields, fn {name, type} -> {name, adbc_type_to_dtype(type)} end)}
  end

  def adbc_type_to_dtype({:dictionary, _index_type, value_type}) do
    adbc_type_to_dtype(value_type)
  end

  def adbc_type_to_dtype(other), do: {:unknown, inspect(other)}
end
