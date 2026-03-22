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
    driver_opts =
      case Keyword.get(opts, :path) do
        nil -> []
        path -> [path: path]
      end

    {:ok, db} = Adbc.Database.start_link(driver: :duckdb, process_options: driver_opts)
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
    result =
      case Adbc.Connection.query(conn, sql) do
        {:ok, r} -> r
        {:error, %Adbc.Error{} = err} -> raise ArgumentError, "DuckDB query failed: #{err.message}"
        {:error, err} -> raise ArgumentError, "DuckDB query failed: #{Exception.message(err)}"
      end

    materialized = Adbc.Result.materialize(result)

    # ADBC returns empty data list for 0-row results. Can't ingest empty columns.
    # Instead, create the temp table via SQL (CREATE ... AS SELECT ... WHERE false)
    # to preserve the schema.
    if materialized.data == [] do
      create_table_from_sql(conn, sql)
    else
      # ADBC ingest doesn't quote column names, so columns with spaces/special
      # chars fail. Fall back to CREATE TABLE AS for those cases.
      has_special_names =
        Enum.any?(materialized.data, fn col ->
          name = col.field.name
          name != String.replace(name, ~r/[^a-zA-Z0-9_]/, "")
        end)

      if has_special_names do
        create_table_with_data(conn, sql)
      else
        ingest_result = Adbc.Connection.ingest!(conn, materialized.data)

        %TableRef{
          name: ingest_result.table,
          gc_ref: ingest_result,
          node: node()
        }
      end
    end
  end

  # Create a temp table from SQL, preserving schema but no data.
  defp create_table_from_sql(conn, sql) do
    name = "__dux_#{:erlang.unique_integer([:positive])}"

    Adbc.Connection.query!(
      conn,
      "CREATE TEMPORARY TABLE #{qi(name)} AS SELECT * FROM (#{sql}) __src WHERE false"
    )

    %TableRef{name: name, gc_ref: nil, node: node()}
  end

  # Create a temp table from SQL, preserving both schema and data.
  # Used when column names contain special characters that break ADBC ingest.
  defp create_table_with_data(conn, sql) do
    name = "__dux_#{:erlang.unique_integer([:positive])}"

    Adbc.Connection.query!(
      conn,
      "CREATE TEMPORARY TABLE #{qi(name)} AS SELECT * FROM (#{sql}) __src"
    )

    %TableRef{name: name, gc_ref: nil, node: node()}
  end

  # ---------------------------------------------------------------------------
  # Metadata
  # ---------------------------------------------------------------------------

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
      Map.new(map, fn {k, vs} -> {k, Enum.map(vs, &normalize_value/1)} end)
    end
  end

  @doc false
  def table_to_rows(conn, %TableRef{} = ref) do
    result = Adbc.Connection.query!(conn, "SELECT * FROM #{qi(ref.name)}")
    map = Adbc.Result.to_map(result)

    if map == %{} do
      []
    else
      col_names = Map.keys(map)
      values = Map.new(map, fn {k, vs} -> {k, Enum.map(vs, &normalize_value/1)} end)
      n = values |> Map.values() |> hd() |> length()

      for i <- 0..(n - 1) do
        Map.new(col_names, fn col ->
          {col, Enum.at(Map.fetch!(values, col), i)}
        end)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Arrow IPC serialization (for distribution)
  # ---------------------------------------------------------------------------

  @doc false
  def table_to_ipc(conn, %TableRef{name: name}) do
    result = Adbc.Connection.query!(conn, "SELECT * FROM #{qi(name)}")
    materialized = Adbc.Result.materialize(result)
    Adbc.Result.to_ipc_stream(materialized)
  end

  @doc false
  def table_from_ipc(conn, binary) when is_binary(binary) do
    result = Adbc.Result.from_ipc_stream!(binary)
    materialized = Adbc.Result.materialize(result)
    ingest_result = Adbc.Connection.ingest!(conn, materialized.data)

    %TableRef{
      name: ingest_result.table,
      gc_ref: ingest_result,
      node: node()
    }
  end

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
  defp duckdb_type_string_to_dtype("TIMESTAMP WITH TIME ZONE"), do: {:datetime, :microsecond, "UTC"}
  defp duckdb_type_string_to_dtype("INTERVAL"), do: {:duration, :microsecond}

  defp duckdb_type_string_to_dtype("DECIMAL" <> rest) do
    case Regex.run(~r/\((\d+),\s*(\d+)\)/, rest) do
      [_, p, s] -> {:decimal, String.to_integer(p), String.to_integer(s)}
      _ -> {:decimal, 18, 3}
    end
  end

  defp duckdb_type_string_to_dtype(other) do
    cond do
      String.ends_with?(other, "[]") ->
        inner = String.slice(other, 0..-3//1)
        {:list, duckdb_type_string_to_dtype(inner)}

      true ->
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
