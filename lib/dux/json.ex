defmodule Dux.JSON do
  @moduledoc """
  JSON processing verbs for semi-structured data.

  DuckDB's JSON extension auto-loads and provides efficient JSON parsing,
  extraction, and flattening. These verbs wrap the common patterns.

  For simple single-field extraction, use `Dux.mutate` with DuckDB's JSON
  functions directly:

      Dux.mutate_with(df, name: "json_extract_string(payload, '$.user.name')")

  The `Dux.JSON` module is for operations that extract multiple fields at once,
  flatten arrays to rows, or auto-detect JSON structure.
  """

  import Dux.SQL.Helpers, only: [qi: 1]

  @doc """
  Extract multiple fields from a JSON column into new columns.

  The `fields` map specifies `{output_column, json_path}` pairs.

  ## Examples

      Dux.JSON.extract(df, :payload, %{
        user_name: "$.user.name",
        user_email: "$.user.email",
        event_type: "$.type"
      })
  """
  def extract(%Dux{} = dux, column, fields) when is_map(fields) do
    exprs =
      Enum.map(fields, fn {output_col, path} ->
        {output_col, "json_extract_string(#{qi(to_string(column))}, '#{escape_path(path)}')"}
      end)

    Dux.mutate_with(dux, exprs)
  end

  @doc """
  Flatten a JSON array column to rows.

  Each element in the JSON array becomes a separate row. The original
  row's other columns are repeated for each element.

  ## Options

    * `:path` — JSON path to extract before unnesting (default: root)
    * `:as` — output column name (default: singularized column name)

  ## Examples

      # Simple: tags column is a JSON array
      Dux.JSON.unnest(df, :tags)

      # With path: extract nested array first
      Dux.JSON.unnest(df, :payload, path: "$.items", as: :item)
  """
  def unnest(%Dux{ops: ops} = dux, column, opts \\ []) do
    col_str = to_string(column)
    path = Keyword.get(opts, :path)
    as_col = Keyword.get(opts, :as, :"#{col_str}_value")

    %{dux | ops: ops ++ [{:json_unnest, col_str, path, to_string(as_col)}]}
  end

  @doc """
  Auto-detect top-level JSON keys and expand into columns.

  Inspects the first row to discover the JSON structure, then extracts
  each top-level key as a new column. Requires `compute/1` first
  (needs data to inspect).

  ## Examples

      df
      |> Dux.compute()
      |> Dux.JSON.expand(:data)
  """
  def expand(%Dux{} = dux, column) do
    col_str = to_string(column)
    conn = Dux.Connection.get_conn()

    # Discover keys from the first non-null row
    {data_sql, _} = Dux.QueryBuilder.build(dux, conn)

    keys_sql = """
    SELECT DISTINCT unnest(json_keys(#{qi(col_str)})) AS k
    FROM (#{data_sql}) __src
    WHERE #{qi(col_str)} IS NOT NULL
    LIMIT 100
    """

    keys_ref = Dux.Backend.query(conn, keys_sql)
    cols = Dux.Backend.table_to_columns(conn, keys_ref)
    keys = cols["k"] || []

    if keys == [] do
      dux
    else
      exprs =
        Enum.map(keys, fn key ->
          safe_key = String.replace(key, "'", "''")
          {String.to_atom(key), "json_extract_string(#{qi(col_str)}, '$.#{safe_key}')"}
        end)

      Dux.mutate_with(dux, exprs)
    end
  end

  defp escape_path(path) do
    String.replace(path, "'", "''")
  end
end
