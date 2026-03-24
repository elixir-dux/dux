defmodule Dux.Remote.PartitionPruner do
  @moduledoc false

  # Prunes Hive-partitioned Parquet files based on pipeline filter ops.
  #
  # Given a list of file paths like:
  #   ["s3://bucket/year=2024/month=01/data.parquet",
  #    "s3://bucket/year=2024/month=02/data.parquet",
  #    "s3://bucket/year=2023/month=12/data.parquet"]
  #
  # And filter ops like:
  #   [{:filter, "\"year\" = 2024"}]
  #
  # Returns only the files where year=2024.
  #
  # Only handles simple equality predicates on partition columns.
  # Complex expressions are ignored (safe — we just read more data).

  @doc """
  Prune files whose Hive partition values don't match filter predicates.

  Returns the filtered list of files. If no partition filters can be
  extracted, returns the original list unchanged.
  """
  def prune(files, ops) do
    filters = extract_partition_filters(ops)

    if filters == [] do
      files
    else
      Enum.filter(files, fn file ->
        partitions = extract_hive_partitions(file)
        matches_all?(partitions, filters)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Hive partition extraction from file paths
  # ---------------------------------------------------------------------------

  # Extract key=value pairs from a file path.
  # "s3://bucket/year=2024/month=01/data.parquet" → %{"year" => "2024", "month" => "01"}
  @doc false
  def extract_hive_partitions(path) do
    path
    |> String.split("/")
    |> Enum.flat_map(fn segment ->
      case String.split(segment, "=", parts: 2) do
        [key, value] when key != "" and value != "" -> [{key, value}]
        _ -> []
      end
    end)
    |> Map.new()
  end

  # ---------------------------------------------------------------------------
  # Filter predicate extraction from ops
  # ---------------------------------------------------------------------------

  # Extract simple equality predicates from filter ops that match partition columns.
  # Returns [{column_name, value}] for predicates like:
  #   "year" = 2024        → {"year", "2024"}
  #   "month" = '01'       → {"month", "01"}
  #   year = 2024          → {"year", "2024"}
  defp extract_partition_filters(ops) do
    ops
    |> Enum.flat_map(fn
      {:filter, expr} when is_binary(expr) -> parse_equality_predicates(expr)
      _ -> []
    end)
  end

  # Parse simple equality predicates from a SQL WHERE clause string.
  # Handles: col = val, "col" = val, col = 'val', and AND-connected predicates.
  defp parse_equality_predicates(expr) do
    # Split on AND (case-insensitive) to handle compound filters
    expr
    |> String.split(~r/\bAND\b/i)
    |> Enum.flat_map(&parse_single_predicate/1)
  end

  defp parse_single_predicate(predicate) do
    trimmed = String.trim(predicate)

    # Match patterns like: "col" = val, col = val, "col" = 'val'
    # Captures: optional quotes around column name, = operator, value
    case Regex.run(
           ~r/^"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s*=\s*(.+)$/,
           trimmed
         ) do
      [_, column, raw_value] ->
        value = clean_value(String.trim(raw_value))
        [{column, value}]

      _ ->
        []
    end
  end

  # Strip quotes and whitespace from a SQL value literal
  defp clean_value(val) do
    val
    |> String.trim("'")
    |> String.trim("\"")
    |> String.trim()
  end

  # ---------------------------------------------------------------------------
  # Matching
  # ---------------------------------------------------------------------------

  # Check if a file's partition values match ALL filter predicates.
  # If the file has no partition for a given filter column, it passes
  # (the filter may apply to a non-partition column).
  defp matches_all?(partitions, filters) do
    Enum.all?(filters, fn {col, expected} ->
      case Map.get(partitions, col) do
        nil -> true
        actual -> actual == expected
      end
    end)
  end
end
