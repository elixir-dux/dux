defmodule Dux.Remote.Merger do
  @moduledoc false

  # Merges partial results from distributed workers.
  #
  # The merge step itself runs on the coordinator's DuckDB — we collect
  # Arrow IPC from each worker, load them as temp tables, then run a
  # DuckDB query to merge. DuckDB handles the heavy lifting.

  @doc """
  Merge multiple Arrow IPC results into a single result.

  Returns an Arrow IPC binary of the merged result.
  """
  def merge(ipc_results, %Dux{ops: ops}) do
    db = Dux.Connection.get_db()

    # Load each worker result as a temp table
    table_names =
      ipc_results
      |> Enum.with_index()
      |> Enum.map(fn {ipc, _idx} ->
        table_ref = Dux.Native.table_from_ipc(ipc)
        name = Dux.Native.table_ensure(db, table_ref)
        # Keep reference alive
        {name, table_ref}
      end)

    # Build UNION ALL of all partitions
    union_sql =
      table_names
      |> Enum.map_join(" UNION ALL ", fn {name, _ref} ->
        ~s(SELECT * FROM "#{name}")
      end)

    # Apply any final operations that need re-aggregation
    final_sql = apply_merge_ops(union_sql, ops)

    Process.put(:dux_merge_refs, table_names)

    result =
      case Dux.Native.df_query(db, final_sql) do
        {:error, reason} ->
          {:error, reason}

        result_ref ->
          Dux.Native.table_to_ipc(result_ref)
      end

    Process.delete(:dux_merge_refs)
    result
  end

  @doc """
  Merge IPC results and return as a %Dux{} struct.
  """
  def merge_to_dux(ipc_results, pipeline) do
    db = Dux.Connection.get_db()

    # Load each worker result as a temp table.
    # Store refs in process dictionary to prevent GC — the BEAM compiler
    # can optimize away local variable references, but not process dict.
    input_refs =
      Enum.map(ipc_results, fn ipc ->
        table_ref = Dux.Native.table_from_ipc(ipc)
        name = Dux.Native.table_ensure(db, table_ref)
        {name, table_ref}
      end)

    Process.put(:dux_merge_refs, input_refs)

    union_sql =
      Enum.map_join(input_refs, " UNION ALL ", fn {name, _ref} ->
        ~s(SELECT * FROM "#{name}")
      end)

    final_sql = apply_merge_ops(union_sql, pipeline.ops)

    result =
      case Dux.Native.df_query(db, final_sql) do
        {:error, reason} ->
          raise ArgumentError, "Merge failed: #{reason}"

        table_ref ->
          names = Dux.Native.table_names(table_ref)
          dtypes = table_ref |> Dux.Native.table_dtypes() |> Map.new()
          %Dux{source: {:table, table_ref}, names: names, dtypes: dtypes}
      end

    Process.delete(:dux_merge_refs)
    result
  end

  # ---------------------------------------------------------------------------
  # Merge operations
  # ---------------------------------------------------------------------------

  # Determine what final SQL to run based on the pipeline's operations.
  # For simple scans/filters, UNION ALL is sufficient.
  # For aggregations, we need to re-aggregate.
  # For sort/head, we need to re-sort/re-limit.
  defp apply_merge_ops(union_sql, ops) do
    # Find the last meaningful operation that affects merge strategy
    case find_merge_strategy(ops) do
      :concat ->
        "SELECT * FROM (#{union_sql}) __merged"

      {:re_sort, sort_spec} ->
        order = format_order(sort_spec)
        "SELECT * FROM (#{union_sql}) __merged ORDER BY #{order}"

      {:re_sort_head, sort_spec, n} ->
        order = format_order(sort_spec)

        "SELECT * FROM (#{union_sql}) __merged ORDER BY #{order} LIMIT #{n}"

      {:re_head, n} ->
        "SELECT * FROM (#{union_sql}) __merged LIMIT #{n}"

      :re_distinct ->
        "SELECT DISTINCT * FROM (#{union_sql}) __merged"

      {:re_aggregate, groups, aggs} ->
        # Re-aggregate partial results with correct functions
        group_cols = Enum.map_join(groups, ", ", &qi/1)

        agg_cols =
          Enum.map_join(aggs, ", ", fn {name, expr} ->
            re_aggregate_expr(name, expr)
          end)

        select = if groups == [], do: agg_cols, else: "#{group_cols}, #{agg_cols}"
        group_clause = if groups == [], do: "", else: " GROUP BY #{group_cols}"

        "SELECT #{select} FROM (#{union_sql}) __merged#{group_clause}"
    end
  end

  defp find_merge_strategy(ops) do
    # Walk ops in reverse to find what the merge needs to handle
    ops
    |> Enum.reverse()
    |> do_find_merge_strategy()
  end

  # Default: just concatenate
  defp do_find_merge_strategy([]), do: :concat

  # Head after sort → re-sort then head
  defp do_find_merge_strategy([{:head, n}, {:sort_by, spec} | _]), do: {:re_sort_head, spec, n}

  # Head alone → re-head (each worker already limited, take from merged)
  defp do_find_merge_strategy([{:head, n} | _]), do: {:re_head, n}

  # Sort → re-sort
  defp do_find_merge_strategy([{:sort_by, spec} | _]), do: {:re_sort, spec}

  # Distinct → re-distinct
  defp do_find_merge_strategy([{:distinct, _} | _]), do: :re_distinct

  # Summarise → re-aggregate
  defp do_find_merge_strategy([{:summarise, aggs} | rest]) do
    groups = find_groups(rest)
    {:re_aggregate, groups, aggs}
  end

  # Skip other ops and keep looking
  defp do_find_merge_strategy([_ | rest]), do: do_find_merge_strategy(rest)

  defp find_groups([{:group_by, cols} | _]), do: cols
  defp find_groups([_ | rest]), do: find_groups(rest)
  defp find_groups([]), do: []

  defp format_order(sort_spec) do
    Enum.map_join(sort_spec, ", ", fn
      {:asc, col} -> "#{qi(col)} ASC"
      {:desc, col} -> "#{qi(col)} DESC"
    end)
  end

  # Determine the correct re-aggregation function based on the original expression.
  # SUM → SUM, COUNT → SUM, MIN → MIN, MAX → MAX
  # AVG columns should have been rewritten by PipelineSplitter before reaching here.
  defp re_aggregate_expr(name, expr) when is_binary(expr) do
    upper = String.upcase(expr)
    quoted = qi(name)

    cond do
      String.contains?(upper, "MIN(") -> "MIN(#{quoted}) AS #{quoted}"
      String.contains?(upper, "MAX(") -> "MAX(#{quoted}) AS #{quoted}"
      String.contains?(upper, "SUM(") -> "SUM(#{quoted}) AS #{quoted}"
      String.contains?(upper, "COUNT(") -> "SUM(#{quoted}) AS #{quoted}"
      # Default: SUM (safe for additive aggregates)
      true -> "SUM(#{quoted}) AS #{quoted}"
    end
  end

  defp re_aggregate_expr(name, _expr) do
    quoted = qi(name)
    "SUM(#{quoted}) AS #{quoted}"
  end

  # Quote identifier — escape double quotes to prevent SQL injection
  defp qi(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end
end
