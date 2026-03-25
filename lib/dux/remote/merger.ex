defmodule Dux.Remote.Merger do
  @moduledoc false

  import Dux.SQL.Helpers, only: [qi: 1, with_refs: 3]

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
    conn = Dux.Connection.get_conn()

    # Load each worker result as a temp table
    table_names =
      ipc_results
      |> Enum.with_index()
      |> Enum.map(fn {ipc, _idx} ->
        table_ref = Dux.Backend.table_from_ipc(conn, ipc)
        name = table_ref.name
        # Keep reference alive
        {name, table_ref}
      end)

    # Keep refs alive BEFORE building SQL that references temp table names
    with_refs(:dux_merge_refs, table_names, fn ->
      union_sql =
        Enum.map_join(table_names, " UNION ALL ", fn {name, _ref} ->
          ~s(SELECT * FROM "#{name}")
        end)

      final_sql = apply_merge_ops(union_sql, ops)

      result_ref = Dux.Backend.query(conn, final_sql)
      Dux.Backend.table_to_ipc(conn, result_ref)
    end)
  end

  @doc """
  Merge IPC results and return as a %Dux{} struct.
  """
  def merge_to_dux(ipc_results, pipeline) do
    conn = Dux.Connection.get_conn()

    # Load each worker result as a temp table.
    # Store refs in process dictionary to prevent GC — the BEAM compiler
    # can optimize away local variable references, but not process dict.
    input_refs =
      Enum.map(ipc_results, fn ipc ->
        table_ref = Dux.Backend.table_from_ipc(conn, ipc)
        name = table_ref.name
        {name, table_ref}
      end)

    # Keep refs alive BEFORE building SQL
    with_refs(:dux_merge_refs, input_refs, fn ->
      union_sql =
        Enum.map_join(input_refs, " UNION ALL ", fn {name, _ref} ->
          ~s(SELECT * FROM "#{name}")
        end)

      final_sql = apply_merge_ops(union_sql, pipeline.ops)

      table_ref = Dux.Backend.query(conn, final_sql)
      names = Dux.Backend.table_names(conn, table_ref)
      dtypes = Dux.Backend.table_dtypes(conn, table_ref) |> Map.new()
      %Dux{source: {:table, table_ref}, names: names, dtypes: dtypes}
    end)
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
    # The primary concern is whether there's an aggregation that needs re-aggregation.
    # Sort/head/distinct are secondary — they apply after re-aggregation if needed.
    has_summarise = Enum.any?(ops, &match?({:summarise, _}, &1))

    if has_summarise do
      # Find the summarise and its groups
      {groups, aggs} = find_aggregation(ops)
      {:re_aggregate, groups, aggs}
    else
      # No aggregation — check for sort/head/distinct
      ops
      |> Enum.reverse()
      |> do_find_merge_strategy()
    end
  end

  defp find_aggregation(ops) do
    summarise_idx = Enum.find_index(ops, &match?({:summarise, _}, &1))
    {:summarise, aggs} = Enum.at(ops, summarise_idx)
    groups = find_groups(Enum.take(ops, summarise_idx))
    {groups, aggs}
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
  # Uses word-boundary regex to prevent substring matches (e.g. COUNT_DISTINCT matching COUNT).
  # Order matters: more specific patterns (APPROX_COUNT_DISTINCT, COUNT_DISTINCT) before COUNT.
  defp re_aggregate_expr(name, expr) when is_binary(expr) do
    quoted = qi(name)

    agg_fn =
      cond do
        Regex.match?(~r/\bMIN\s*\(/i, expr) -> "MIN"
        Regex.match?(~r/\bMAX\s*\(/i, expr) -> "MAX"
        Regex.match?(~r/\bSUM\s*\(/i, expr) -> "SUM"
        Regex.match?(~r/\bAPPROX_COUNT_DISTINCT\s*\(/i, expr) -> "SUM"
        Regex.match?(~r/\bCOUNT_DISTINCT\s*\(/i, expr) -> "SUM"
        Regex.match?(~r/\bCOUNT\s*\(/i, expr) -> "SUM"
        # Default: SUM (safe for additive aggregates)
        true -> "SUM"
      end

    "#{agg_fn}(#{quoted}) AS #{quoted}"
  end

  defp re_aggregate_expr(name, _expr) do
    quoted = qi(name)
    "SUM(#{quoted}) AS #{quoted}"
  end
end
