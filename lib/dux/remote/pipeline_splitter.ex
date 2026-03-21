defmodule Dux.Remote.PipelineSplitter do
  @moduledoc false

  # Splits a pipeline's ops into worker-safe ops (pushed to workers)
  # and coordinator ops (applied after merge).
  #
  # Safe to push: filter, mutate, select, discard, rename, drop_nil,
  #   group_by, summarise (with rewrite), sort_by, head, distinct
  #
  # Coordinator-only: slice, pivot_wider, pivot_longer (after merge),
  #   join (requires special handling)
  #
  # The splitter also rewrites AVG aggregations: workers compute
  # SUM + COUNT, coordinator divides to get the correct average.

  @doc """
  Split pipeline ops into worker and coordinator portions.

  Returns `%{worker_ops: [...], coordinator_ops: [...], agg_rewrites: %{}}`.

  `agg_rewrites` maps output column names to their rewrite info
  (e.g., AVG rewritten to SUM/COUNT pair).
  """
  def split(ops) do
    {worker_ops, coordinator_ops, rewrites} = do_split(ops, [], [], %{})

    %{
      worker_ops: Enum.reverse(worker_ops),
      coordinator_ops: Enum.reverse(coordinator_ops),
      agg_rewrites: rewrites
    }
  end

  # Walk ops left-to-right. Push safe ops to workers, keep others for coordinator.

  # Filter, mutate, select, discard, rename, drop_nil — always safe
  defp do_split([{:filter, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  defp do_split([{:mutate, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  defp do_split([{:select, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  defp do_split([{:discard, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  defp do_split([{:rename, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  defp do_split([{:drop_nil, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  # Group by — push to workers (sets state for summarise)
  defp do_split([{:group_by, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  defp do_split([{:ungroup} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  # Summarise — push to workers, but rewrite AVG and track for re-aggregation
  defp do_split([{:summarise, aggs} | rest], worker, coord, rewrites) do
    {worker_aggs, new_rewrites} = rewrite_aggregates(aggs)
    do_split(rest, [{:summarise, worker_aggs} | worker], coord, Map.merge(rewrites, new_rewrites))
  end

  # Sort, head, distinct — push to workers AND add to coordinator for re-merge
  defp do_split([{:sort_by, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], [op | coord], rewrites)
  end

  defp do_split([{:head, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], [op | coord], rewrites)
  end

  defp do_split([{:distinct, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], [op | coord], rewrites)
  end

  # Slice — coordinator only (OFFSET is positional, not safe per-partition)
  defp do_split([{:slice, _, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, worker, [op | coord], rewrites)
  end

  # Pivot — coordinator only (schema may differ across partitions)
  defp do_split([{:pivot_wider, _, _, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, worker, [op | coord], rewrites)
  end

  defp do_split([{:pivot_longer, _, _, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, worker, [op | coord], rewrites)
  end

  # Join — push to workers (right side will be broadcast/replicated by the partitioner)
  defp do_split([{:join, _, _, _, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  # Concat — push to workers
  defp do_split([{:concat_rows, _} = op | rest], worker, coord, rewrites) do
    do_split(rest, [op | worker], coord, rewrites)
  end

  # Unknown op — keep on coordinator for safety
  defp do_split([op | rest], worker, coord, rewrites) do
    do_split(rest, worker, [op | coord], rewrites)
  end

  defp do_split([], worker, coord, rewrites) do
    {worker, coord, rewrites}
  end

  # ---------------------------------------------------------------------------
  # Aggregate rewrites
  # ---------------------------------------------------------------------------

  # Rewrite aggregates that can't be naively re-aggregated with SUM.
  # AVG(x) → workers compute __sum_name = SUM(x), __count_name = COUNT(x)
  #           coordinator computes name = __sum_name / __count_name
  defp rewrite_aggregates(aggs) do
    {worker_aggs, rewrites} =
      Enum.reduce(aggs, {[], %{}}, fn {name, expr}, {acc_aggs, acc_rewrites} ->
        if avg_expr?(expr) do
          inner = extract_inner_expr(expr, "AVG")
          sum_name = "__avg_sum_#{name}"
          count_name = "__avg_count_#{name}"

          new_aggs = [
            {sum_name, rewrite_func(expr, "AVG", "SUM")},
            {count_name, rewrite_func(expr, "AVG", "COUNT")} | acc_aggs
          ]

          new_rewrites = Map.put(acc_rewrites, name, {:avg, sum_name, count_name, inner})
          {new_aggs, new_rewrites}
        else
          {[{name, expr} | acc_aggs], acc_rewrites}
        end
      end)

    {Enum.reverse(worker_aggs), rewrites}
  end

  defp avg_expr?(expr) when is_binary(expr), do: String.contains?(String.upcase(expr), "AVG(")
  defp avg_expr?(_), do: false

  defp extract_inner_expr(expr, func) do
    # Extract the inner expression from FUNC(inner)
    regex = ~r/#{func}\((.+)\)/i

    case Regex.run(regex, expr) do
      [_, inner] -> inner
      _ -> expr
    end
  end

  defp rewrite_func(expr, from, to) do
    String.replace(expr, ~r/#{from}\(/i, "#{to}(")
  end
end
