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
        classify_and_rewrite(name, expr, acc_aggs, acc_rewrites)
      end)

    {Enum.reverse(worker_aggs), rewrites}
  end

  defp classify_and_rewrite(name, expr, acc_aggs, acc_rewrites) do
    upper = if is_binary(expr), do: String.upcase(expr), else: ""

    case classify_agg(upper) do
      :avg -> rewrite_avg(name, expr, acc_aggs, acc_rewrites)
      :stddev -> rewrite_stddev(name, expr, upper, acc_aggs, acc_rewrites)
      :count_distinct -> rewrite_count_distinct(name, expr, acc_aggs, acc_rewrites)
      :passthrough -> {[{name, expr} | acc_aggs], acc_rewrites}
    end
  end

  @stddev_keywords ~w(STDDEV VARIANCE VAR_SAMP VAR_POP STDDEV_SAMP STDDEV_POP)

  defp classify_agg(upper) do
    cond do
      String.contains?(upper, "AVG(") ->
        :avg

      Enum.any?(@stddev_keywords, &String.contains?(upper, &1)) ->
        :stddev

      String.contains?(upper, "COUNT(DISTINCT") or String.contains?(upper, "COUNT_DISTINCT(") ->
        :count_distinct

      true ->
        :passthrough
    end
  end

  # AVG(x) → workers: SUM(x), COUNT(x). Coordinator: SUM/COUNT
  defp rewrite_avg(name, expr, acc_aggs, acc_rewrites) do
    sum_name = "__avg_sum_#{name}"
    count_name = "__avg_count_#{name}"

    new_aggs = [
      {sum_name, rewrite_func(expr, "AVG", "SUM")},
      {count_name, rewrite_func(expr, "AVG", "COUNT")} | acc_aggs
    ]

    new_rewrites = Map.put(acc_rewrites, name, {:avg, sum_name, count_name})
    {new_aggs, new_rewrites}
  end

  # STDDEV/VARIANCE → workers: COUNT(x), SUM(x), SUM(x*x). Coordinator: algebraic formula
  defp rewrite_stddev(name, expr, upper, acc_aggs, acc_rewrites) do
    inner = extract_inner_expr(expr)
    n_name = "__sd_n_#{name}"
    sum_name = "__sd_sum_#{name}"
    sum2_name = "__sd_sum2_#{name}"

    new_aggs = [
      {n_name, "COUNT(#{inner})"},
      {sum_name, "SUM(CAST(#{inner} AS DOUBLE))"},
      {sum2_name, "SUM(CAST(#{inner} AS DOUBLE) * CAST(#{inner} AS DOUBLE))"} | acc_aggs
    ]

    func_type =
      cond do
        String.contains?(upper, "STDDEV_POP") -> :stddev_pop
        String.contains?(upper, "VAR_POP") -> :var_pop
        String.contains?(upper, "VAR_SAMP") or String.contains?(upper, "VARIANCE") -> :var_samp
        true -> :stddev_samp
      end

    new_rewrites = Map.put(acc_rewrites, name, {:stddev, func_type, n_name, sum_name, sum2_name})
    {new_aggs, new_rewrites}
  end

  # COUNT(DISTINCT x) → workers compute approx_count_distinct (HyperLogLog).
  # Coordinator sums the approximations. ~2% error, constant wire cost.
  defp rewrite_count_distinct(name, expr, acc_aggs, acc_rewrites) do
    inner = extract_inner_expr(expr)
    hll_name = "__hll_#{name}"

    # Workers compute HyperLogLog approximate count (one number per group, not arrays)
    new_aggs = [{hll_name, "approx_count_distinct(#{inner})"} | acc_aggs]
    new_rewrites = Map.put(acc_rewrites, name, {:count_distinct_hll, hll_name})
    {new_aggs, new_rewrites}
  end

  defp extract_inner_expr(expr) when is_binary(expr) do
    # Extract the inner expression from FUNC(inner) or FUNC(DISTINCT inner)
    case Regex.run(~r/\w+\(\s*(?:DISTINCT\s+)?(.+)\)/i, expr) do
      [_, inner] -> String.trim(inner)
      _ -> expr
    end
  end

  defp extract_inner_expr(expr), do: inspect(expr)

  defp rewrite_func(expr, from, to) do
    String.replace(expr, ~r/#{from}\(/i, "#{to}(")
  end
end
