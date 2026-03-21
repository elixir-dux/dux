defmodule Dux.Remote.Coordinator do
  @moduledoc """
  Orchestrates distributed query execution across a cluster of Dux workers.

  The coordinator:
  1. Discovers available workers via `:pg`
  2. Partitions the data source across workers
  3. Fans out the pipeline to each worker via `Worker.execute/2`
  4. Merges partial results on the coordinator node

  ## Usage

      # Execute a pipeline across all available workers
      result = Dux.Remote.Coordinator.execute(pipeline)

      # Execute with specific workers
      result = Dux.Remote.Coordinator.execute(pipeline, workers: [w1, w2, w3])

  The result is a `%Dux{}` struct with the merged data.
  """

  alias Dux.Remote.{Merger, Partitioner, PipelineSplitter, Worker}
  import Dux.SQL.Helpers, only: [qi: 1]

  # Broadcast threshold: 256MB serialized Arrow IPC
  @broadcast_threshold 256 * 1024 * 1024

  @doc """
  Execute a `%Dux{}` pipeline across distributed workers.

  Partitions the source, fans out to workers, collects Arrow IPC results,
  and merges on the coordinator node.

  ## Options

    * `:workers` — list of worker PIDs (default: all workers from `:pg`)
    * `:timeout` — per-worker timeout in ms (default: `:infinity`)
    * `:strategy` — partitioning strategy (default: `:round_robin`)

  Returns a `%Dux{}` struct with the merged results.
  """
  def execute(%Dux{} = pipeline, opts \\ []) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    timeout = Keyword.get(opts, :timeout, :infinity)
    strategy = Keyword.get(opts, :strategy, :round_robin)

    if workers == [] do
      raise ArgumentError, "no workers available for distributed execution"
    end

    # Split pipeline: worker ops push down, coordinator ops apply post-merge
    %{worker_ops: worker_ops, coordinator_ops: coord_ops, agg_rewrites: rewrites} =
      PipelineSplitter.split(pipeline.ops)

    # Preprocess joins: broadcast/shuffle right sides that aren't worker-safe
    {processed_ops, broadcast_tables} = preprocess_joins(worker_ops, workers, timeout)

    worker_pipeline = %{pipeline | ops: processed_ops}

    try do
      # Partition the worker pipeline across workers
      assignments = Partitioner.assign(worker_pipeline, workers, strategy: strategy)

      # Fan out: each worker executes its partition
      results = fan_out(assignments, timeout)

      # Collect successful results, handle failures
      {successes, failures} = partition_results(results)

      if successes == [] do
        reasons = Enum.map(failures, fn {:error, reason} -> reason end)
        raise ArgumentError, "all workers failed: #{inspect(reasons)}"
      end

      # Merge partial results on coordinator
      merged = Merger.merge_to_dux(successes, worker_pipeline)

      # Apply AVG rewrites if any
      merged = apply_avg_rewrites(merged, rewrites)

      # Apply coordinator-only ops (slice, pivot, etc.)
      apply_coordinator_ops(merged, coord_ops)
    after
      # Clean up broadcast tables on all workers
      cleanup_broadcast_tables(workers, broadcast_tables)
    end
  end

  @doc """
  Execute a pipeline across workers, returning raw Arrow IPC binaries.

  Useful when you want to handle merging yourself or stream results.
  """
  def fan_out_raw(%Dux{} = pipeline, opts \\ []) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    timeout = Keyword.get(opts, :timeout, :infinity)

    assignments = Partitioner.assign(pipeline, workers)
    fan_out(assignments, timeout)
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp fan_out(assignments, timeout) do
    # Execute in parallel via Task.async_stream
    assignments
    |> Task.async_stream(
      fn {worker, partition_pipeline} ->
        Worker.execute(worker, partition_pipeline, timeout)
      end,
      timeout: timeout,
      max_concurrency: length(assignments),
      ordered: true
    )
    |> Enum.map(fn
      {:ok, {:ok, ipc}} -> {:ok, ipc}
      {:ok, {:error, reason}} -> {:error, reason}
      {:exit, reason} -> {:error, {:worker_crash, reason}}
    end)
  end

  defp partition_results(results) do
    Enum.split_with(results, fn
      {:ok, _} -> true
      _ -> false
    end)
    |> then(fn {ok, err} ->
      {Enum.map(ok, fn {:ok, ipc} -> ipc end), err}
    end)
  end

  # ---------------------------------------------------------------------------
  # Join preprocessing — broadcast/shuffle non-worker-safe right sides
  # ---------------------------------------------------------------------------

  # Scan worker ops for joins where the right side holds a source that
  # workers can't resolve (e.g. a local {:table, ref}).  For each such join,
  # compute the right side on the coordinator, broadcast it to all workers,
  # and replace the join op with a join against the broadcast table.
  #
  # Returns {processed_ops, broadcast_table_names} where broadcast_table_names
  # is a list of names to clean up after query execution.
  defp preprocess_joins(ops, workers, timeout) do
    Enum.map_reduce(ops, [], fn
      {:join, %Dux{} = right, how, on_cols, suffix}, broadcast_names ->
        if worker_safe_source?(right.source) and Enum.all?(right.ops, &worker_safe_op?/1) do
          # Right side is resolvable on workers — pass through unchanged
          {{:join, right, how, on_cols, suffix}, broadcast_names}
        else
          # Right side has a local source — need to broadcast or shuffle
          route_join(right, how, on_cols, suffix, workers, timeout, broadcast_names)
        end

      op, broadcast_names ->
        {op, broadcast_names}
    end)
  end

  # Route a join with a non-worker-safe right side to broadcast or shuffle.
  defp route_join(right, how, on_cols, suffix, workers, timeout, broadcast_names) do
    # Compute the right side on the coordinator
    right_computed = Dux.compute(right)
    {:table, right_ref} = right_computed.source
    right_ipc = Dux.Native.table_to_ipc(right_ref)

    if byte_size(right_ipc) <= @broadcast_threshold do
      # Small enough to broadcast
      broadcast_name = "__bcast_#{:erlang.unique_integer([:positive])}"
      broadcast_to_workers(workers, broadcast_name, right_ipc, timeout)

      # Replace right side with a query against the broadcast table
      broadcast_right = Dux.from_query("SELECT * FROM #{qi(broadcast_name)}")
      {{:join, broadcast_right, how, on_cols, suffix}, [broadcast_name | broadcast_names]}
    else
      # Too large — fall back to shuffle.
      # We can't inline this into the op list; the shuffle needs to execute
      # as a separate coordinated stage. For now, raise with a clear message.
      # Full shuffle-join integration is tracked as brief 2f (future work).
      raise ArgumentError,
            "right side of distributed join is too large to broadcast " <>
              "(#{div(byte_size(right_ipc), 1024 * 1024)}MB > " <>
              "#{div(@broadcast_threshold, 1024 * 1024)}MB threshold). " <>
              "Use Dux.Remote.Shuffle.execute/3 directly for large-large joins."
    end
  end

  defp broadcast_to_workers(workers, name, ipc_binary, _timeout) do
    tasks =
      Enum.map(workers, fn worker ->
        Task.async(fn -> Worker.register_table(worker, name, ipc_binary) end)
      end)

    Task.await_many(tasks, 30_000)
  end

  defp cleanup_broadcast_tables(_workers, []), do: :ok

  defp cleanup_broadcast_tables(workers, broadcast_names) do
    Enum.each(broadcast_names, fn name ->
      Enum.each(workers, fn worker ->
        try do
          Worker.drop_table(worker, name)
        catch
          _, _ -> :ok
        end
      end)
    end)
  end

  # A source is worker-safe if every worker can independently resolve it.
  # File paths, SQL queries, and inline lists are worker-safe.
  # Table refs ({:table, ResourceArc}) are node-local — only the coordinator has them.
  defp worker_safe_source?({:parquet, _}), do: true
  defp worker_safe_source?({:csv, _, _}), do: true
  defp worker_safe_source?({:ndjson, _, _}), do: true
  defp worker_safe_source?({:sql, _}), do: true
  defp worker_safe_source?({:query, _}), do: true
  defp worker_safe_source?({:list, _}), do: true
  defp worker_safe_source?({:table, _}), do: false
  defp worker_safe_source?(_), do: false

  # Check if an op itself is worker-safe (no nested non-worker-safe sources).
  # Joins can nest other Dux structs in the right side.
  defp worker_safe_op?({:join, %Dux{} = right, _, _, _}) do
    worker_safe_source?(right.source) and Enum.all?(right.ops, &worker_safe_op?/1)
  end

  defp worker_safe_op?({:concat_rows, others}) do
    Enum.all?(others, fn %Dux{} = other ->
      worker_safe_source?(other.source) and Enum.all?(other.ops, &worker_safe_op?/1)
    end)
  end

  defp worker_safe_op?(_), do: true

  # ---------------------------------------------------------------------------

  # Apply aggregate rewrites (AVG, STDDEV, VARIANCE, COUNT DISTINCT)
  defp apply_avg_rewrites(dux, rewrites) when map_size(rewrites) == 0, do: dux

  defp apply_avg_rewrites(dux, rewrites) do
    computed_exprs =
      Enum.map(rewrites, fn
        {name, {:avg, sum_col, count_col}} ->
          {name, "#{qi(sum_col)} / #{qi(count_col)}"}

        {name, {:stddev, :stddev_samp, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :samp, true)}

        {name, {:stddev, :stddev_pop, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :pop, true)}

        {name, {:stddev, :var_samp, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :samp, false)}

        {name, {:stddev, :var_pop, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :pop, false)}

        {name, {:count_distinct, cd_col, _inner}} ->
          # ARRAY_AGG(DISTINCT) across workers → flatten and count distinct
          {name, "list_distinct(flatten(list(#{qi(cd_col)}))).__len"}
      end)

    intermediate_cols =
      Enum.flat_map(rewrites, fn
        {_, {:avg, sum_col, count_col}} ->
          [String.to_atom(sum_col), String.to_atom(count_col)]

        {_, {:stddev, _, n, sum_x, sum_x2}} ->
          [String.to_atom(n), String.to_atom(sum_x), String.to_atom(sum_x2)]

        {_, {:count_distinct, cd_col, _}} ->
          [String.to_atom(cd_col)]
      end)

    dux
    |> Dux.mutate_with(computed_exprs)
    |> Dux.discard(intermediate_cols)
  end

  defp stddev_formula(n_col, sum_col, sum2_col, pop_or_samp, sqrt?) do
    n = qi(n_col)
    sx = qi(sum_col)
    sx2 = qi(sum2_col)

    divisor = if pop_or_samp == :samp, do: "(#{n} - 1)", else: n

    variance = "GREATEST(0, (#{sx2} - #{sx} * #{sx} / #{n}::DOUBLE) / #{divisor}::DOUBLE)"

    if sqrt? do
      "CASE WHEN #{n} <= 1 AND '#{pop_or_samp}' = 'samp' THEN NULL ELSE SQRT(#{variance}) END"
    else
      "CASE WHEN #{n} <= 1 AND '#{pop_or_samp}' = 'samp' THEN NULL ELSE #{variance} END"
    end
  end

  # Apply coordinator-only ops to the merged result
  defp apply_coordinator_ops(dux, []), do: dux

  defp apply_coordinator_ops(dux, [op | rest]) do
    updated = apply_single_op(dux, op)
    apply_coordinator_ops(updated, rest)
  end

  defp apply_single_op(dux, {:slice, offset, length}), do: Dux.slice(dux, offset, length)
  defp apply_single_op(dux, {:head, n}), do: Dux.head(dux, n)
  defp apply_single_op(dux, {:sort_by, spec}), do: %{dux | ops: dux.ops ++ [{:sort_by, spec}]}
  defp apply_single_op(dux, {:distinct, cols}), do: %{dux | ops: dux.ops ++ [{:distinct, cols}]}

  defp apply_single_op(dux, {:pivot_wider, names_col, values_col, agg}) do
    Dux.pivot_wider(dux, names_col, values_col, agg: agg)
  end

  defp apply_single_op(dux, {:pivot_longer, cols, names_to, values_to}) do
    Dux.pivot_longer(dux, cols, names_to: names_to, values_to: values_to)
  end

  defp apply_single_op(dux, op) do
    # Unknown op — append directly
    %{dux | ops: dux.ops ++ [op]}
  end

end
