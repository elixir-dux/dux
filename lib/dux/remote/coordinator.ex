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

    worker_pipeline = %{pipeline | ops: worker_ops}

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

  # Apply AVG rewrites: compute __sum / __count for each rewritten AVG column
  defp apply_avg_rewrites(dux, rewrites) when map_size(rewrites) == 0, do: dux

  defp apply_avg_rewrites(dux, rewrites) do
    # For each AVG rewrite, add a mutate computing sum/count,
    # then discard the intermediate columns
    avg_exprs =
      Enum.map(rewrites, fn {name, {:avg, sum_col, count_col, _inner}} ->
        {name, "\"#{esc(sum_col)}\" / \"#{esc(count_col)}\""}
      end)

    intermediate_cols =
      Enum.flat_map(rewrites, fn {_name, {:avg, sum_col, count_col, _}} ->
        [String.to_atom(sum_col), String.to_atom(count_col)]
      end)

    dux
    |> Dux.mutate_with(avg_exprs)
    |> Dux.discard(intermediate_cols)
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

  defp esc(name), do: String.replace(name, ~s("), ~s(""))
end
