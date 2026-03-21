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

  alias Dux.Remote.{Merger, Partitioner, Worker}

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

    # Partition the pipeline across workers
    assignments = Partitioner.assign(pipeline, workers, strategy: strategy)

    # Fan out: each worker executes its partition
    results = fan_out(assignments, timeout)

    # Collect successful results, handle failures
    {successes, failures} = partition_results(results)

    if successes == [] do
      reasons = Enum.map(failures, fn {:error, reason} -> reason end)
      raise ArgumentError, "all workers failed: #{inspect(reasons)}"
    end

    # Merge partial results on coordinator
    Merger.merge_to_dux(successes, pipeline)
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
end
