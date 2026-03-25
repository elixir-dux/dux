defmodule Dux.Remote.StreamingMerger do
  @moduledoc false

  # Incrementally collects worker IPC results and merges via DuckDB SQL.
  #
  # Instead of loading all IPC results at once (batch Merger), the
  # StreamingMerger accepts results as workers complete, enabling
  # progressive tracking and lower peak memory.
  #
  # The actual re-aggregation runs in DuckDB at finalization —
  # the same SQL the batch Merger uses (UNION ALL + GROUP BY with
  # correct re-aggregation functions). DuckDB handles the heavy lifting.

  alias Dux.Lattice
  alias Dux.Remote.Merger

  defstruct [
    :groups,
    :agg_columns,
    :worker_ops,
    :ipc_results,
    :workers_total,
    :workers_complete,
    :workers_failed
  ]

  @doc """
  Create a streaming merger for a pipeline with lattice-mergeable aggregates.

  `worker_ops` are the ops pushed to workers (after PipelineSplitter rewrite).
  `n_workers` is the total number of workers.

  Returns a `%StreamingMerger{}` or `nil` if the pipeline can't be streamed
  (non-lattice aggregates, no summarise, etc.).
  """
  def new(worker_ops, n_workers) do
    with {:ok, groups} <- find_groups(worker_ops),
         {:ok, aggs} <- find_summarise(worker_ops),
         {:ok, agg_columns} <- classify_rewritten_aggs(aggs) do
      %__MODULE__{
        groups: groups,
        agg_columns: agg_columns,
        worker_ops: worker_ops,
        ipc_results: [],
        workers_total: n_workers,
        workers_complete: 0,
        workers_failed: 0
      }
    else
      :not_streamable -> nil
    end
  end

  @doc """
  Fold one worker's IPC result into the accumulator.
  """
  def fold(%__MODULE__{} = merger, ipc_binary) do
    %{
      merger
      | ipc_results: [ipc_binary | merger.ipc_results],
        workers_complete: merger.workers_complete + 1
    }
  end

  @doc """
  Record a worker failure without crashing the merge.
  """
  def record_failure(%__MODULE__{} = merger) do
    %{merger | workers_failed: merger.workers_failed + 1}
  end

  @doc """
  Convert collected results to a `%Dux{}` struct via DuckDB re-aggregation.
  """
  def to_dux(%__MODULE__{ipc_results: []} = _merger) do
    Dux.from_query("SELECT 1 WHERE false") |> Dux.compute()
  end

  def to_dux(%__MODULE__{} = merger) do
    # Delegate to the batch Merger which already handles re-aggregation SQL
    Merger.merge_to_dux(
      Enum.reverse(merger.ipc_results),
      %Dux{source: nil, ops: merger.worker_ops, names: [], dtypes: %{}, groups: []}
    )
  end

  @doc """
  Finalize the accumulator into a list of row maps.
  """
  def finalize(%__MODULE__{} = merger) do
    dux = to_dux(merger)
    Dux.to_rows(dux)
  end

  @doc """
  Get progress metadata.
  """
  def progress(%__MODULE__{} = merger) do
    %{
      workers_complete: merger.workers_complete,
      workers_total: merger.workers_total,
      workers_failed: merger.workers_failed,
      complete?: merger.workers_complete + merger.workers_failed >= merger.workers_total
    }
  end

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  defp find_groups(ops) do
    case Enum.find(ops, &match?({:group_by, _}, &1)) do
      {:group_by, cols} -> {:ok, cols}
      nil -> {:ok, []}
    end
  end

  defp find_summarise(ops) do
    case Enum.find(ops, &match?({:summarise, _}, &1)) do
      {:summarise, aggs} -> {:ok, aggs}
      nil -> :not_streamable
    end
  end

  # Classify the rewritten aggregate columns into lattice types.
  # Still needed to determine if streaming is possible (all must be lattice-compatible).
  defp classify_rewritten_aggs(aggs) do
    result =
      Enum.reduce_while(aggs, [], fn {name, expr}, acc ->
        case classify_rewritten(expr) do
          nil -> {:halt, :not_streamable}
          lattice -> {:cont, [{name, lattice} | acc]}
        end
      end)

    case result do
      :not_streamable -> :not_streamable
      classified -> {:ok, Enum.reverse(classified)}
    end
  end

  defp classify_rewritten(expr) when is_binary(expr) do
    upper = String.upcase(expr)
    Lattice.classify(upper)
  end

  defp classify_rewritten(_), do: nil
end
