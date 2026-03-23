defmodule Dux.Remote.StreamingMerger do
  @moduledoc false

  # Folds worker IPC results incrementally using lattice merge operations.
  # Instead of loading all IPC results as temp tables and running one big
  # SQL query (batch Merger), the StreamingMerger decodes each result and
  # merges aggregate values in Elixir as workers report in.
  #
  # Benefits:
  # - Lower memory: don't hold all IPC binaries simultaneously
  # - Lower latency: merge starts when first worker finishes
  # - Enables progressive results (Phase C)
  #
  # The StreamingMerger operates on the *rewritten* worker output — after
  # PipelineSplitter has transformed AVG → SUM+COUNT etc. It uses the
  # re-aggregation functions (SUM→SUM, COUNT→SUM, MIN→MIN, MAX→MAX)
  # matching what the batch Merger does in SQL.

  alias Dux.Lattice

  defstruct [
    :groups,
    :agg_columns,
    :accumulator,
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
        accumulator: %{},
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
    conn = Dux.Connection.get_conn()
    ref = Dux.Backend.table_from_ipc(conn, ipc_binary)
    rows = Dux.Backend.table_to_rows(conn, ref)

    accumulator =
      Enum.reduce(rows, merger.accumulator, fn row, acc ->
        group_key = extract_group_key(row, merger.groups)
        group_state = Map.get(acc, group_key, init_group(merger.agg_columns))
        updated = merge_row(group_state, row, merger.agg_columns)
        Map.put(acc, group_key, updated)
      end)

    %{merger | accumulator: accumulator, workers_complete: merger.workers_complete + 1}
  end

  @doc """
  Record a worker failure without crashing the merge.
  """
  def record_failure(%__MODULE__{} = merger) do
    %{merger | workers_failed: merger.workers_failed + 1}
  end

  @doc """
  Finalize the accumulator into a list of row maps.
  """
  def finalize(%__MODULE__{} = merger) do
    Enum.map(merger.accumulator, fn {group_key, agg_states} ->
      finalized =
        Map.new(agg_states, fn {col_name, {lattice, state}} ->
          {col_name, lattice.finalize(state)}
        end)

      Map.merge(group_key, finalized)
    end)
  end

  @doc """
  Convert finalized rows to a `%Dux{}` struct.
  """
  def to_dux(%__MODULE__{} = merger) do
    rows = finalize(merger)

    if rows == [] do
      Dux.from_query("SELECT 1 WHERE false") |> Dux.compute()
    else
      Dux.from_list(rows) |> Dux.compute()
    end
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
  # These are the worker output columns (after PipelineSplitter rewrite).
  # SUM(x) → Sum, COUNT(x) → Count (re-aggregated as SUM), MIN → Min, MAX → Max
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

  defp extract_group_key(row, groups) do
    Map.take(row, groups)
  end

  defp init_group(agg_columns) do
    Map.new(agg_columns, fn {name, lattice} ->
      {name, {lattice, lattice.bottom()}}
    end)
  end

  defp merge_row(group_state, row, agg_columns) do
    Enum.reduce(agg_columns, group_state, fn {name, _lattice}, state ->
      {lattice, current} = Map.fetch!(state, name)
      value = Map.get(row, name, lattice.bottom())

      # Coerce nil to bottom
      value = if is_nil(value), do: lattice.bottom(), else: value

      Map.put(state, name, {lattice, lattice.merge(current, value)})
    end)
  end
end
