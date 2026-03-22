defmodule Dux.Remote.Broadcast do
  @moduledoc """
  Broadcast join support for distributed queries.

  When joining a large partitioned dataset with a small dimension table,
  the dimension table is serialized as Arrow IPC, broadcast to all workers,
  and registered as a temp table. Each worker then joins its data partition
  against the local copy of the dimension table.

  This is the star-schema pattern — the most common distributed join.

  ## Usage

      # Automatic: coordinator detects small right side
      Dux.Remote.Coordinator.execute(
        fact_table
        |> Dux.join(dim_table, on: :id),
        workers: workers
      )

      # Explicit: force broadcast of the right side
      Dux.Remote.Broadcast.execute(
        fact_pipeline,
        dim_dux,
        on: :region_id,
        workers: workers
      )
  """

  import Dux.SQL.Helpers, only: [qi: 1]
  alias Dux.Remote.{Merger, Partitioner, Worker}

  # Default broadcast threshold: 256MB serialized Arrow IPC
  @default_threshold 256 * 1024 * 1024

  @doc """
  Execute a broadcast join: broadcast the small (right) table to all workers,
  then each worker joins its partition of the large (left) table locally.

  ## Options

    * `:workers` — list of worker PIDs (default: all from `:pg`)
    * `:on` — join column(s) (required)
    * `:how` — join type (default: `:inner`)
    * `:broadcast_name` — name for the broadcast table on workers (auto-generated if nil)
    * `:timeout` — per-worker timeout (default: `:infinity`)

  Returns a `%Dux{}` with the merged join result.
  """
  def execute(%Dux{} = left, %Dux{} = right, opts \\ []) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    on = Keyword.fetch!(opts, :on)
    how = Keyword.get(opts, :how, :inner)
    timeout = Keyword.get(opts, :timeout, :infinity)

    broadcast_name =
      Keyword.get_lazy(opts, :broadcast_name, fn ->
        "__broadcast_#{:erlang.unique_integer([:positive])}"
      end)

    if workers == [] do
      raise ArgumentError, "no workers available for broadcast join"
    end

    # Step 1: Compute the small table on the coordinator and serialize to IPC
    right_computed = Dux.compute(right)
    {:table, right_ref} = right_computed.source
    right_ipc = Dux.Backend.table_to_ipc(Dux.Connection.get_conn(), right_ref)

    # Step 2: Broadcast to all workers
    broadcast_to_workers(workers, broadcast_name, right_ipc)

    # Step 3: Rewrite the left pipeline to join against the broadcast table
    on_spec = normalize_on(on)

    join_pipeline =
      left
      |> Dux.join(
        Dux.from_query("SELECT * FROM #{qi(broadcast_name)}"),
        on: on_spec,
        how: how
      )

    # Step 4: Fan out the join pipeline to workers
    assignments = Partitioner.assign(join_pipeline, workers)

    results =
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
        {:ok, {:ok, ipc}} -> ipc
        {:ok, {:error, reason}} -> raise "Worker failed: #{reason}"
        {:exit, reason} -> raise "Worker crashed: #{inspect(reason)}"
      end)

    # Step 5: Merge results on coordinator
    result = Merger.merge_to_dux(results, join_pipeline)

    # Step 6: Cleanup broadcast tables on all workers
    cleanup_broadcast(workers, broadcast_name)

    result
  end

  @doc """
  Check if a table is small enough to broadcast.

  Computes the table and checks its serialized Arrow IPC size
  against the threshold.
  """
  def should_broadcast?(%Dux{} = dux, threshold \\ @default_threshold) do
    computed = Dux.compute(dux)
    {:table, ref} = computed.source
    ipc = Dux.Backend.table_to_ipc(Dux.Connection.get_conn(), ref)
    byte_size(ipc) <= threshold
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp broadcast_to_workers(workers, name, ipc_binary) do
    # Broadcast in parallel
    tasks =
      Enum.map(workers, fn worker ->
        Task.async(fn ->
          Worker.register_table(worker, name, ipc_binary)
        end)
      end)

    Task.await_many(tasks, 30_000)
  end

  defp cleanup_broadcast(workers, name) do
    # Best-effort cleanup — don't crash if a worker is already dead
    Enum.each(workers, fn worker ->
      try do
        Worker.drop_table(worker, name)
      catch
        _, _ -> :ok
      end
    end)
  end

  defp normalize_on(col) when is_atom(col), do: [col]
  defp normalize_on(col) when is_binary(col), do: [String.to_atom(col)]
  defp normalize_on(cols) when is_list(cols), do: cols
end
