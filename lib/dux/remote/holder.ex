defmodule Dux.Remote.Holder do
  @moduledoc false

  # GenServer spawned on the origin node (the node that owns the DuckDB temp table).
  # Keeps a reference to the ResourceArc alive, preventing the Drop impl from
  # firing and dropping the temp table.
  #
  # Lifecycle:
  # 1. Spawned by Remote.place/1 via :erpc on the origin node
  # 2. Monitors the LocalGC process on the caller's node
  # 3. Holds the resource_ref (ResourceArc) in state, keeping it alive
  # 4. Receives {:gc, ref} when the caller's GC sentinel fires → removes ref
  # 5. Receives :DOWN if the caller node disconnects → exits, releasing all refs
  # 6. Stops when all refs are released

  use GenServer

  @doc false
  def start_child(resource_ref, local_gc_pid) do
    DynamicSupervisor.start_child(
      Dux.Remote.HolderSupervisor,
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [{resource_ref, local_gc_pid}]},
        restart: :temporary
      }
    )
  end

  def start_link({resource_ref, local_gc_pid}) do
    GenServer.start_link(__MODULE__, {resource_ref, local_gc_pid})
  end

  @impl true
  def init({resource_ref, local_gc_pid}) do
    # Monitor the remote LocalGC process — if the remote node disconnects,
    # we get :DOWN and release all refs
    Process.monitor(local_gc_pid)
    {:ok, %{refs: %{make_ref() => resource_ref}, local_gc_pid: local_gc_pid}}
  end

  # GC sentinel on the remote node fired — the caller no longer needs this ref
  @impl true
  def handle_info({:gc, _resource_ref}, state) do
    # For now we only hold one ref per holder.
    # Remove it and stop.
    {:stop, :normal, state}
  end

  # Remote LocalGC process died or node disconnected
  @impl true
  def handle_info({:DOWN, _monitor_ref, :process, _pid, _reason}, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
