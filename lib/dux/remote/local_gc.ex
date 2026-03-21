defmodule Dux.Remote.LocalGC do
  @moduledoc false

  # GenServer on each node that receives :gc notifications from
  # NIF GC sentinel destructors and forwards them to Holder processes.
  #
  # The NIF destructor can only send to a local PID (enif_send limitation),
  # so LocalGC acts as a local relay that forwards the message cross-node
  # to the Holder on the origin node.

  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc false
  def pid do
    Process.whereis(__MODULE__)
  end

  @impl true
  def init([]) do
    {:ok, %{}}
  end

  # Received from NIF GC sentinel destructor.
  # Forward to the Holder on the origin node.
  @impl true
  def handle_info({:gc, holder_pid, resource_ref}, state) do
    send(holder_pid, {:gc, resource_ref})
    {:noreply, state}
  end

  # Ignore unexpected messages
  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
