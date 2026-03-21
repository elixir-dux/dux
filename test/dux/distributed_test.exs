defmodule Dux.DistributedTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.{Coordinator, Worker}

  @moduletag :distributed
  @moduletag timeout: 60_000

  # ---------------------------------------------------------------------------
  # Peer node helpers
  #
  # Uses :peer with connection: :standard_io so it works WITHOUT the parent
  # needing to be a distributed node. Communication goes through the peer
  # control channel, not Erlang distribution.
  # ---------------------------------------------------------------------------

  defp start_peer(name) do
    unless Node.alive?() do
      raise "distributed tests require a named node — see test_helper.exs"
    end

    # Collect all code paths so the peer can find compiled modules + NIF
    pa_args =
      :code.get_path()
      |> Enum.flat_map(fn path -> [~c"-pa", path] end)

    # Use Erlang distribution so :pg groups, GenServer.call, and :erpc
    # work across nodes. Requires parent to be alive (Node.alive?() == true).
    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        args: pa_args
      })

    # Start the dux application on the peer
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:dux])

    {peer, node}
  end

  defp stop_peer(peer) do
    :peer.stop(peer)
  end

  defp start_worker_on(node) do
    :erpc.call(node, DynamicSupervisor, :start_child, [
      Dux.Remote.HolderSupervisor,
      %{
        id: Worker,
        start: {Worker, :start_link, [[]]},
        restart: :temporary
      }
    ])
  end

  # ---------------------------------------------------------------------------
  # Peer node: worker discovery
  # ---------------------------------------------------------------------------

  describe "peer node worker discovery" do
    test "workers on peer nodes are discoverable via :pg" do
      {peer, node} = start_peer(:dist_disc1)

      try do
        {:ok, remote_worker} =
          start_worker_on(node)

        # Give pg time to propagate
        Process.sleep(200)

        # Workers from the peer should be visible
        all_workers = Worker.list()
        remote_workers = Enum.filter(all_workers, &(node(&1) == node))

        assert remote_workers != []
      after
        stop_peer(peer)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-node pipeline execution
  # ---------------------------------------------------------------------------

  describe "cross-node pipeline execution" do
    test "execute simple pipeline on remote worker" do
      {peer, node} = start_peer(:dist_exec1)

      try do
        {:ok, remote_worker} =
          start_worker_on(node)

        pipeline = Dux.from_query("SELECT 42 AS answer, 'hello' AS greeting")
        {:ok, ipc} = Worker.execute(remote_worker, pipeline)

        table = Dux.Native.table_from_ipc(ipc)
        cols = Dux.Native.table_to_columns(table)

        assert cols["answer"] == [42]
        assert cols["greeting"] == ["hello"]
      after
        stop_peer(peer)
      end
    end

    test "execute filter + mutate on remote worker" do
      {peer, node} = start_peer(:dist_exec2)

      try do
        {:ok, remote_worker} =
          start_worker_on(node)

        pipeline =
          Dux.from_query("SELECT * FROM range(1, 11) t(x)")
          |> Dux.filter(x > 5)
          |> Dux.mutate(doubled: x * 2)

        {:ok, ipc} = Worker.execute(remote_worker, pipeline)
        table = Dux.Native.table_from_ipc(ipc)
        cols = Dux.Native.table_to_columns(table)

        assert cols["x"] == [6, 7, 8, 9, 10]
        assert cols["doubled"] == [12, 14, 16, 18, 20]
      after
        stop_peer(peer)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Coordinator with peer workers
  # ---------------------------------------------------------------------------

  describe "coordinator with peer workers" do
    test "coordinator fans out to multiple peer workers" do
      {peer1, node1} = start_peer(:dist_coord1)
      {peer2, node2} = start_peer(:dist_coord2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)

        Process.sleep(200)

        result =
          Dux.from_query("SELECT 1 AS x")
          |> Coordinator.execute(workers: [w1, w2])

        # Each worker returns x=1, merger concatenates → 2 rows
        assert Dux.n_rows(result) == 2
      after
        stop_peer(peer1)
        stop_peer(peer2)
      end
    end

    test "distributed aggregation across peer workers" do
      {peer1, node1} = start_peer(:dist_agg1)
      {peer2, node2} = start_peer(:dist_agg2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)

        Process.sleep(200)

        result =
          Dux.from_query("SELECT * FROM range(1, 11) t(x)")
          |> Dux.summarise_with(total: "SUM(x)")
          |> Coordinator.execute(workers: [w1, w2])

        cols = Dux.to_columns(result)
        # Each worker sums 1..10=55, merger re-aggregates: 55+55=110
        assert hd(cols["total"]) == 110
      after
        stop_peer(peer1)
        stop_peer(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Broadcast table on peer workers
  # ---------------------------------------------------------------------------

  describe "broadcast table on peer workers" do
    test "register and query broadcast table on remote worker" do
      {peer, node} = start_peer(:dist_bcast1)

      try do
        {:ok, remote_worker} =
          start_worker_on(node)

        # Create dimension data locally, serialize to IPC
        db = Dux.Connection.get_db()

        dim =
          Dux.Native.df_query(
            db,
            "SELECT 1 AS id, 'Widget' AS name UNION ALL SELECT 2, 'Gadget'"
          )

        dim_ipc = Dux.Native.table_to_ipc(dim)

        # Register on remote worker
        {:ok, "products"} = Worker.register_table(remote_worker, "products", dim_ipc)

        # Query the broadcast table through the remote worker
        pipeline = Dux.from_query(~s(SELECT * FROM "products" ORDER BY id))
        {:ok, result_ipc} = Worker.execute(remote_worker, pipeline)
        table = Dux.Native.table_from_ipc(result_ipc)
        cols = Dux.Native.table_to_columns(table)

        assert cols["id"] == [1, 2]
        assert cols["name"] == ["Widget", "Gadget"]
      after
        stop_peer(peer)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Node disconnection
  # ---------------------------------------------------------------------------

  describe "node disconnection" do
    test "peer worker leaves pg group when node stops" do
      {peer, node} = start_peer(:dist_disconn1)

      {:ok, _remote_worker} =
        start_worker_on(node)

      Process.sleep(200)

      remote_workers = Enum.filter(Worker.list(), &(node(&1) == node))
      assert remote_workers != []

      stop_peer(peer)
      Process.sleep(500)

      remote_workers_after = Enum.filter(Worker.list(), &(node(&1) == node))
      assert remote_workers_after == []
    end
  end
end
