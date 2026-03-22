defmodule Dux.DistributedTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker

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
        {:ok, _remote_worker} =
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

        conn = Dux.Connection.get_conn()
        ref = Dux.Backend.table_from_ipc(conn, ipc)
        cols = Dux.Backend.table_to_columns(conn, ref)

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
        conn = Dux.Connection.get_conn()
        ref = Dux.Backend.table_from_ipc(conn, ipc)
        cols = Dux.Backend.table_to_columns(conn, ref)

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
          |> Dux.distribute([w1, w2])
          |> Dux.compute()

        # Each worker returns x=1, merger concatenates → 2 rows
        assert Dux.n_rows(result) == 2
        # Workers are preserved after compute
        assert result.workers == [w1, w2]
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
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(total: "SUM(x)")
          |> Dux.to_columns()

        # Each worker sums 1..10=55, merger re-aggregates: 55+55=110
        assert hd(result["total"]) == 110
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
        conn = Dux.Connection.get_conn()

        dim =
          Dux.Backend.query(
            conn,
            "SELECT 1 AS id, 'Widget' AS name UNION ALL SELECT 2, 'Gadget'"
          )

        dim_ipc = Dux.Backend.table_to_ipc(conn, dim)

        # Register on remote worker
        {:ok, "products"} = Worker.register_table(remote_worker, "products", dim_ipc)

        # Query the broadcast table through the remote worker
        pipeline = Dux.from_query(~s(SELECT * FROM "products" ORDER BY id))
        {:ok, result_ipc} = Worker.execute(remote_worker, pipeline)
        ref = Dux.Backend.table_from_ipc(conn, result_ipc)
        cols = Dux.Backend.table_to_columns(conn, ref)

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

  # ---------------------------------------------------------------------------
  # Full distributed workflow: create → distribute → aggregate → collect
  # ---------------------------------------------------------------------------

  describe "full distributed workflow" do
    test "create locally → fan out to workers → aggregate → collect result" do
      {peer1, node1} = start_peer(:dist_workflow1)
      {peer2, node2} = start_peer(:dist_workflow2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)

        Process.sleep(200)

        # Create a big-ish dataset locally
        data =
          for i <- 1..1000 do
            %{
              "id" => i,
              "region" => Enum.at(["US", "EU", "APAC"], rem(i, 3)),
              "amount" => i * 10
            }
          end

        # Build the pipeline: filter → group → aggregate
        pipeline =
          Dux.from_list(data)
          |> Dux.filter_with("amount > 500")
          |> Dux.group_by(:region)
          |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")

        # Distribute across 2 peer workers and collect
        result =
          pipeline
          |> Dux.distribute([w1, w2])
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        # Verify the result makes sense
        # Both workers process the same data (replicated source), so
        # the merger re-aggregates: SUM of partial SUMs, SUM of partial COUNTs
        assert length(result) == 3

        regions = Enum.map(result, & &1["region"])
        assert regions == ["APAC", "EU", "US"]

        # Each region's count should be doubled (2 workers)
        # Original: ~950 rows with amount > 500, split ~317 per region
        # With 2 workers replicating: 2x
        totals = Enum.map(result, & &1["total"])
        assert Enum.all?(totals, &(&1 > 0))

        # Verify locally for correctness
        local_result =
          Dux.from_list(data)
          |> Dux.filter_with("amount > 500")
          |> Dux.group_by(:region)
          |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        # Distributed totals should be 2x local (replicated source)
        for {local, dist} <- Enum.zip(local_result, result) do
          assert dist["total"] == local["total"] * 2
          assert dist["n"] == local["n"] * 2
          assert dist["region"] == local["region"]
        end
      after
        stop_peer(peer1)
        stop_peer(peer2)
      end
    end

    test "broadcast join: dimension table on workers, fact table distributed" do
      {peer1, node1} = start_peer(:dist_bcast_join1)
      {peer2, node2} = start_peer(:dist_bcast_join2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)

        Process.sleep(200)

        # Create dimension table (small) and broadcast to all workers
        conn = Dux.Connection.get_conn()

        dim =
          Dux.Backend.query(conn, """
            SELECT 0 AS region_id, 'US' AS region_name
            UNION ALL SELECT 1, 'EU'
            UNION ALL SELECT 2, 'APAC'
          """)

        dim_ipc = Dux.Backend.table_to_ipc(conn, dim)

        # Broadcast to both workers
        {:ok, _} = Worker.register_table(w1, "regions", dim_ipc)
        {:ok, _} = Worker.register_table(w2, "regions", dim_ipc)

        # Execute a pipeline that joins against the broadcast table
        pipeline =
          Dux.from_query("""
            SELECT x % 3 AS region_id, x * 10 AS amount
            FROM range(1, 101) t(x)
          """)
          |> Dux.join(
            Dux.from_query(~s(SELECT * FROM "regions")),
            on: :region_id
          )
          |> Dux.group_by(:region_name)
          |> Dux.summarise_with(total: "SUM(amount)")

        result = pipeline |> Dux.distribute([w1, w2]) |> Dux.compute()
        rows = Dux.sort_by(result, :region_name) |> Dux.to_rows()

        # Each worker processes the full source (replicated), merger re-aggregates
        region_names = Enum.map(rows, & &1["region_name"]) |> Enum.uniq() |> Enum.sort()
        assert region_names == ["APAC", "EU", "US"]
        assert Enum.all?(rows, &(&1["total"] > 0))
      after
        stop_peer(peer1)
        stop_peer(peer2)
      end
    end
  end
end
