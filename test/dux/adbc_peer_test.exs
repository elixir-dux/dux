defmodule Dux.AdbcPeerTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker
  alias Dux.Test.Datasets

  @moduletag :distributed
  @moduletag timeout: 120_000

  defp start_peer(name) do
    unless Node.alive?() do
      raise "distributed tests require a named node — see test_helper.exs"
    end

    pa_args =
      :code.get_path()
      |> Enum.flat_map(fn path -> [~c"-pa", path] end)

    {:ok, peer, node} = :peer.start(%{name: name, args: pa_args})
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:dux])
    {peer, node}
  end

  defp start_worker_on(node) do
    :erpc.call(node, DynamicSupervisor, :start_child, [
      Dux.DynamicSupervisor,
      %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
    ])
  end

  # ---------------------------------------------------------------------------
  # IPC type fidelity across nodes
  # ---------------------------------------------------------------------------

  describe "IPC type fidelity across peer nodes" do
    test "integer, float, string, boolean survive cross-node IPC" do
      {peer, node} = start_peer(:ipc_types1)

      try do
        {:ok, w} = start_worker_on(node)
        Process.sleep(200)

        result =
          Dux.from_query("""
            SELECT 42 AS int_col, 3.14 AS float_col,
                   'hello' AS str_col, true AS bool_col
          """)
          |> Dux.distribute([w])
          |> Dux.to_rows()

        row = hd(result)
        assert row["int_col"] == 42
        assert_in_delta row["float_col"], 3.14, 0.01
        assert row["str_col"] == "hello"
        assert row["bool_col"] == true
      after
        :peer.stop(peer)
      end
    end

    test "NULL values survive cross-node IPC" do
      {peer, node} = start_peer(:ipc_null1)

      try do
        {:ok, w} = start_worker_on(node)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT NULL::INTEGER AS x, NULL::VARCHAR AS y")
          |> Dux.distribute([w])
          |> Dux.to_rows()

        row = hd(result)
        assert row["x"] == nil
        assert row["y"] == nil
      after
        :peer.stop(peer)
      end
    end

    test "Decimal normalization works across nodes" do
      {peer, node} = start_peer(:ipc_decimal1)

      try do
        {:ok, w} = start_worker_on(node)
        Process.sleep(200)

        result =
          Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
          |> Dux.distribute([w])
          |> Dux.summarise_with(total: "SUM(x)")
          |> Dux.to_rows()

        total = hd(result)["total"]
        assert is_integer(total)
        assert total == 60
      after
        :peer.stop(peer)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Chained distributed operations
  # ---------------------------------------------------------------------------

  describe "chained distributed operations" do
    test "compute → filter → distribute again" do
      {peer1, node1} = start_peer(:chain1)
      {peer2, node2} = start_peer(:chain2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Step 1: distributed compute
        step1 =
          Dux.from_query("SELECT * FROM range(1, 21) t(x)")
          |> Dux.distribute([w1, w2])
          |> Dux.compute()

        # Step 2: local filter on the result
        step2 = step1 |> Dux.filter_with("x > 10") |> Dux.collect()

        # Step 3: distribute the filtered result again
        result =
          step2
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")
          |> Dux.to_rows()

        row = hd(result)
        assert row["total"] > 0
        assert row["n"] > 0
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "distributed group_by → collect → distributed join" do
      {peer1, node1} = start_peer(:chain_join1)
      {peer2, node2} = start_peer(:chain_join2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Step 1: distributed aggregation
        summary =
          Dux.from_list([
            %{region: "US", amount: 100},
            %{region: "EU", amount: 200},
            %{region: "US", amount: 150}
          ])
          |> Dux.distribute([w1, w2])
          |> Dux.group_by(:region)
          |> Dux.summarise_with(total: "SUM(amount)")
          |> Dux.collect()

        # Step 2: join with local dimension table
        dim =
          Dux.from_list([%{region: "US", name: "United States"}, %{region: "EU", name: "Europe"}])

        result =
          summary
          |> Dux.join(dim, on: :region)
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        names = Enum.map(result, & &1["name"]) |> Enum.uniq() |> Enum.sort()
        assert names == ["Europe", "United States"]
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Graph algorithms across peer nodes
  # ---------------------------------------------------------------------------

  describe "graph algorithms across peer nodes" do
    test "distributed connected components" do
      {peer1, node1} = start_peer(:graph_cc1)
      {peer2, node2} = start_peer(:graph_cc2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}, %{id: 4}])

        edges =
          Dux.from_list([
            %{src: 1, dst: 2},
            %{src: 2, dst: 1},
            %{src: 3, dst: 4},
            %{src: 4, dst: 3}
          ])

        graph =
          Dux.Graph.new(vertices: vertices, edges: edges)
          |> Dux.Graph.distribute([w1, w2])

        result = graph |> Dux.Graph.connected_components() |> Dux.sort_by(:id) |> Dux.to_columns()

        [c1, c2, c3, c4] = result["component"]
        assert c1 == c2
        assert c3 == c4
        assert c1 != c3
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "distributed shortest paths" do
      {peer1, node1} = start_peer(:graph_sp1)
      {peer2, node2} = start_peer(:graph_sp2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        vertices = Dux.from_list(Enum.map(1..5, &%{id: &1}))

        edges =
          Dux.from_list([
            %{src: 1, dst: 2},
            %{src: 2, dst: 3},
            %{src: 3, dst: 4},
            %{src: 4, dst: 5}
          ])

        graph =
          Dux.Graph.new(vertices: vertices, edges: edges)
          |> Dux.Graph.distribute([w1, w2])

        result =
          graph
          |> Dux.Graph.shortest_paths(1)
          |> Dux.sort_by(:node)
          |> Dux.to_columns()

        assert result["node"] == [1, 2, 3, 4, 5]
        assert result["dist"] == [0, 1, 2, 3, 4]
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "distributed triangle count" do
      {peer1, node1} = start_peer(:graph_tri1)
      {peer2, node2} = start_peer(:graph_tri2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

        edges =
          Dux.from_list([
            %{src: 1, dst: 2},
            %{src: 2, dst: 1},
            %{src: 2, dst: 3},
            %{src: 3, dst: 2},
            %{src: 1, dst: 3},
            %{src: 3, dst: 1}
          ])

        graph =
          Dux.Graph.new(vertices: vertices, edges: edges)
          |> Dux.Graph.distribute([w1, w2])

        assert Dux.Graph.triangle_count(graph) == 1
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Dataset-based peer tests
  # ---------------------------------------------------------------------------

  describe "real dataset operations across peers" do
    test "nycflights star schema join across peers" do
      {peer1, node1} = start_peer(:flights1)
      {peer2, node2} = start_peer(:flights2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        airlines = Datasets.airlines() |> Dux.compute()

        result =
          Datasets.flights()
          |> Dux.distribute([w1, w2])
          |> Dux.join(airlines, on: :carrier)
          |> Dux.group_by(:name)
          |> Dux.summarise_with(n: "COUNT(*)")
          |> Dux.sort_by(desc: :n)
          |> Dux.head()
          |> Dux.to_rows()

        assert result != []
        assert Enum.all?(result, &Map.has_key?(&1, "name"))
        assert Enum.all?(result, &(&1["n"] > 0))
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "penguins group_by across peers" do
      {peer1, node1} = start_peer(:penguins1)
      {peer2, node2} = start_peer(:penguins2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Datasets.penguins()
          |> Dux.distribute([w1, w2])
          |> Dux.drop_nil([:body_mass_g])
          |> Dux.group_by(:species)
          |> Dux.summarise_with(avg_mass: "AVG(body_mass_g)", n: "COUNT(*)")
          |> Dux.sort_by(:species)
          |> Dux.to_rows()

        species = Enum.map(result, & &1["species"]) |> Enum.uniq() |> Enum.sort()
        assert species == ["Adelie", "Chinstrap", "Gentoo"]
        assert Enum.all?(result, &(&1["avg_mass"] > 0))
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 3-worker peer tests
  # ---------------------------------------------------------------------------

  describe "3-worker peer operations" do
    test "3 peer workers with AVG rewrite" do
      {peer1, node1} = start_peer(:three_w1)
      {peer2, node2} = start_peer(:three_w2)
      {peer3, node3} = start_peer(:three_w3)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        {:ok, w3} = start_worker_on(node3)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT * FROM range(1, 11) t(x)")
          |> Dux.distribute([w1, w2, w3])
          |> Dux.summarise_with(average: "AVG(x)")
          |> Dux.to_rows()

        # AVG(1..10) = 5.5 regardless of replication
        assert_in_delta hd(result)["average"], 5.5, 0.01
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        :peer.stop(peer3)
      end
    end
  end
end
