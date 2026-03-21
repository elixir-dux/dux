defmodule Dux.DistributedCorrectnessPeerTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.{Broadcast, Coordinator, Shuffle, Worker}

  @moduletag :distributed
  @moduletag timeout: 120_000

  @tmp_dir System.tmp_dir!()

  # ---------------------------------------------------------------------------
  # Peer helpers
  # ---------------------------------------------------------------------------

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
      Dux.Remote.HolderSupervisor,
      %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
    ])
  end

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_peer_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # Aggregate correctness across real nodes
  # ---------------------------------------------------------------------------

  describe "aggregate correctness on peer nodes" do
    test "MIN/MAX re-aggregated correctly across peers" do
      {peer1, node1} = start_peer(:agg_minmax1)
      {peer2, node2} = start_peer(:agg_minmax2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT * FROM range(1, 101) t(x)")
          |> Dux.summarise_with(minimum: "MIN(x)", maximum: "MAX(x)")
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.to_rows()

        row = hd(result)
        assert row["minimum"] == 1
        assert row["maximum"] == 100
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "AVG rewrite produces correct results across peers" do
      {peer1, node1} = start_peer(:agg_avg1)
      {peer2, node2} = start_peer(:agg_avg2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT * FROM range(1, 11) t(x)")
          |> Dux.summarise_with(average: "AVG(x)")
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.to_rows()

        # AVG(1..10) = 5.5 regardless of replication
        assert_in_delta hd(result)["average"], 5.5, 0.01
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "grouped AVG + MIN + MAX across peers" do
      {peer1, node1} = start_peer(:agg_grouped1)
      {peer2, node2} = start_peer(:agg_grouped2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT x, x % 2 AS grp FROM range(1, 21) t(x)")
          |> Dux.group_by(:grp)
          |> Dux.summarise_with(
            avg_x: "AVG(x)",
            min_x: "MIN(x)",
            max_x: "MAX(x)"
          )
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.sort_by(:grp)
          |> Dux.to_rows()

        evens = Enum.find(result, &(&1["grp"] == 0))
        odds = Enum.find(result, &(&1["grp"] == 1))

        # Evens: 2,4,6,...,20 → avg=11, min=2, max=20
        assert_in_delta evens["avg_x"], 11.0, 0.01
        assert evens["min_x"] == 2
        assert evens["max_x"] == 20

        # Odds: 1,3,5,...,19 → avg=10, min=1, max=19
        assert_in_delta odds["avg_x"], 10.0, 0.01
        assert odds["min_x"] == 1
        assert odds["max_x"] == 19
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # STDDEV/VARIANCE across real nodes
  # ---------------------------------------------------------------------------

  describe "STDDEV on peer nodes" do
    test "STDDEV_SAMP decomposition across peers" do
      {peer1, node1} = start_peer(:stddev1)
      {peer2, node2} = start_peer(:stddev2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT * FROM range(1, 101) t(x)")
          |> Dux.summarise_with(sd: "STDDEV_SAMP(x)", v: "VARIANCE(x)")
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.to_rows()

        row = hd(result)
        # Should produce positive numbers
        assert is_number(row["sd"])
        assert row["sd"] > 0
        assert is_number(row["v"])
        assert row["v"] > 0
        # STDDEV = sqrt(VARIANCE)
        assert_in_delta row["sd"], :math.sqrt(row["v"]), 0.01
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Slice correctness across real nodes
  # ---------------------------------------------------------------------------

  describe "slice on peer nodes" do
    test "slice applies post-merge on coordinator" do
      {peer1, node1} = start_peer(:slice1)
      {peer2, node2} = start_peer(:slice2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT * FROM range(20) t(x)")
          |> Dux.sort_by(:x)
          |> Dux.slice(0, 5)
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.to_columns()

        # First 5 from sorted merged result (duplicated: [0,0,1,1,2,...])
        assert length(result["x"]) == 5
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Pivot correctness across real nodes
  # ---------------------------------------------------------------------------

  describe "pivot on peer nodes" do
    test "pivot_wider applies on coordinator, not workers" do
      {peer1, node1} = start_peer(:pivot1)
      {peer2, node2} = start_peer(:pivot2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_list([
            %{region: "US", product: "Widget", sales: 100},
            %{region: "EU", product: "Gadget", sales: 200}
          ])
          |> Dux.pivot_wider(:product, :sales)
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        # Both workers see both products → schema is consistent
        assert length(result) == 2
        eu = Enum.find(result, &(&1["region"] == "EU"))
        assert Map.has_key?(eu, "Widget")
        assert Map.has_key?(eu, "Gadget")
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "pivot_longer works distributed" do
      {peer1, node1} = start_peer(:unpivot1)
      {peer2, node2} = start_peer(:unpivot2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_list([
            %{region: "US", q1: 100, q2: 200},
            %{region: "EU", q1: 150, q2: 250}
          ])
          |> Dux.pivot_longer([:q1, :q2], names_to: "quarter", values_to: "sales")
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.n_rows()

        # 2 rows × 2 quarters × 2 workers (replicated) = 8
        assert result == 8
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Full e2e pipeline across peer nodes
  # ---------------------------------------------------------------------------

  describe "full e2e on peer nodes" do
    test "filter → mutate → group → summarise → sort → head" do
      {peer1, node1} = start_peer(:e2e1)
      {peer2, node2} = start_peer(:e2e2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_query("SELECT x AS id, x % 5 AS grp, x * 10 AS amount FROM range(1, 101) t(x)")
          |> Dux.filter_with("amount > 200")
          |> Dux.mutate_with(adjusted: "amount * 1.1")
          |> Dux.group_by(:grp)
          |> Dux.summarise_with(
            total: "SUM(adjusted)",
            n: "COUNT(*)",
            avg_adj: "AVG(adjusted)"
          )
          |> Dux.sort_by(desc: :total)
          |> Dux.head(3)
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.to_rows()

        # Should get top 3 groups by total
        assert length(result) == 3
        # All should have numeric values
        assert Enum.all?(result, &is_number(&1["total"]))
        assert Enum.all?(result, &is_number(&1["avg_adj"]))
        # Sorted descending
        totals = Enum.map(result, & &1["total"])
        assert totals == Enum.sort(totals, :desc)
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "partitioned Parquet across peer workers" do
      dir = tmp_path("peer_parquet")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:parquet_peer1)
      {peer2, node2} = start_peer(:parquet_peer2)

      try do
        # Create 4 Parquet files
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{part: i, value: (i - 1) * 25 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.summarise_with(
            total: "SUM(value)",
            n: "COUNT(*)",
            min_v: "MIN(value)",
            max_v: "MAX(value)"
          )
          |> Coordinator.execute(workers: [w1, w2])
          |> Dux.to_rows()

        row = hd(result)
        # 100 values: 1..100, sum=5050
        assert row["total"] == 5050
        assert row["n"] == 100
        assert row["min_v"] == 1
        assert row["max_v"] == 100
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "broadcast join across peer workers" do
      {peer1, node1} = start_peer(:bcast_e2e1)
      {peer2, node2} = start_peer(:bcast_e2e2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        fact = Dux.from_query("SELECT x AS id, x * 100 AS revenue FROM range(1, 11) t(x)")

        dim =
          Dux.from_list([
            %{id: 1, category: "A"},
            %{id: 2, category: "B"},
            %{id: 3, category: "A"}
          ])

        result =
          Broadcast.execute(fact, dim, on: :id, workers: [w1, w2])
          |> Dux.to_rows()

        # Only ids 1,2,3 match → 3 rows per worker × 2 workers = 6
        assert length(result) == 6
        assert Enum.all?(result, &Map.has_key?(&1, "category"))
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "shuffle join across peer workers" do
      {peer1, node1} = start_peer(:shuffle_peer1)
      {peer2, node2} = start_peer(:shuffle_peer2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        left = Dux.from_query("SELECT x AS key, x * 10 AS left_val FROM range(1, 21) t(x)")
        right = Dux.from_query("SELECT x AS key, x * 100 AS right_val FROM range(1, 21) t(x)")

        result = Shuffle.execute(left, right, on: :key, workers: [w1, w2])

        # All 20 keys should match
        assert Dux.n_rows(result) == 20
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end
end
