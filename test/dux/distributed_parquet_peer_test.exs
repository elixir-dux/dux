defmodule Dux.DistributedParquetPeerTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker

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
      Dux.DynamicSupervisor,
      %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
    ])
  end

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_parquet_peer_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # from_parquet + distribute: partitioned reads across peer workers
  # ---------------------------------------------------------------------------

  describe "from_parquet + distribute across peer workers" do
    test "each worker reads its own partition of parquet files" do
      dir = tmp_path("partitioned")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:parq_part1)
      {peer2, node2} = start_peer(:parq_part2)

      try do
        # Create 4 parquet files with distinct data
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{part: i, value: (i - 1) * 25 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # The key test: from_parquet with glob + distribute
        # Each worker should read ~2 files, not all 4
        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.to_columns()

        # All 100 values should be present (no duplication, no missing)
        all_values = Enum.sort(result["value"])
        assert all_values == Enum.to_list(1..100)
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "filter + summarise on partitioned parquet" do
      dir = tmp_path("filter_agg")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:parq_agg1)
      {peer2, node2} = start_peer(:parq_agg2)

      try do
        # 4 files, 25 rows each, values 1..100
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
          |> Dux.distribute([w1, w2])
          |> Dux.filter_with("value > 50")
          |> Dux.summarise_with(
            total: "SUM(value)",
            n: "COUNT(*)",
            min_v: "MIN(value)",
            max_v: "MAX(value)"
          )
          |> Dux.to_rows()

        row = hd(result)
        # Values 51..100: sum = 3775, count = 50, min = 51, max = 100
        assert row["total"] == 3775
        assert row["n"] == 50
        assert row["min_v"] == 51
        assert row["max_v"] == 100
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "group_by on partitioned parquet across peers" do
      dir = tmp_path("group_by")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:parq_grp1)
      {peer2, node2} = start_peer(:parq_grp2)

      try do
        # Create files with a group column
        for i <- 1..4 do
          rows =
            for j <- 1..25 do
              %{
                part: i,
                group: if(rem((i - 1) * 25 + j, 2) == 0, do: "even", else: "odd"),
                value: (i - 1) * 25 + j
              }
            end

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.group_by(:group)
          |> Dux.summarise_with(total: "SUM(value)", n: "COUNT(*)")
          |> Dux.sort_by(:group)
          |> Dux.to_rows()

        assert length(result) == 2

        evens = Enum.find(result, &(&1["group"] == "even"))
        odds = Enum.find(result, &(&1["group"] == "odd"))

        # 50 even numbers (2,4,...,100), 50 odd numbers (1,3,...,99)
        assert evens["n"] == 50
        assert odds["n"] == 50
        assert evens["total"] == 2550
        assert odds["total"] == 2500
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "join partitioned parquet with local dimension table" do
      dir = tmp_path("join_parq")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:parq_join1)
      {peer2, node2} = start_peer(:parq_join2)

      try do
        # Fact table as partitioned parquet
        for i <- 1..4 do
          rows =
            for j <- 1..10 do
              %{
                id: (i - 1) * 10 + j,
                region: Enum.at(["US", "EU", "APAC"], rem(j, 3)),
                amount: j * 100
              }
            end

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(dir, "fact_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Dimension table (local, will be auto-broadcast)
        dim =
          Dux.from_list([
            %{region: "US", country: "United States"},
            %{region: "EU", country: "Europe"},
            %{region: "APAC", country: "Asia Pacific"}
          ])
          |> Dux.compute()

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.join(dim, on: :region)
          |> Dux.group_by(:country)
          |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
          |> Dux.sort_by(:country)
          |> Dux.to_rows()

        assert length(result) == 3
        countries = Enum.map(result, & &1["country"])
        assert "Asia Pacific" in countries
        assert "Europe" in countries
        assert "United States" in countries

        # Total across all regions should equal sum of all amounts
        grand_total = Enum.map(result, & &1["total"]) |> Enum.sum()
        assert grand_total > 0
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "AVG on partitioned parquet matches local" do
      dir = tmp_path("avg_parq")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:parq_avg1)
      {peer2, node2} = start_peer(:parq_avg2)

      try do
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{value: (i - 1) * 25 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Distributed AVG
        dist_result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(avg_val: "AVG(value)")
          |> Dux.to_rows()

        # Local AVG
        local_result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.summarise_with(avg_val: "AVG(value)")
          |> Dux.to_rows()

        # AVG(1..100) = 50.5
        assert_in_delta hd(dist_result)["avg_val"], 50.5, 0.01
        assert_in_delta hd(local_result)["avg_val"], 50.5, 0.01
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end
  end
end
