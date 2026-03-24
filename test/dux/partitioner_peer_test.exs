defmodule Dux.PartitionerPeerTest do
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
    Path.join(@tmp_dir, "dux_part_peer_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # Size-balanced reads: end-to-end with real peer workers
  # ---------------------------------------------------------------------------

  describe "size-balanced partitioning across peer workers" do
    test "skewed files produce correct results with balanced assignment" do
      dir = tmp_path("skewed")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:part_skew1)
      {peer2, node2} = start_peer(:part_skew2)

      try do
        # One large file (5000 rows) and five small files (20 rows each)
        Dux.from_query("SELECT x, x * 2 AS doubled FROM range(1, 5001) t(x)")
        |> Dux.to_parquet(Path.join(dir, "big.parquet"))

        for i <- 1..5 do
          start = 5000 + (i - 1) * 20 + 1
          stop = start + 19

          Dux.from_query("SELECT x, x * 2 AS doubled FROM range(#{start}, #{stop + 1}) t(x)")
          |> Dux.to_parquet(Path.join(dir, "small_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Distributed read with size-balanced assignment
        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(
            total: "SUM(x)",
            n: "COUNT(*)",
            min_x: "MIN(x)",
            max_x: "MAX(x)"
          )
          |> Dux.to_rows()

        row = hd(result)
        # 5000 + 5*20 = 5100 rows total, values 1..5100
        assert row["n"] == 5100
        assert row["min_x"] == 1
        assert row["max_x"] == 5100
        assert row["total"] == div(5100 * 5101, 2)
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "filter + group_by on skewed partitioned data" do
      dir = tmp_path("skewed_group")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:part_grp1)
      {peer2, node2} = start_peer(:part_grp2)
      {peer3, node3} = start_peer(:part_grp3)

      try do
        # Create files with different sizes and a group column
        for {name, n} <- [{"a_2000", 2000}, {"b_500", 500}, {"c_500", 500}, {"d_100", 100}] do
          Dux.from_query("""
            SELECT
              x,
              CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END AS parity
            FROM range(1, #{n + 1}) t(x)
          """)
          |> Dux.to_parquet(Path.join(dir, "#{name}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        {:ok, w3} = start_worker_on(node3)
        Process.sleep(200)

        # Local baseline
        local =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.filter(x > 100)
          |> Dux.group_by(:parity)
          |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")
          |> Dux.sort_by(:parity)
          |> Dux.to_rows()

        # Distributed
        distributed =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2, w3])
          |> Dux.filter(x > 100)
          |> Dux.group_by(:parity)
          |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")
          |> Dux.sort_by(:parity)
          |> Dux.to_rows()

        # Results should match exactly
        assert local == distributed
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        :peer.stop(peer3)
        File.rm_rf!(dir)
      end
    end

    test "AVG + STDDEV on skewed partitioned data matches local" do
      dir = tmp_path("skewed_stats")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:part_stat1)
      {peer2, node2} = start_peer(:part_stat2)

      try do
        # Highly skewed: one file with 3000 rows, three with 10 rows
        Dux.from_query("SELECT x::DOUBLE AS val FROM range(1, 3001) t(x)")
        |> Dux.to_parquet(Path.join(dir, "big.parquet"))

        for i <- 1..3 do
          start = 3000 + (i - 1) * 10 + 1

          Dux.from_query("SELECT x::DOUBLE AS val FROM range(#{start}, #{start + 10}) t(x)")
          |> Dux.to_parquet(Path.join(dir, "small_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        local =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.summarise_with(
            avg_val: "AVG(val)",
            std_val: "STDDEV_SAMP(val)",
            n: "COUNT(*)"
          )
          |> Dux.to_rows()

        distributed =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(
            avg_val: "AVG(val)",
            std_val: "STDDEV_SAMP(val)",
            n: "COUNT(*)"
          )
          |> Dux.to_rows()

        local_row = hd(local)
        dist_row = hd(distributed)

        assert local_row["n"] == dist_row["n"]
        assert_in_delta local_row["avg_val"], dist_row["avg_val"], 0.01
        assert_in_delta local_row["std_val"], dist_row["std_val"], 0.1
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # DuckLake-style file list distribution across peers
  # ---------------------------------------------------------------------------

  describe "ducklake_files source across peer workers" do
    test "coordinator-resolved file list distributes correctly" do
      dir = tmp_path("ducklake_sim")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:dl_sim1)
      {peer2, node2} = start_peer(:dl_sim2)

      try do
        # Simulate DuckLake: create parquet files as if resolved from catalog
        for i <- 1..6 do
          Dux.from_list([%{"id" => i, "value" => i * 100}])
          |> Dux.to_parquet(Path.join(dir, "ducklake_file_#{i}.parquet"))
        end

        files = Path.wildcard(Path.join(dir, "*.parquet")) |> Enum.sort()

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Build a pipeline with {:ducklake_files, files} source — simulates
        # what the coordinator does after resolving a DuckLake catalog
        pipeline = %Dux{
          source: {:ducklake_files, files},
          ops: [],
          names: [],
          dtypes: %{},
          groups: []
        }

        result =
          pipeline
          |> Dux.distribute([w1, w2])
          |> Dux.sort_by(:id)
          |> Dux.to_columns()

        assert result["id"] == [1, 2, 3, 4, 5, 6]
        assert result["value"] == [100, 200, 300, 400, 500, 600]
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Hive partition pruning across peers
  # ---------------------------------------------------------------------------

  describe "partition pruning across peer workers" do
    test "reads only matching partitions from Hive-partitioned data" do
      dir = tmp_path("hive_peer")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:hive_prune1)
      {peer2, node2} = start_peer(:hive_prune2)

      try do
        # Create Hive-partitioned dataset: year=2023 and year=2024
        for year <- [2023, 2024], month <- 1..3 do
          part_dir =
            Path.join([dir, "year=#{year}", "month=#{String.pad_leading("#{month}", 2, "0")}"])

          File.mkdir_p!(part_dir)

          rows = for i <- 1..100, do: %{"value" => year * 1000 + month * 10 + i}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(part_dir, "data.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Filter to year=2024 — should prune year=2023 files
        result =
          Dux.from_parquet(Path.join(dir, "**/*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.filter_with("year = 2024")
          |> Dux.summarise_with(n: "COUNT(*)", min_v: "MIN(value)", max_v: "MAX(value)")
          |> Dux.to_rows()

        row = hd(result)
        # 3 months × 100 rows = 300 rows for year=2024
        assert row["n"] == 300
        # All values should be in the 2024xxx range
        assert row["min_v"] >= 2_024_000
        assert row["max_v"] < 2_025_000
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end

    test "pruned distributed result matches local result" do
      dir = tmp_path("hive_peer_match")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:hive_match1)
      {peer2, node2} = start_peer(:hive_match2)

      try do
        for region <- ["US", "EU", "APAC"] do
          part_dir = Path.join(dir, "region=#{region}")
          File.mkdir_p!(part_dir)

          rows = for i <- 1..50, do: %{"amount" => i * 10, "region_col" => region}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(part_dir, "data.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        query =
          Dux.from_parquet(Path.join(dir, "**/*.parquet"))
          |> Dux.filter_with("region = 'US'")
          |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")

        local = query |> Dux.to_rows()
        distributed = query |> Dux.distribute([w1, w2]) |> Dux.to_rows()

        assert hd(local)["total"] == hd(distributed)["total"]
        assert hd(local)["n"] == hd(distributed)["n"]
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "single large file still works with multiple workers" do
      dir = tmp_path("single_file")
      File.mkdir_p!(dir)

      {peer1, node1} = start_peer(:part_single1)
      {peer2, node2} = start_peer(:part_single2)

      try do
        Dux.from_query("SELECT * FROM range(1000) t(x)")
        |> Dux.to_parquet(Path.join(dir, "only_file.parquet"))

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Single file can't be split — replicated to all workers
        result =
          Dux.from_parquet(Path.join(dir, "only_file.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(n: "COUNT(*)")
          |> Dux.to_rows()

        # Replicated: each worker returns 1000, merger sums counts
        row = hd(result)
        assert row["n"] == 2000
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(dir)
      end
    end
  end
end
