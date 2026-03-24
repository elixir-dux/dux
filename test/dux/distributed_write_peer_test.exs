defmodule Dux.DistributedWritePeerTest do
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
    Path.join(@tmp_dir, "dux_dw_peer_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # Distributed writes: to_parquet
  # ---------------------------------------------------------------------------

  describe "distributed to_parquet across peer workers" do
    test "writes partitioned parquet files in parallel" do
      input_dir = tmp_path("dw_input")
      output_dir = tmp_path("dw_output")
      File.mkdir_p!(input_dir)
      File.mkdir_p!(output_dir)

      {peer1, node1} = start_peer(:dw_parq1)
      {peer2, node2} = start_peer(:dw_parq2)

      try do
        # Create input data as 4 parquet files
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{"x" => (i - 1) * 25 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Distributed write: each worker writes its partition
        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.to_parquet(output_dir)

        # Verify: read back the output and check correctness
        result =
          Dux.from_parquet(Path.join(output_dir, "*.parquet"))
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(x)")
          |> Dux.to_rows()

        row = hd(result)
        assert row["n"] == 100
        assert row["total"] == div(100 * 101, 2)

        # Should have created 2 files (one per worker)
        output_files = Path.wildcard(Path.join(output_dir, "*.parquet"))
        assert length(output_files) == 2
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(input_dir)
        File.rm_rf!(output_dir)
      end
    end

    test "distributed write with filter + mutate pipeline" do
      input_dir = tmp_path("dw_pipeline_input")
      output_dir = tmp_path("dw_pipeline_output")
      File.mkdir_p!(input_dir)
      File.mkdir_p!(output_dir)

      {peer1, node1} = start_peer(:dw_pipe1)
      {peer2, node2} = start_peer(:dw_pipe2)

      try do
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{"x" => (i - 1) * 25 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.filter_with("x > 50")
        |> Dux.mutate_with(doubled: "x * 2")
        |> Dux.to_parquet(output_dir)

        result =
          Dux.from_parquet(Path.join(output_dir, "*.parquet"))
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert length(result["x"]) == 50
        assert hd(result["x"]) == 51
        assert List.last(result["x"]) == 100
        assert hd(result["doubled"]) == 102
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(input_dir)
        File.rm_rf!(output_dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed writes: to_csv
  # ---------------------------------------------------------------------------

  describe "distributed to_csv across peer workers" do
    test "writes CSV files in parallel" do
      input_dir = tmp_path("dw_csv_input")
      output_dir = tmp_path("dw_csv_output")
      File.mkdir_p!(input_dir)
      File.mkdir_p!(output_dir)

      {peer1, node1} = start_peer(:dw_csv1)
      {peer2, node2} = start_peer(:dw_csv2)

      try do
        for i <- 1..4 do
          rows = for j <- 1..10, do: %{"id" => (i - 1) * 10 + j, "name" => "row_#{j}"}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.to_csv(output_dir)

        output_files = Path.wildcard(Path.join(output_dir, "*.csv"))
        assert length(output_files) == 2

        # Read back all CSVs and verify total row count
        total_rows =
          Enum.reduce(output_files, 0, fn file, acc ->
            acc + (Dux.from_csv(file) |> Dux.n_rows())
          end)

        assert total_rows == 40
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(input_dir)
        File.rm_rf!(output_dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Round-trip: distributed write → read
  # ---------------------------------------------------------------------------

  describe "distributed write → read round-trip" do
    test "distributed write then distributed read produces same data" do
      input_dir = tmp_path("dw_roundtrip_input")
      output_dir = tmp_path("dw_roundtrip_output")
      File.mkdir_p!(input_dir)
      File.mkdir_p!(output_dir)

      {peer1, node1} = start_peer(:dw_rt1)
      {peer2, node2} = start_peer(:dw_rt2)

      try do
        for i <- 1..6 do
          rows = for j <- 1..50, do: %{"x" => (i - 1) * 50 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Distributed write
        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.filter_with("x <= 200")
        |> Dux.to_parquet(output_dir)

        # Distributed read of what we just wrote
        result =
          Dux.from_parquet(Path.join(output_dir, "*.parquet"))
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(x)")
          |> Dux.to_rows()

        row = hd(result)
        assert row["n"] == 200
        assert row["total"] == div(200 * 201, 2)
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(input_dir)
        File.rm_rf!(output_dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed partition_by writes
  # ---------------------------------------------------------------------------

  describe "distributed to_parquet with partition_by" do
    test "each worker writes Hive-partitioned output" do
      input_dir = tmp_path("dw_hive_input")
      output_dir = tmp_path("dw_hive_output")
      File.mkdir_p!(input_dir)

      {peer1, node1} = start_peer(:dw_hive1)
      {peer2, node2} = start_peer(:dw_hive2)

      try do
        # Create input with a partition column
        for i <- 1..4 do
          rows =
            for j <- 1..25 do
              %{
                "region" => Enum.at(["US", "EU", "APAC"], rem((i - 1) * 25 + j, 3)),
                "value" => (i - 1) * 25 + j
              }
            end

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Distributed write with partition_by
        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.to_parquet(output_dir, partition_by: :region)

        # Each worker creates Hive directories — read back all output
        result =
          Dux.from_parquet(Path.join(output_dir, "**/*.parquet"))
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(value)")
          |> Dux.to_rows()

        row = hd(result)
        assert row["n"] == 100
        assert row["total"] == div(100 * 101, 2)

        # Verify Hive directories exist
        subdirs =
          File.ls!(output_dir)
          |> Enum.filter(&String.starts_with?(&1, "region="))
          |> Enum.sort()

        assert subdirs == ["region=APAC", "region=EU", "region=US"]
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        File.rm_rf!(input_dir)
        File.rm_rf!(output_dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "distributed write to invalid path raises" do
      {peer1, node1} = start_peer(:dw_sad1)
      {peer2, node2} = start_peer(:dw_sad2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        assert_raise ArgumentError, ~r/all workers failed/, fn ->
          Dux.from_query("SELECT 1 AS x")
          |> Dux.distribute([w1, w2])
          |> Dux.to_parquet("/nonexistent/path/that/cannot/exist")
        end
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end
end
