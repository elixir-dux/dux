defmodule Dux.CoordinatorTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.{Coordinator, Merger, Partitioner, Worker}

  @tmp_dir System.tmp_dir!()

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_coord_#{System.unique_integer([:positive])}_#{name}")
  end

  # Start N workers for a test, stop them on exit
  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> start_worker() end)

    on_exit(fn ->
      Enum.each(workers, &stop_worker/1)
    end)

    workers
  end

  defp start_worker do
    {:ok, pid} = Worker.start_link()
    pid
  end

  defp stop_worker(w) do
    GenServer.stop(w)
  catch
    :exit, _ -> :ok
  end

  # ---------------------------------------------------------------------------
  # Partitioner unit tests
  # ---------------------------------------------------------------------------

  describe "Partitioner.assign/3" do
    test "replicates non-glob pipeline to all workers" do
      workers = [:w1, :w2, :w3]
      pipeline = Dux.from_query("SELECT 1 AS x")
      assignments = Partitioner.assign(pipeline, workers)

      assert length(assignments) == 3
      assert Enum.all?(assignments, fn {_w, p} -> p.source == {:sql, "SELECT 1 AS x"} end)
    end

    test "partitions local parquet glob across workers" do
      dir = tmp_path("partition_test")
      File.mkdir_p!(dir)

      try do
        for i <- 1..6 do
          Dux.from_list([%{"x" => i}])
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        pipeline = Dux.from_parquet(Path.join(dir, "*.parquet"))
        workers = [:w1, :w2, :w3]
        assignments = Partitioner.assign(pipeline, workers)

        assert length(assignments) == 3

        # Each worker should get 2 files (6 files / 3 workers)
        total_files =
          Enum.flat_map(assignments, fn {_w, p} ->
            case p.source do
              {:parquet_list, files, _} -> files
              {:parquet, file, _} -> [file]
            end
          end)

        assert length(total_files) == 6
      after
        File.rm_rf!(dir)
      end
    end
  end

  describe "Partitioner.assign/3 with ducklake_files" do
    test "distributes DuckLake file list across workers as parquet sources" do
      files = [
        "s3://bucket/data/file1.parquet",
        "s3://bucket/data/file2.parquet",
        "s3://bucket/data/file3.parquet",
        "s3://bucket/data/file4.parquet",
        "s3://bucket/data/file5.parquet",
        "s3://bucket/data/file6.parquet"
      ]

      pipeline = %Dux{
        source: {:ducklake_files, files},
        ops: [],
        names: [],
        dtypes: %{},
        groups: []
      }

      workers = [:w1, :w2, :w3]
      assignments = Partitioner.assign(pipeline, workers)

      assert length(assignments) == 3

      total_files =
        Enum.flat_map(assignments, fn {_w, p} ->
          case p.source do
            {:parquet_list, f, _} -> f
            {:parquet, f, _} -> [f]
          end
        end)

      assert length(total_files) == 6
      assert Enum.sort(total_files) == Enum.sort(files)
    end

    test "single DuckLake file goes to one worker, others dropped" do
      files = ["s3://bucket/data/file1.parquet"]

      pipeline = %Dux{
        source: {:ducklake_files, files},
        ops: [],
        names: [],
        dtypes: %{},
        groups: []
      }

      workers = [:w1, :w2]
      assignments = Partitioner.assign(pipeline, workers)

      # Only 1 assignment — workers with no files are filtered out
      assert length(assignments) == 1
      [{_worker, assigned}] = assignments
      assert assigned.source == {:parquet, "s3://bucket/data/file1.parquet", []}
    end

    test "more workers than files assigns one file per worker" do
      files = ["s3://bucket/f1.parquet", "s3://bucket/f2.parquet"]

      pipeline = %Dux{
        source: {:ducklake_files, files},
        ops: [],
        names: [],
        dtypes: %{},
        groups: []
      }

      workers = [:w1, :w2, :w3, :w4, :w5]
      assignments = Partitioner.assign(pipeline, workers)

      # Only 2 workers get assignments; the rest are empty and filtered out
      assert length(assignments) == 2

      all_files =
        Enum.flat_map(assignments, fn {_w, p} ->
          case p.source do
            {:parquet, f, _} -> [f]
            {:parquet_list, f, _} -> f
          end
        end)

      assert Enum.sort(all_files) == Enum.sort(files)
    end

    test "preserves pipeline ops through distribution" do
      require Dux

      files = ["s3://bucket/f1.parquet", "s3://bucket/f2.parquet"]

      pipeline =
        %Dux{
          source: {:ducklake_files, files},
          ops: [],
          names: [],
          dtypes: %{},
          groups: []
        }
        |> Dux.filter_with("x > 10")
        |> Dux.select([:x])

      workers = [:w1, :w2]
      assignments = Partitioner.assign(pipeline, workers)

      # Ops should be preserved on each worker's pipeline
      Enum.each(assignments, fn {_w, p} ->
        assert length(p.ops) == 2
      end)
    end
  end

  describe "Partitioner size-balanced assignment" do
    test "balances skewed file sizes across workers" do
      dir = tmp_path("size_balanced_test")
      File.mkdir_p!(dir)

      try do
        # Create files with very different sizes:
        # 1 large file (~10K rows) and 5 small files (~10 rows each)
        Dux.from_query("SELECT * FROM range(10000) t(x)")
        |> Dux.to_parquet(Path.join(dir, "big.parquet"))

        for i <- 1..5 do
          Dux.from_list([%{"x" => i}])
          |> Dux.to_parquet(Path.join(dir, "small_#{i}.parquet"))
        end

        pipeline = Dux.from_parquet(Path.join(dir, "*.parquet"))
        workers = [:w1, :w2, :w3]
        assignments = Partitioner.assign(pipeline, workers)

        assert length(assignments) == 3

        # The big file should be alone on one worker (or with at most one small file).
        # With round-robin, 6 files / 3 workers = 2 files each, which would pair
        # the big file with a small one arbitrarily. With bin-packing, the big file
        # gets its own worker and the small files cluster on the other workers.
        worker_file_counts =
          Enum.map(assignments, fn {_w, p} ->
            case p.source do
              {:parquet_list, files, _} -> length(files)
              {:parquet, _, _} -> 1
            end
          end)
          |> Enum.sort()

        # The worker with the big file should have fewer files than others
        # (bin-packing: big goes first, then small files fill lightest workers)
        assert hd(worker_file_counts) <= 2

        # All files accounted for
        total =
          Enum.flat_map(assignments, fn {_w, p} ->
            case p.source do
              {:parquet_list, f, _} -> f
              {:parquet, f, _} -> [f]
            end
          end)

        assert length(total) == 6
      after
        File.rm_rf!(dir)
      end
    end

    test "size-balanced produces more balanced loads than round-robin on skewed data" do
      dir = tmp_path("balance_comparison")
      File.mkdir_p!(dir)

      try do
        # Create: 1 file with 5000 rows, 1 with 3000, 1 with 1000, 3 with 100 each
        for {name, n} <- [
              {"a_5000", 5000},
              {"b_3000", 3000},
              {"c_1000", 1000},
              {"d_100", 100},
              {"e_100", 100},
              {"f_100", 100}
            ] do
          Dux.from_query("SELECT * FROM range(#{n}) t(x)")
          |> Dux.to_parquet(Path.join(dir, "#{name}.parquet"))
        end

        pipeline = Dux.from_parquet(Path.join(dir, "*.parquet"))
        workers = [:w1, :w2, :w3]
        assignments = Partitioner.assign(pipeline, workers)

        # Get file sizes per worker
        worker_sizes =
          Enum.map(assignments, fn {_w, p} ->
            files =
              case p.source do
                {:parquet_list, f, _} -> f
                {:parquet, f, _} -> [f]
              end

            Enum.reduce(files, 0, fn f, acc ->
              {:ok, %{size: s}} = File.stat(f)
              acc + s
            end)
          end)

        max_load = Enum.max(worker_sizes)
        min_load = Enum.min(worker_sizes)

        # With size-balanced bin-packing, the ratio of max to min load
        # should be reasonable (within 4x). Round-robin on this data would
        # likely produce much worse balance.
        assert max_load / max(min_load, 1) < 4.0
      after
        File.rm_rf!(dir)
      end
    end

    test "falls back to round-robin for S3 paths" do
      # S3 paths can't be stat'd locally — should fall back to round-robin
      # and still produce valid assignments (not crash)
      pipeline = Dux.from_parquet("s3://bucket/data/*.parquet")
      workers = [:w1, :w2]
      assignments = Partitioner.assign(pipeline, workers)

      # S3 globs can't be expanded locally, so the whole glob is replicated
      assert length(assignments) == 2

      assert Enum.all?(assignments, fn {_w, p} ->
               p.source == {:parquet, "s3://bucket/data/*.parquet", []}
             end)
    end
  end

  describe "Partitioner partition pruning" do
    test "prunes Hive-partitioned files based on filter ops" do
      dir = tmp_path("hive_pruning")
      File.mkdir_p!(dir)

      try do
        # Create Hive-partitioned structure
        for year <- [2023, 2024], month <- ["01", "02"] do
          part_dir = Path.join([dir, "year=#{year}", "month=#{month}"])
          File.mkdir_p!(part_dir)

          Dux.from_list([%{"x" => year * 100 + String.to_integer(month)}])
          |> Dux.to_parquet(Path.join(part_dir, "data.parquet"))
        end

        pipeline =
          Dux.from_parquet(Path.join(dir, "**/*.parquet"))
          |> Dux.filter_with("year = 2024")

        workers = [:w1, :w2]
        assignments = Partitioner.assign(pipeline, workers)

        # Should only have year=2024 files (2 files), not year=2023
        all_files =
          Enum.flat_map(assignments, fn {_w, p} ->
            case p.source do
              {:parquet, f, _} -> [f]
              {:parquet_list, f, _} -> f
            end
          end)

        assert length(all_files) == 2
        assert Enum.all?(all_files, &String.contains?(&1, "year=2024"))
      after
        File.rm_rf!(dir)
      end
    end

    test "prunes on multiple partition columns" do
      dir = tmp_path("hive_multi_prune")
      File.mkdir_p!(dir)

      try do
        for year <- [2023, 2024], month <- ["01", "02"] do
          part_dir = Path.join([dir, "year=#{year}", "month=#{month}"])
          File.mkdir_p!(part_dir)

          Dux.from_list([%{"x" => 1}])
          |> Dux.to_parquet(Path.join(part_dir, "data.parquet"))
        end

        pipeline =
          Dux.from_parquet(Path.join(dir, "**/*.parquet"))
          |> Dux.filter_with("year = 2024 AND month = '01'")

        workers = [:w1, :w2]
        assignments = Partitioner.assign(pipeline, workers)

        all_files =
          Enum.flat_map(assignments, fn {_w, p} ->
            case p.source do
              {:parquet, f, _} -> [f]
              {:parquet_list, f, _} -> f
            end
          end)

        assert length(all_files) == 1
        assert hd(all_files) |> String.contains?("year=2024")
        assert hd(all_files) |> String.contains?("month=01")
      after
        File.rm_rf!(dir)
      end
    end

    test "no pruning when filter doesn't match partition columns" do
      dir = tmp_path("hive_no_prune")
      File.mkdir_p!(dir)

      try do
        for year <- [2023, 2024] do
          part_dir = Path.join(dir, "year=#{year}")
          File.mkdir_p!(part_dir)

          Dux.from_list([%{"x" => year}])
          |> Dux.to_parquet(Path.join(part_dir, "data.parquet"))
        end

        pipeline =
          Dux.from_parquet(Path.join(dir, "**/*.parquet"))
          |> Dux.filter_with("x > 100")

        workers = [:w1, :w2]
        assignments = Partitioner.assign(pipeline, workers)

        all_files =
          Enum.flat_map(assignments, fn {_w, p} ->
            case p.source do
              {:parquet, f, _} -> [f]
              {:parquet_list, f, _} -> f
            end
          end)

        # x is not a partition column, so all files pass through
        assert length(all_files) == 2
      after
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Merger unit tests
  # ---------------------------------------------------------------------------

  describe "Merger.merge_to_dux/2" do
    test "merges simple concatenation" do
      conn = Dux.Connection.get_conn()

      ipc1 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 1 AS x"))
      ipc2 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 2 AS x"))
      ipc3 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 3 AS x"))

      pipeline = Dux.from_query("SELECT x FROM source")
      result = Merger.merge_to_dux([ipc1, ipc2, ipc3], pipeline)

      assert result.names == ["x"]
      cols = Dux.to_columns(result)
      assert Enum.sort(cols["x"]) == [1, 2, 3]
    end

    test "merges with re-sort" do
      conn = Dux.Connection.get_conn()

      ipc1 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 3 AS x"))
      ipc2 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 1 AS x"))
      ipc3 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 2 AS x"))

      pipeline =
        Dux.from_query("SELECT x FROM source")
        |> Dux.sort_by(:x)

      result = Merger.merge_to_dux([ipc1, ipc2, ipc3], pipeline)
      cols = Dux.to_columns(result)
      assert cols["x"] == [1, 2, 3]
    end

    test "merges with re-head" do
      conn = Dux.Connection.get_conn()

      ipc1 =
        Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT * FROM range(1, 4) t(x)"))

      ipc2 =
        Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT * FROM range(4, 7) t(x)"))

      pipeline =
        Dux.from_query("SELECT x FROM source")
        |> Dux.sort_by(:x)
        |> Dux.head(3)

      result = Merger.merge_to_dux([ipc1, ipc2], pipeline)
      cols = Dux.to_columns(result)
      assert cols["x"] == [1, 2, 3]
    end
  end

  # ---------------------------------------------------------------------------
  # Coordinator integration tests (local workers)
  # ---------------------------------------------------------------------------

  describe "Coordinator.execute/2 with local workers" do
    test "simple query distributed across workers" do
      workers = start_workers(3)

      result =
        Dux.from_query("SELECT 42 AS answer")
        |> Coordinator.execute(workers: workers)

      # Each worker returns the same result, merger concatenates
      # 3 workers × 1 row = 3 rows
      assert Dux.n_rows(result) == 3
    end

    test "filter + aggregate distributed across workers" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.filter(x > 5)
        |> Coordinator.execute(workers: workers)

      # Each worker independently filters and returns x > 5
      # Merger concatenates: [6..10] from worker1 + [6..10] from worker2
      cols = Dux.to_columns(result)
      assert length(cols["x"]) == 10
    end

    test "distributed scan of partitioned Parquet files" do
      dir = tmp_path("distributed_scan")
      File.mkdir_p!(dir)

      try do
        # Create 4 Parquet files
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{"partition" => i, "value" => (i - 1) * 25 + j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        workers = start_workers(2)

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.summarise_with(total: "SUM(value)", n: "COUNT(*)")
          |> Coordinator.execute(workers: workers)

        cols = Dux.to_columns(result)

        # 100 values total (1..100), sum = 5050
        # But since summarise is re-aggregated, we get the SUM of partial SUMs
        total = hd(cols["total"])
        n = hd(cols["n"])
        assert total == 5050
        assert n == 100
      after
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "no workers raises" do
      assert_raise ArgumentError, ~r/no workers available/, fn ->
        Coordinator.execute(Dux.from_query("SELECT 1"), workers: [])
      end
    end

    test "bad pipeline returns error from all workers" do
      workers = start_workers(2)

      assert_raise ArgumentError, ~r/all workers failed/, fn ->
        Coordinator.execute(Dux.from_query("NOT VALID SQL"), workers: workers)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "single worker handles full workload" do
      workers = start_workers(1)

      result =
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.filter(x > 50)
        |> Coordinator.execute(workers: workers)

      assert Dux.n_rows(result) == 49
    end
  end
end
