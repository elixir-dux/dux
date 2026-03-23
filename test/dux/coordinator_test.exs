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
