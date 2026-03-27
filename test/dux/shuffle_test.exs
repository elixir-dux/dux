defmodule Dux.ShuffleTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.{Shuffle, Worker}

  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> start_one_worker() end)
    on_exit(fn -> Enum.each(workers, &stop_worker/1) end)
    workers
  end

  defp start_one_worker do
    {:ok, pid} = Worker.start_link()
    pid
  end

  defp stop_worker(w) do
    GenServer.stop(w)
  catch
    :exit, _ -> :ok
  end

  # ---------------------------------------------------------------------------
  # Worker hash_partition
  # ---------------------------------------------------------------------------

  describe "Worker.hash_partition/5" do
    test "partitions data into correct number of buckets" do
      workers = start_workers(1)
      worker = hd(workers)

      pipeline = Dux.from_query("SELECT x AS id, x * 10 AS val FROM range(100) t(x)")
      {:ok, buckets} = Worker.hash_partition(worker, pipeline, :id, 4)

      # Should have 4 buckets (some may be nil if empty)
      assert map_size(buckets) == 4

      # All rows should be accounted for
      total =
        buckets
        |> Enum.reject(fn {_k, v} -> is_nil(v) end)
        |> Enum.map(fn {_k, ipc} ->
          table = Dux.Backend.table_from_ipc(Dux.Connection.get_conn(), ipc)
          Dux.Backend.table_n_rows(Dux.Connection.get_conn(), table)
        end)
        |> Enum.sum()

      assert total == 100
    end

    test "same key always lands in same bucket" do
      workers = start_workers(1)
      worker = hd(workers)

      # Insert duplicate keys
      pipeline =
        Dux.from_query(
          "SELECT 42 AS id, 'a' AS val UNION ALL SELECT 42, 'b' UNION ALL SELECT 42, 'c'"
        )

      {:ok, buckets} = Worker.hash_partition(worker, pipeline, :id, 8)

      # All 3 rows with id=42 should be in the same bucket
      non_empty =
        buckets
        |> Enum.reject(fn {_k, v} -> is_nil(v) end)
        |> Enum.map(fn {_k, ipc} ->
          table = Dux.Backend.table_from_ipc(Dux.Connection.get_conn(), ipc)
          Dux.Backend.table_n_rows(Dux.Connection.get_conn(), table)
        end)

      assert non_empty == [3]
    end
  end

  # ---------------------------------------------------------------------------
  # Worker append_chunk
  # ---------------------------------------------------------------------------

  describe "Worker.append_chunk/3" do
    test "creates table on first chunk, appends on subsequent" do
      workers = start_workers(1)
      worker = hd(workers)

      conn = Dux.Connection.get_conn()
      chunk1 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 1 AS x"))
      chunk2 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 2 AS x"))
      chunk3 = Dux.Backend.table_to_ipc(conn, Dux.Backend.query(conn, "SELECT 3 AS x"))

      {:ok, _} = Worker.append_chunk(worker, "test_accum", chunk1)
      {:ok, _} = Worker.append_chunk(worker, "test_accum", chunk2)
      {:ok, _} = Worker.append_chunk(worker, "test_accum", chunk3)

      # Query the accumulated table
      {:ok, ipc} =
        Worker.execute(worker, Dux.from_query(~s(SELECT * FROM "test_accum" ORDER BY x)))

      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_to_columns(conn, ref) == %{"x" => [1, 2, 3]}
    end
  end

  # ---------------------------------------------------------------------------
  # Shuffle join — happy path
  # ---------------------------------------------------------------------------

  describe "Shuffle.execute/3" do
    test "basic shuffle join" do
      workers = start_workers(2)

      left =
        Dux.from_list([
          %{id: 1, name: "Alice"},
          %{id: 2, name: "Bob"},
          %{id: 3, name: "Carol"}
        ])

      right =
        Dux.from_list([
          %{id: 1, score: 95},
          %{id: 2, score: 87},
          %{id: 3, score: 92}
        ])

      result =
        Shuffle.execute(left, right, on: :id, workers: workers)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert result != []
      ids = Enum.map(result, & &1["id"]) |> Enum.uniq() |> Enum.sort()
      assert ids == [1, 2, 3]
    end

    test "shuffle join with filtering" do
      workers = start_workers(2)

      left = Dux.from_query("SELECT x AS id, x * 10 AS amount FROM range(1, 51) t(x)")

      right =
        Dux.from_query(
          "SELECT x AS id, 'item_' || CAST(x AS VARCHAR) AS name FROM range(1, 51) t(x)"
        )

      result =
        Shuffle.execute(left, right, on: :id, workers: workers)
        |> Dux.filter(amount > 200)
        |> Dux.n_rows()

      # ids 21-50 have amount > 200, each matched
      assert result > 0
    end

    test "shuffle join preserves all matched rows" do
      workers = start_workers(2)

      left = Dux.from_query("SELECT x AS key, x AS left_val FROM range(20) t(x)")
      right = Dux.from_query("SELECT x AS key, x * 2 AS right_val FROM range(20) t(x)")

      result = Shuffle.execute(left, right, on: :key, workers: workers)
      # Each row in left should match exactly one in right
      assert Dux.n_rows(result) == 20
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "no workers raises" do
      assert_raise ArgumentError, ~r/no workers/, fn ->
        Shuffle.execute(
          Dux.from_query("SELECT 1 AS id"),
          Dux.from_query("SELECT 1 AS id"),
          on: :id,
          workers: []
        )
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "join with no matching rows" do
      workers = start_workers(2)

      left = Dux.from_list([%{id: 1, val: "a"}])
      right = Dux.from_list([%{id: 999, val: "b"}])

      result = Shuffle.execute(left, right, on: :id, workers: workers)
      assert Dux.n_rows(result) == 0
    end

    test "single worker handles full shuffle" do
      workers = start_workers(1)

      left = Dux.from_query("SELECT x AS id FROM range(50) t(x)")
      right = Dux.from_query("SELECT x AS id, x * 2 AS val FROM range(50) t(x)")

      result = Shuffle.execute(left, right, on: :id, workers: workers)
      assert Dux.n_rows(result) == 50
    end
  end

  # ---------------------------------------------------------------------------
  # Worker resource configuration
  # ---------------------------------------------------------------------------

  describe "worker memory configuration" do
    test "worker accepts memory_limit option" do
      {:ok, w} = Worker.start_link(memory_limit: "256MB")

      on_exit(fn -> stop_worker(w) end)

      {:ok, ipc} =
        Worker.execute(
          w,
          Dux.from_query("SELECT current_setting('memory_limit') AS ml")
        )

      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      [row] = Dux.Backend.table_to_rows(conn, ref)
      assert row["ml"] =~ "MiB"
    end

    test "worker accepts temp_directory option" do
      tmp =
        Path.join(
          System.tmp_dir!(),
          "dux_test_worker_spill_#{System.unique_integer([:positive])}"
        )

      {:ok, w} = Worker.start_link(temp_directory: tmp)

      on_exit(fn ->
        stop_worker(w)
        File.rm_rf!(tmp)
      end)

      {:ok, ipc} =
        Worker.execute(
          w,
          Dux.from_query("SELECT current_setting('temp_directory') AS td")
        )

      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      [row] = Dux.Backend.table_to_rows(conn, ref)
      assert row["td"] == tmp
    end
  end

  # ---------------------------------------------------------------------------
  # Scale / wicked
  # ---------------------------------------------------------------------------

  describe "scale" do
    test "shuffle with 1000+ rows produces correct count" do
      workers = start_workers(2)

      left = Dux.from_query("SELECT x AS key, x * 10 AS val FROM range(2000) t(x)")
      right = Dux.from_query("SELECT x AS key, x * 0.5 AS score FROM range(2000) t(x)")

      result = Shuffle.execute(left, right, on: :key, workers: workers)
      assert Dux.n_rows(result) == 2000
    end

    test "shuffle with 4 workers" do
      workers = start_workers(4)

      left = Dux.from_query("SELECT x AS id, x * 2 AS val FROM range(500) t(x)")
      right = Dux.from_query("SELECT x AS id, CONCAT('row_', x) AS label FROM range(500) t(x)")

      result = Shuffle.execute(left, right, on: :id, workers: workers)
      assert Dux.n_rows(result) == 500
    end

    test "shuffle result feeds into downstream pipeline" do
      workers = start_workers(2)

      left = Dux.from_query("SELECT x AS key, x AS amount FROM range(100) t(x)")

      right =
        Dux.from_query(
          "SELECT x AS key, CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END AS grp FROM range(100) t(x)"
        )

      rows =
        Shuffle.execute(left, right, on: :key, workers: workers)
        |> Dux.group_by("grp")
        |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
        |> Dux.sort_by("grp")
        |> Dux.to_rows()

      assert length(rows) == 2
      total = rows |> Enum.map(& &1["total"]) |> Enum.sum()
      # Sum of 0..99 = 4950
      assert total == 4950
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial: skewed data
  # ---------------------------------------------------------------------------

  describe "skewed data" do
    test "highly skewed join keys still produce correct results" do
      workers = start_workers(2)

      # 90% of left rows have key=1, 10% have key=2
      left =
        Dux.from_query("""
          SELECT 1 AS key, x AS val FROM range(90) t(x)
          UNION ALL
          SELECT 2 AS key, x AS val FROM range(10) t(x)
        """)

      right =
        Dux.from_list([
          %{key: 1, label: "majority"},
          %{key: 2, label: "minority"}
        ])

      result =
        Shuffle.execute(left, right, on: :key, workers: workers)
        |> Dux.sort_by(:key)
        |> Dux.to_rows()

      key_1_count = Enum.count(result, &(&1["key"] == 1))
      key_2_count = Enum.count(result, &(&1["key"] == 2))

      assert key_1_count == 90
      assert key_2_count == 10
    end

    test "single-key join (all rows same key) works" do
      workers = start_workers(2)

      left = Dux.from_query("SELECT 1 AS key, x AS val FROM range(50) t(x)")
      right = Dux.from_list([%{key: 1, label: "only"}])

      result = Shuffle.execute(left, right, on: :key, workers: workers)
      assert Dux.n_rows(result) == 50
    end

    test "skew mitigation splits heavy buckets and produces correct results" do
      workers = start_workers(2)

      # 90% key=1, 10% key=2. With skew_min_bytes: 0, the heavy bucket triggers splitting.
      left =
        Dux.from_query("""
          SELECT 1 AS key, x AS val FROM range(90) t(x)
          UNION ALL
          SELECT 2 AS key, x AS val FROM range(10) t(x)
        """)

      right =
        Dux.from_list([
          %{key: 1, label: "majority"},
          %{key: 2, label: "minority"}
        ])

      # Force skew mitigation by setting threshold to 0 bytes
      result =
        Shuffle.execute(left, right,
          on: :key,
          workers: workers,
          skew_min_bytes: 0
        )

      rows = result |> Dux.sort_by(:key) |> Dux.to_rows()
      key_1 = Enum.count(rows, &(&1["key"] == 1))
      key_2 = Enum.count(rows, &(&1["key"] == 2))

      assert key_1 == 90
      assert key_2 == 10
      assert length(rows) == 100
    end

    test "skew mitigation emits telemetry when data is heavily skewed" do
      workers = start_workers(2)

      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        "skew-test-#{inspect(ref)}",
        [:dux, :distributed, :shuffle, :skew_detected],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:skew, measurements, metadata})
        end,
        nil
      )

      # Generate enough data that the heavy bucket's IPC byte_size is >5x mean.
      # 10K rows in key=1, 10 rows each in keys 2-8 — extreme skew.
      left =
        Dux.from_query("""
          SELECT 1 AS key, x AS val, REPEAT('x', 100) AS pad FROM range(10000) t(x)
          UNION ALL
          SELECT x % 7 + 2 AS key, x AS val, 'y' AS pad FROM range(70) t(x)
        """)

      right =
        Dux.from_query("SELECT x AS key, CONCAT('label_', x) AS label FROM range(1, 9) t(x)")

      _result =
        Shuffle.execute(left, right,
          on: :key,
          workers: workers,
          skew_min_bytes: 0
        )

      assert_receive {:skew, measurements, _metadata}, 2000
      assert measurements.left_heavy_count > 0

      :telemetry.detach("skew-test-#{inspect(ref)}")
    end

    test "skew-mitigated result matches local join" do
      workers = start_workers(2)

      left =
        Dux.from_query("""
          SELECT CASE WHEN x < 80 THEN 1 ELSE x END AS key, x AS val
          FROM range(100) t(x)
        """)

      right =
        Dux.from_query("SELECT x AS key, CONCAT('r_', x) AS label FROM range(100) t(x)")

      local_rows =
        left
        |> Dux.join(right, on: :key)
        |> Dux.sort_by(:val)
        |> Dux.to_rows()

      shuffle_rows =
        Shuffle.execute(left, right,
          on: :key,
          workers: workers,
          skew_min_bytes: 0
        )
        |> Dux.sort_by(:val)
        |> Dux.to_rows()

      assert length(shuffle_rows) == length(local_rows)

      local_keys = Enum.map(local_rows, & &1["key"]) |> Enum.sort()
      shuffle_keys = Enum.map(shuffle_rows, & &1["key"]) |> Enum.sort()
      assert shuffle_keys == local_keys
    end
  end

  # ---------------------------------------------------------------------------
  # Input validation
  # ---------------------------------------------------------------------------

  describe "configure_duckdb validation" do
    test "rejects invalid memory_limit format" do
      {_db, conn} = Dux.Backend.open()

      assert_raise ArgumentError, ~r/invalid memory_limit/, fn ->
        Dux.Connection.configure_duckdb(conn, memory_limit: "lots")
      end
    end

    test "rejects temp_directory with dangerous characters" do
      {_db, conn} = Dux.Backend.open()

      for bad_path <- ["/tmp/it's bad", "/tmp/foo;bar", "/tmp/back\\slash"] do
        assert_raise ArgumentError, ~r/must not contain/, fn ->
          Dux.Connection.configure_duckdb(conn, temp_directory: bad_path)
        end
      end
    end

    test "accepts valid memory_limit formats" do
      {_db, conn} = Dux.Backend.open()

      for fmt <- ["256MB", "2GB", "1.5GiB", "512MiB", "100KB"] do
        assert :ok = Dux.Connection.configure_duckdb(conn, memory_limit: fmt)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Correctness: shuffle == local join
  # ---------------------------------------------------------------------------

  describe "correctness" do
    test "shuffle join matches local join result" do
      workers = start_workers(2)

      left = Dux.from_query("SELECT x AS id, x * 10 AS amount FROM range(1, 11) t(x)")

      right =
        Dux.from_query(
          "SELECT x AS id, 'item_' || CAST(x AS VARCHAR) AS name FROM range(1, 11) t(x)"
        )

      # Local join
      local_result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # Shuffle join
      shuffle_result =
        Shuffle.execute(left, right, on: :id, workers: workers)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # Same number of rows
      assert length(shuffle_result) == length(local_result)

      # Same IDs
      local_ids = Enum.map(local_result, & &1["id"]) |> Enum.sort()
      shuffle_ids = Enum.map(shuffle_result, & &1["id"]) |> Enum.sort()
      assert shuffle_ids == local_ids
    end
  end
end
