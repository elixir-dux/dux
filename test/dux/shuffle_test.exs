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
    if Process.alive?(w), do: GenServer.stop(w)
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
          table = Dux.Native.table_from_ipc(ipc)
          Dux.Native.table_n_rows(table)
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
          table = Dux.Native.table_from_ipc(ipc)
          Dux.Native.table_n_rows(table)
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

      db = Dux.Connection.get_db()
      chunk1 = Dux.Native.df_query(db, "SELECT 1 AS x") |> Dux.Native.table_to_ipc()
      chunk2 = Dux.Native.df_query(db, "SELECT 2 AS x") |> Dux.Native.table_to_ipc()
      chunk3 = Dux.Native.df_query(db, "SELECT 3 AS x") |> Dux.Native.table_to_ipc()

      {:ok, _} = Worker.append_chunk(worker, "test_accum", chunk1)
      {:ok, _} = Worker.append_chunk(worker, "test_accum", chunk2)
      {:ok, _} = Worker.append_chunk(worker, "test_accum", chunk3)

      # Query the accumulated table
      {:ok, ipc} =
        Worker.execute(worker, Dux.from_query(~s(SELECT * FROM "test_accum" ORDER BY x)))

      table = Dux.Native.table_from_ipc(ipc)
      assert Dux.Native.table_to_columns(table) == %{"x" => [1, 2, 3]}
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
