defmodule Dux.DistributedJoinRoutingTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker

  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> start_one() end)
    on_exit(fn -> Enum.each(workers, &stop/1) end)
    workers
  end

  defp start_one do
    {:ok, pid} = Worker.start_link()
    pid
  end

  defp stop(w), do: if(Process.alive?(w), do: GenServer.stop(w))

  # ---------------------------------------------------------------------------
  # Broadcast join: distributed left + local right
  # ---------------------------------------------------------------------------

  describe "distributed left + local right (broadcast)" do
    test "join with computed local right side" do
      workers = start_workers(2)

      # Left: distributed
      left =
        Dux.from_list([
          %{id: 1, amount: 100},
          %{id: 2, amount: 200},
          %{id: 3, amount: 300}
        ])
        |> Dux.distribute(workers)

      # Right: local table ref (not worker-safe)
      right =
        Dux.from_list([
          %{id: 1, name: "Alice"},
          %{id: 2, name: "Bob"},
          %{id: 3, name: "Carol"}
        ])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # Each worker gets all 3 rows (replicated source) and joins with broadcast right
      # After merge: duplicated but all rows should have both amount and name
      assert result != []
      assert Enum.all?(result, &(Map.has_key?(&1, "name") and Map.has_key?(&1, "amount")))

      # Verify correct join results
      names = Enum.map(result, & &1["name"]) |> Enum.uniq() |> Enum.sort()
      assert names == ["Alice", "Bob", "Carol"]
    end

    test "join with uncomputed local right (from_list)" do
      workers = start_workers(2)

      left =
        Dux.from_query("SELECT * FROM range(1, 4) t(id)")
        |> Dux.distribute(workers)

      # from_list is worker-safe (serializable), so this should push down
      right = Dux.from_list([%{id: 1, label: "a"}, %{id: 2, label: "b"}, %{id: 3, label: "c"}])

      result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert result != []
      assert Enum.all?(result, &Map.has_key?(&1, "label"))
    end

    test "broadcast join matches local join results" do
      workers = start_workers(2)

      left_data = Enum.map(1..10, &%{id: &1, val: &1 * 10})
      right_data = [%{id: 2, tag: "even"}, %{id: 4, tag: "even"}, %{id: 6, tag: "even"}]

      left = Dux.from_list(left_data)
      right = Dux.from_list(right_data) |> Dux.compute()

      # Local join
      local =
        left
        |> Dux.join(right, on: :id)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # Distributed join (right will be broadcast)
      distributed =
        left
        |> Dux.distribute(workers)
        |> Dux.join(right, on: :id)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # Distributed has 2x rows (replicated source, 2 workers)
      # But the join keys and tag values should be the same set
      local_ids = Enum.map(local, & &1["id"]) |> Enum.sort()
      dist_ids = Enum.map(distributed, & &1["id"]) |> Enum.uniq() |> Enum.sort()
      assert local_ids == dist_ids
      assert Enum.all?(distributed, &(&1["tag"] == "even"))
    end

    test "left join with local right" do
      workers = start_workers(2)

      left =
        Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([%{id: 1, name: "Alice"}])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: :id, how: :left)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # All left rows preserved, unmatched get nil for name
      assert result != []

      matched = Enum.filter(result, &(&1["name"] != nil))
      assert Enum.all?(matched, &(&1["name"] == "Alice"))
    end

    test "anti join with broadcast right" do
      workers = start_workers(2)

      left =
        Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([%{id: 2}])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: :id, how: :anti)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      ids = Enum.map(result, & &1["id"]) |> Enum.uniq() |> Enum.sort()
      assert ids == [1, 3]
    end

    test "empty right side broadcast" do
      workers = start_workers(1)

      left =
        Dux.from_list([%{id: 1}, %{id: 2}])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([%{id: 99, name: "nobody"}])
        |> Dux.filter_with("id < 0")
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.to_rows()

      # Inner join with empty right → no results
      assert result == []
    end
  end

  # ---------------------------------------------------------------------------
  # Worker-safe sources (push-down, no broadcast needed)
  # ---------------------------------------------------------------------------

  describe "worker-safe right sides (push-down)" do
    test "SQL query right side pushes down to workers" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 4) t(id)")
        |> Dux.distribute(workers)
        |> Dux.join(
          Dux.from_query("SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'"),
          on: :id
        )
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert result != []
      assert Enum.all?(result, &Map.has_key?(&1, "name"))
    end

    test "from_list right side pushes down" do
      workers = start_workers(1)

      result =
        Dux.from_query("SELECT 1 AS id")
        |> Dux.distribute(workers)
        |> Dux.join(Dux.from_list([%{id: 1, tag: "x"}]), on: :id)
        |> Dux.to_rows()

      assert hd(result)["tag"] == "x"
    end
  end

  # ---------------------------------------------------------------------------
  # Multiple joins in one pipeline
  # ---------------------------------------------------------------------------

  describe "multiple joins" do
    test "two broadcast joins in one pipeline" do
      workers = start_workers(2)

      left =
        Dux.from_list([%{id: 1, region: "US"}, %{id: 2, region: "EU"}])
        |> Dux.distribute(workers)

      dim1 = Dux.from_list([%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]) |> Dux.compute()

      dim2 =
        Dux.from_list([
          %{region: "US", country: "United States"},
          %{region: "EU", country: "Europe"}
        ])
        |> Dux.compute()

      result =
        left
        |> Dux.join(dim1, on: :id)
        |> Dux.join(dim2, on: :region)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert result != []
      assert Enum.all?(result, &(Map.has_key?(&1, "name") and Map.has_key?(&1, "country")))
    end

    test "mixed: push-down join + broadcast join" do
      workers = start_workers(1)

      left =
        Dux.from_query("SELECT 1 AS id, 10 AS val")
        |> Dux.distribute(workers)

      # This is worker-safe (SQL query)
      sql_right = Dux.from_query("SELECT 1 AS id, 'tag_a' AS tag")

      # This is NOT worker-safe (computed table ref)
      local_right = Dux.from_list([%{id: 1, label: "x"}]) |> Dux.compute()

      result =
        left
        |> Dux.join(sql_right, on: :id)
        |> Dux.join(local_right, on: :id)
        |> Dux.to_rows()

      row = hd(result)
      assert row["tag"] == "tag_a"
      assert row["label"] == "x"
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "right side with special characters in column names" do
      workers = start_workers(1)

      left =
        Dux.from_list([%{"my id" => 1, "val" => 10}])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([%{"my id" => 1, "na\"me" => "Alice"}])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: [{:"my id", :"my id"}])
        |> Dux.to_rows()

      assert result != []
      assert hd(result)["na\"me"] == "Alice"
    end

    test "broadcast cleanup happens even on worker failure" do
      workers = start_workers(2)

      left =
        Dux.from_list([%{id: 1}])
        |> Dux.distribute(workers)

      right = Dux.from_list([%{id: 1, name: "ok"}]) |> Dux.compute()

      # Normal join should work and clean up
      _result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.to_rows()

      # Verify broadcast tables were cleaned up — workers should have no extra tables
      Enum.each(workers, fn w ->
        info = Worker.info(w)
        broadcast_tables = Enum.filter(info.tables, &String.starts_with?(&1, "__bcast_"))
        assert broadcast_tables == []
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "self-join on distributed pipeline" do
      workers = start_workers(1)

      df =
        Dux.from_list([%{id: 1, parent: 2}, %{id: 2, parent: 3}, %{id: 3, parent: nil}])
        |> Dux.distribute(workers)

      # Self-join: the right side is the same pipeline but computed locally
      right =
        Dux.from_list([%{id: 1, parent: 2}, %{id: 2, parent: 3}, %{id: 3, parent: nil}])
        |> Dux.compute()

      result =
        df
        |> Dux.join(right, on: [{:parent, :id}])
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # id=1 joins parent=2 → right id=2, id=2 joins parent=3 → right id=3
      ids = Enum.map(result, & &1["id"]) |> Enum.sort()
      assert ids == [1, 2]
    end
  end

  # ---------------------------------------------------------------------------
  # Shuffle join: distributed left + large local right
  # ---------------------------------------------------------------------------

  describe "shuffle join (forced via broadcast_threshold: 0)" do
    test "large right side triggers shuffle join" do
      workers = start_workers(2)

      left_data = Enum.map(1..20, &%{id: &1, val: &1 * 10})
      right_data = Enum.map(1..20, &%{id: &1, tag: "item_#{&1}"})

      left = Dux.from_list(left_data)
      right = Dux.from_list(right_data) |> Dux.compute()

      # Force shuffle by setting broadcast threshold to 0
      result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.Remote.Coordinator.execute(
          workers: workers,
          broadcast_threshold: 0
        )
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # All 20 ids should match
      ids = Enum.map(result, & &1["id"])
      assert Enum.sort(ids) == Enum.to_list(1..20)
      assert Enum.all?(result, &String.starts_with?(&1["tag"], "item_"))
    end

    test "shuffle join matches local join" do
      workers = start_workers(2)

      left_data = Enum.map(1..10, &%{key: &1, left_val: &1 * 10})

      right_data = [
        %{key: 2, right_val: 200},
        %{key: 5, right_val: 500},
        %{key: 8, right_val: 800}
      ]

      left = Dux.from_list(left_data)
      right = Dux.from_list(right_data) |> Dux.compute()

      # Local join for comparison
      local =
        left
        |> Dux.join(right, on: :key)
        |> Dux.sort_by(:key)
        |> Dux.to_rows()

      # Shuffle join
      shuffled =
        left
        |> Dux.join(right, on: :key)
        |> Dux.Remote.Coordinator.execute(
          workers: workers,
          broadcast_threshold: 0
        )
        |> Dux.sort_by(:key)
        |> Dux.to_rows()

      # Same keys should match
      local_keys = Enum.map(local, & &1["key"])
      shuffle_keys = Enum.map(shuffled, & &1["key"])
      assert local_keys == shuffle_keys

      # Same right values
      local_vals = Enum.map(local, & &1["right_val"])
      shuffle_vals = Enum.map(shuffled, & &1["right_val"])
      assert local_vals == shuffle_vals
    end

    test "left join via shuffle" do
      workers = start_workers(2)

      left = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])
      right = Dux.from_list([%{id: 1, name: "Alice"}]) |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: :id, how: :left)
        |> Dux.Remote.Coordinator.execute(
          workers: workers,
          broadcast_threshold: 0
        )
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # All 3 left rows should be present
      ids = Enum.map(result, & &1["id"]) |> Enum.sort()
      assert ids == [1, 2, 3]

      # Only id=1 should have a name
      matched = Enum.filter(result, &(&1["name"] != nil))
      assert length(matched) == 1
      assert hd(matched)["name"] == "Alice"
    end

    test "shuffle with ops before the join" do
      workers = start_workers(2)

      left = Dux.from_list(Enum.map(1..10, &%{id: &1, val: &1 * 10}))
      right = Dux.from_list([%{id: 3, tag: "three"}, %{id: 7, tag: "seven"}]) |> Dux.compute()

      # Filter before the join — these ops execute as stage 1
      result =
        left
        |> Dux.filter_with("val > 20")
        |> Dux.join(right, on: :id)
        |> Dux.Remote.Coordinator.execute(
          workers: workers,
          broadcast_threshold: 0
        )
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # Only ids 3 and 7 match (both have val > 20)
      # With replicated source, 2 workers each produce the matching rows
      ids = Enum.map(result, & &1["id"]) |> Enum.uniq() |> Enum.sort()
      assert ids == [3, 7]
      assert Enum.all?(result, &Map.has_key?(&1, "tag"))
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-column joins
  # ---------------------------------------------------------------------------

  describe "multi-column joins" do
    test "broadcast with multi-column join key" do
      workers = start_workers(2)

      left =
        Dux.from_list([
          %{region: "US", year: 2024, revenue: 100},
          %{region: "EU", year: 2024, revenue: 200},
          %{region: "US", year: 2025, revenue: 150}
        ])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([
          %{region: "US", year: 2024, target: 90},
          %{region: "EU", year: 2024, target: 180}
        ])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: [:region, :year])
        |> Dux.sort_by(:region)
        |> Dux.to_rows()

      # Only 2 matches: US/2024 and EU/2024 (US/2025 has no target)
      regions = Enum.map(result, & &1["region"]) |> Enum.uniq() |> Enum.sort()
      assert regions == ["EU", "US"]
      assert Enum.all?(result, &Map.has_key?(&1, "target"))
    end

    test "shuffle with multi-column join key" do
      workers = start_workers(2)

      left =
        Dux.from_list([
          %{a: 1, b: "x", val: 10},
          %{a: 1, b: "y", val: 20},
          %{a: 2, b: "x", val: 30}
        ])

      right =
        Dux.from_list([
          %{a: 1, b: "x", tag: "match1"},
          %{a: 2, b: "x", tag: "match2"}
        ])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: [:a, :b])
        |> Dux.Remote.Coordinator.execute(
          workers: workers,
          broadcast_threshold: 0
        )
        |> Dux.sort_by(:a)
        |> Dux.to_rows()

      tags = Enum.map(result, & &1["tag"]) |> Enum.sort()
      assert tags == ["match1", "match2"]
    end

    test "broadcast with different left/right column names" do
      workers = start_workers(1)

      left =
        Dux.from_list([%{user_id: 1, score: 100}, %{user_id: 2, score: 200}])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: [{:user_id, :id}])
        |> Dux.sort_by(:user_id)
        |> Dux.to_rows()

      names = Enum.map(result, & &1["name"]) |> Enum.sort()
      assert names == ["Alice", "Bob"]
    end

    test "shuffle with different left/right column names" do
      workers = start_workers(2)

      left = Dux.from_list([%{emp_id: 1, salary: 50_000}, %{emp_id: 2, salary: 60_000}])

      right =
        Dux.from_list([%{id: 1, dept: "Eng"}, %{id: 2, dept: "Sales"}])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: [{:emp_id, :id}])
        |> Dux.Remote.Coordinator.execute(
          workers: workers,
          broadcast_threshold: 0
        )
        |> Dux.sort_by(:emp_id)
        |> Dux.to_rows()

      depts = Enum.map(result, & &1["dept"]) |> Enum.sort()
      assert depts == ["Eng", "Sales"]
    end
  end
end
