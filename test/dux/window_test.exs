defmodule Dux.WindowTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  require Dux

  alias Dux.Remote.Worker

  # ---------- Happy path ----------

  describe "over/2 in mutate" do
    test "row_number with partition and order" do
      rows =
        Dux.from_list([
          %{dept: "a", salary: 100},
          %{dept: "a", salary: 200},
          %{dept: "b", salary: 150}
        ])
        |> Dux.mutate(rank: over(row_number(), partition_by: :dept, order_by: [desc: :salary]))
        |> Dux.sort_by([:dept, :rank])
        |> Dux.to_rows()

      assert [%{"rank" => 1, "salary" => 200}, %{"rank" => 2, "salary" => 100}, %{"rank" => 1}] =
               rows
    end

    test "running sum with order_by" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(running: over(sum(x), order_by: :x))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["running"]) == [10, 30, 60]
    end

    test "aggregate over entire window (no partition, no order)" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(total: over(sum(x)))
        |> Dux.to_rows()

      assert Enum.all?(rows, &(&1["total"] == 60))
    end

    test "lag and lead" do
      rows =
        Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}])
        |> Dux.mutate(
          prev: over(lag(x, 1), order_by: :x),
          next: over(lead(x, 1), order_by: :x)
        )
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert [
               %{"prev" => nil, "next" => 2},
               %{"prev" => 1, "next" => 3},
               %{"prev" => 2, "next" => nil}
             ] = rows
    end

    test "multiple partition columns" do
      rows =
        Dux.from_list([
          %{a: "x", b: 1, val: 10},
          %{a: "x", b: 1, val: 20},
          %{a: "x", b: 2, val: 30}
        ])
        |> Dux.mutate(rn: over(row_number(), partition_by: [:a, :b], order_by: :val))
        |> Dux.sort_by(:val)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["rn"]) == [1, 2, 1]
    end

    test "window expression in arithmetic" do
      rows =
        Dux.from_list([%{dept: "a", salary: 100}, %{dept: "a", salary: 300}])
        |> Dux.mutate(pct: salary * 100.0 / over(sum(salary), partition_by: :dept))
        |> Dux.sort_by(:salary)
        |> Dux.to_rows()

      assert_in_delta hd(rows)["pct"], 25.0, 0.1
      assert_in_delta List.last(rows)["pct"], 75.0, 0.1
    end

    test "first_value and last_value" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(
          first: over(first_value(x), order_by: :x),
          last:
            over(last_value(x),
              order_by: :x,
              frame: "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
            )
        )
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.all?(rows, &(&1["first"] == 10))
      assert Enum.all?(rows, &(&1["last"] == 30))
    end

    test "multiple different windows in one mutate" do
      rows =
        Dux.from_list([
          %{dept: "a", date: "2026-01-01", amount: 100},
          %{dept: "a", date: "2026-01-02", amount: 200},
          %{dept: "b", date: "2026-01-01", amount: 300}
        ])
        |> Dux.mutate(
          dept_rank: over(row_number(), partition_by: :dept, order_by: [desc: :amount]),
          global_rank: over(row_number(), order_by: [desc: :amount]),
          dept_total: over(sum(amount), partition_by: :dept),
          running: over(sum(amount), order_by: :date)
        )
        |> Dux.sort_by(desc: :amount)
        |> Dux.to_rows()

      assert length(rows) == 3
      # Highest amount (300) should be global rank 1
      assert hd(rows)["global_rank"] == 1
      assert hd(rows)["amount"] == 300
    end
  end

  # ---------- Frame clauses (tuple syntax) ----------

  describe "tuple frame" do
    test "rows between N preceding and current" do
      rows =
        Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}, %{x: 4}])
        |> Dux.mutate(avg3: over(avg(x), order_by: :x, frame: {:rows, -2, :current}))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      # x=1: avg(1)=1, x=2: avg(1,2)=1.5, x=3: avg(1,2,3)=2, x=4: avg(2,3,4)=3
      assert_in_delta Enum.at(rows, 0)["avg3"], 1.0, 0.01
      assert_in_delta Enum.at(rows, 2)["avg3"], 2.0, 0.01
      assert_in_delta Enum.at(rows, 3)["avg3"], 3.0, 0.01
    end

    test "unbounded preceding to current (cumulative)" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(cum: over(sum(x), order_by: :x, frame: {:rows, :unbounded, :current}))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["cum"]) == [10, 30, 60]
    end

    test "centered window (N preceding to N following)" do
      rows =
        Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}, %{x: 4}, %{x: 5}])
        |> Dux.mutate(avg5: over(avg(x), order_by: :x, frame: {:rows, -1, 1}))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      # x=1: avg(1,2)=1.5, x=2: avg(1,2,3)=2, x=3: avg(2,3,4)=3, x=4: avg(3,4,5)=4, x=5: avg(4,5)=4.5
      assert_in_delta Enum.at(rows, 2)["avg5"], 3.0, 0.01
    end

    test "unbounded to unbounded (entire partition)" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(total: over(sum(x), frame: {:rows, :unbounded, :unbounded}))
        |> Dux.to_rows()

      assert Enum.all?(rows, &(&1["total"] == 60))
    end

    test "range frame" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(cum: over(sum(x), order_by: :x, frame: {:range, :unbounded, :current}))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["cum"]) == [10, 30, 60]
    end

    test "current row to unbounded following" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(remaining: over(sum(x), order_by: :x, frame: {:rows, :current, :unbounded}))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["remaining"]) == [60, 50, 30]
    end

    test "0 as current row alias" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(cum: over(sum(x), order_by: :x, frame: {:rows, :unbounded, 0}))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["cum"]) == [10, 30, 60]
    end

    test "exclude current row" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(
          others: over(sum(x), frame: {:rows, :unbounded, :unbounded, exclude: :current})
        )
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      # Each row's "others" = total - self
      assert Enum.at(rows, 0)["others"] == 50
      assert Enum.at(rows, 1)["others"] == 40
      assert Enum.at(rows, 2)["others"] == 30
    end

    test "exclude ties" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 10}, %{x: 20}])
        |> Dux.mutate(
          s:
            over(sum(x),
              order_by: :x,
              frame: {:range, :unbounded, :unbounded, exclude: :ties}
            )
        )
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      # For the two x=10 rows: sum of all (40) minus the tied value (10) = 30
      assert Enum.at(rows, 0)["s"] == 30
      assert Enum.at(rows, 1)["s"] == 30
      # For x=20: no ties, sum = 20 + 10 + 10 - 0 = 40... actually exclude ties for unique = total
      assert Enum.at(rows, 2)["s"] == 40
    end
  end

  # ---------- String frame (backward compat) ----------

  describe "string frame fallback" do
    test "raw SQL string still works" do
      rows =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.mutate(
          cum:
            over(sum(x),
              order_by: :x,
              frame: "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            )
        )
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["cum"]) == [10, 30, 60]
    end
  end

  # ---------- Ranking functions ----------

  describe "ranking functions" do
    test "dense_rank with ties" do
      rows =
        Dux.from_list([%{score: 100}, %{score: 90}, %{score: 90}, %{score: 80}])
        |> Dux.mutate(dr: over(dense_rank(), order_by: [desc: :score]))
        |> Dux.sort_by(desc: :score)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["dr"]) == [1, 2, 2, 3]
    end

    test "rank with gaps" do
      rows =
        Dux.from_list([%{score: 100}, %{score: 90}, %{score: 90}, %{score: 80}])
        |> Dux.mutate(r: over(rank(), order_by: [desc: :score]))
        |> Dux.sort_by(desc: :score)
        |> Dux.to_rows()

      # rank has gaps: 1, 2, 2, 4 (not 3)
      assert Enum.map(rows, & &1["r"]) == [1, 2, 2, 4]
    end

    test "ntile distributes rows into buckets" do
      rows =
        Dux.from_list(for i <- 1..12, do: %{x: i})
        |> Dux.mutate(quartile: over(ntile(4), order_by: :x))
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      quartiles = Enum.map(rows, & &1["quartile"])
      assert Enum.count(quartiles, &(&1 == 1)) == 3
      assert Enum.count(quartiles, &(&1 == 4)) == 3
    end
  end

  # ---------- Sad path ----------

  describe "sad path" do
    test "invalid frame string raises DuckDB error" do
      assert_raise ArgumentError, fn ->
        Dux.from_list([%{x: 1}])
        |> Dux.mutate(bad: over(sum(x), frame: "INVALID FRAME"))
        |> Dux.to_rows()
      end
    end

    test "nonexistent column in partition_by raises" do
      assert_raise ArgumentError, fn ->
        Dux.from_list([%{x: 1}])
        |> Dux.mutate(r: over(row_number(), partition_by: :nonexistent, order_by: :x))
        |> Dux.to_rows()
      end
    end

    test "nonexistent column in order_by raises" do
      assert_raise ArgumentError, fn ->
        Dux.from_list([%{x: 1}])
        |> Dux.mutate(r: over(row_number(), order_by: :nonexistent))
        |> Dux.to_rows()
      end
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial" do
    test "window over empty dataset" do
      rows =
        Dux.from_query("SELECT 1 AS x WHERE false")
        |> Dux.mutate(rn: over(row_number(), order_by: :x))
        |> Dux.to_rows()

      assert rows == []
    end

    test "window over single row" do
      rows =
        Dux.from_list([%{x: 42}])
        |> Dux.mutate(
          rn: over(row_number(), order_by: :x),
          total: over(sum(x)),
          prev: over(lag(x, 1), order_by: :x)
        )
        |> Dux.to_rows()

      assert [%{"rn" => 1, "total" => 42, "prev" => nil}] = rows
    end

    test "window with NULL values" do
      rows =
        Dux.from_query("SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 3")
        |> Dux.mutate(rn: over(row_number(), order_by: :x), total: over(sum(x)))
        |> Dux.sort_by(:rn)
        |> Dux.to_rows()

      assert length(rows) == 3
      # sum(x) should be 4 (1 + 3, NULL excluded)
      assert Enum.all?(rows, &(&1["total"] == 4))
    end

    test "window with interpolated partition value" do
      dept = "engineering"

      rows =
        Dux.from_list([%{dept: "engineering", val: 1}, %{dept: "engineering", val: 2}])
        |> Dux.filter(dept == ^dept)
        |> Dux.mutate(rn: over(row_number(), order_by: :val))
        |> Dux.sort_by(:val)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["rn"]) == [1, 2]
    end
  end

  # ---------- Scale / wicked ----------

  describe "wicked" do
    test "window over 2000 rows with partition" do
      data = for i <- 1..2000, do: %{grp: rem(i, 10), val: i}

      rows =
        Dux.from_list(data)
        |> Dux.mutate(
          grp_rank: over(row_number(), partition_by: :grp, order_by: [desc: :val]),
          grp_total: over(sum(val), partition_by: :grp)
        )
        |> Dux.filter(grp_rank == 1)
        |> Dux.sort_by(:grp)
        |> Dux.to_rows()

      # Top-1 per group, 10 groups
      assert length(rows) == 10
      # Each group's top value should be the max for that group
      assert Enum.all?(rows, &(&1["grp_rank"] == 1))
    end

    test "top-N per group pipeline" do
      data = for i <- 1..500, do: %{category: "cat_#{rem(i, 5)}", score: i}

      rows =
        Dux.from_list(data)
        |> Dux.mutate(rank: over(row_number(), partition_by: :category, order_by: [desc: :score]))
        |> Dux.filter(rank <= 3)
        |> Dux.sort_by([:category, :rank])
        |> Dux.to_rows()

      # 5 categories × 3 = 15
      assert length(rows) == 15

      for cat <- Enum.map(0..4, &"cat_#{&1}") do
        cat_rows = Enum.filter(rows, &(&1["category"] == cat))
        assert length(cat_rows) == 3
        assert Enum.map(cat_rows, & &1["rank"]) == [1, 2, 3]
      end
    end

    test "chained compute with window preserves results" do
      computed =
        Dux.from_list(for i <- 1..100, do: %{x: i})
        |> Dux.mutate(rn: over(row_number(), order_by: :x))
        |> Dux.compute()

      # Chain from computed result
      rows =
        computed
        |> Dux.filter(rn <= 5)
        |> Dux.to_rows()

      assert length(rows) == 5
      assert Enum.map(rows, & &1["rn"]) |> Enum.sort() == [1, 2, 3, 4, 5]
    end
  end

  # ---------- Property-based ----------

  describe "property-based" do
    property "running sum of last row equals total sum" do
      check all(values <- list_of(integer(1..1000), min_length: 1, max_length: 50)) do
        data = Enum.map(values, &%{x: &1})

        rows =
          Dux.from_list(data)
          |> Dux.mutate(
            running: over(sum(x), order_by: :x),
            total: over(sum(x))
          )
          |> Dux.sort_by(desc: :running)
          |> Dux.head(1)
          |> Dux.to_rows()

        [row] = rows
        assert row["running"] == row["total"]
      end
    end

    property "row_number produces 1..n for each partition" do
      check all(n <- integer(1..20)) do
        data = for i <- 1..n, do: %{x: i}

        rows =
          Dux.from_list(data)
          |> Dux.mutate(rn: over(row_number(), order_by: :x))
          |> Dux.sort_by(:rn)
          |> Dux.to_rows()

        assert Enum.map(rows, & &1["rn"]) == Enum.to_list(1..n)
      end
    end
  end

  # ---------- Distributed ----------

  describe "distributed" do
    test "window functions work with a single local worker" do
      {:ok, w1} = Worker.start_link()

      try do
        rows =
          Dux.from_list(for i <- 1..100, do: %{grp: rem(i, 5), val: i})
          |> Dux.distribute([w1])
          |> Dux.mutate_with(rn: "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val)")
          |> Dux.filter_with("rn = 1")
          |> Dux.collect()
          |> Dux.sort_by(:grp)
          |> Dux.to_rows()

        assert length(rows) == 5
        assert Enum.all?(rows, &(&1["rn"] == 1))
      after
        GenServer.stop(w1)
      end
    end

    test "window result matches local for single worker" do
      {:ok, w1} = Worker.start_link()

      try do
        data = Dux.from_list(for i <- 1..50, do: %{x: i})

        local =
          data
          |> Dux.mutate_with(running: "SUM(x) OVER (ORDER BY x)")
          |> Dux.sort_by(:x)
          |> Dux.to_rows()

        distributed =
          data
          |> Dux.distribute([w1])
          |> Dux.mutate_with(running: "SUM(x) OVER (ORDER BY x)")
          |> Dux.collect()
          |> Dux.sort_by(:x)
          |> Dux.to_rows()

        assert local == distributed
      after
        GenServer.stop(w1)
      end
    end
  end
end
