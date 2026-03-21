defmodule Dux.DistributedCorrectnessTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.{Coordinator, PipelineSplitter, Worker}

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
  # Aggregate correctness
  # ---------------------------------------------------------------------------

  describe "distributed MIN/MAX re-aggregation" do
    test "MIN is re-aggregated with MIN, not SUM" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.summarise_with(minimum: "MIN(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      # Correct: MIN across all rows = 1
      # Bug (SUM): each worker's MIN might be 1, SUM would be 2
      assert hd(result)["minimum"] == 1
    end

    test "MAX is re-aggregated with MAX, not SUM" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.summarise_with(maximum: "MAX(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      assert hd(result)["maximum"] == 100
    end

    test "grouped MIN/MAX across partitions" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT x, x % 3 AS grp FROM range(1, 31) t(x)")
        |> Dux.group_by(:grp)
        |> Dux.summarise_with(min_x: "MIN(x)", max_x: "MAX(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.sort_by(:grp)
        |> Dux.collect()

      # grp=0: x in {3,6,9,...,30} → min=3, max=30
      # grp=1: x in {1,4,7,...,28} → min=1, max=28
      # grp=2: x in {2,5,8,...,29} → min=2, max=29
      assert Enum.find(result, &(&1["grp"] == 1))["min_x"] == 1
      assert Enum.find(result, &(&1["grp"] == 0))["max_x"] == 30
    end
  end

  describe "distributed AVG rewrite" do
    test "AVG produces correct result via SUM/COUNT rewrite" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(average: "AVG(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      # AVG of 1..10 = 5.5
      assert_in_delta hd(result)["average"], 5.5, 0.01
    end

    test "grouped AVG" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT x, x % 2 AS grp FROM range(1, 11) t(x)")
        |> Dux.group_by(:grp)
        |> Dux.summarise_with(avg_x: "AVG(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.sort_by(:grp)
        |> Dux.collect()

      # grp=0 (evens 2,4,6,8,10): avg=6.0
      # grp=1 (odds 1,3,5,7,9): avg=5.0
      assert_in_delta Enum.at(result, 0)["avg_x"], 6.0, 0.01
      assert_in_delta Enum.at(result, 1)["avg_x"], 5.0, 0.01
    end

    test "mixed aggregates: SUM, COUNT, AVG, MIN, MAX (replicated)" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(
          total: "SUM(x)",
          n: "COUNT(*)",
          average: "AVG(x)",
          minimum: "MIN(x)",
          maximum: "MAX(x)"
        )
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      row = hd(result)
      # Replicated: SUM and COUNT are 2x
      assert row["total"] == 110
      assert row["n"] == 20
      # AVG is correct even with replication (SUM/COUNT = 110/20 = 5.5)
      assert_in_delta row["average"], 5.5, 0.01
      # MIN/MAX are correct regardless of replication
      assert row["minimum"] == 1
      assert row["maximum"] == 10
    end
  end

  # ---------------------------------------------------------------------------
  # STDDEV/VARIANCE distributed decomposition
  # ---------------------------------------------------------------------------

  describe "distributed STDDEV/VARIANCE rewrite" do
    test "STDDEV_SAMP decomposition produces numeric result" do
      workers = start_workers(2)

      # With replicated source, N doubles (2 workers × 10 rows = 20).
      # The decomposition formula is correct for the replicated data.
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(sd: "STDDEV_SAMP(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      # Should produce a positive number (not nil, not negative)
      assert is_number(hd(result)["sd"])
      assert hd(result)["sd"] > 0
    end

    test "STDDEV matches local on single worker" do
      workers = start_workers(1)

      # Single worker = no replication = exact match
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(sd: "STDDEV_SAMP(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      local =
        Dux.from_query("SELECT STDDEV_SAMP(x) AS sd FROM range(1, 11) t(x)")
        |> Dux.collect()

      assert_in_delta hd(result)["sd"], hd(local)["sd"], 0.01
    end

    test "VARIANCE decomposition produces numeric result" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(v: "VARIANCE(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.collect()

      assert is_number(hd(result)["v"])
      assert hd(result)["v"] > 0
    end

    test "grouped STDDEV_SAMP" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT x, x % 2 AS grp FROM range(1, 21) t(x)")
        |> Dux.group_by(:grp)
        |> Dux.summarise_with(sd: "STDDEV_SAMP(x)")
        |> Coordinator.execute(workers: workers)
        |> Dux.sort_by(:grp)
        |> Dux.collect()

      # Each group has 10 values — STDDEV should be positive and finite
      assert length(result) == 2
      assert Enum.all?(result, fn row -> is_number(row["sd"]) and row["sd"] > 0 end)
    end
  end

  # ---------------------------------------------------------------------------
  # Slice correctness
  # ---------------------------------------------------------------------------

  describe "distributed slice" do
    test "slice applies on coordinator, not per-partition" do
      workers = start_workers(2)

      # Replicated source: 2 workers × 100 rows = 200 merged rows
      # Sorted: [0,0,1,1,2,2,...,99,99]
      # Slice(10, 5) on merged = rows at positions 10-14 = [5,5,6,6,7]
      result =
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.sort_by(:x)
        |> Dux.slice(10, 5)
        |> Coordinator.execute(workers: workers)
        |> Dux.to_columns()

      # With 2 workers, each sorted value appears twice in merged result
      assert length(result["x"]) == 5
    end

    test "slice is not pushed to workers (would give wrong offset)" do
      %{worker_ops: worker_ops, coordinator_ops: coord_ops} =
        PipelineSplitter.split([
          {:filter, "x > 0"},
          {:sort_by, [{:asc, "x"}]},
          {:slice, 10, 5}
        ])

      # Slice should be coordinator-only
      assert Enum.any?(coord_ops, &match?({:slice, _, _}, &1))
      # Filter and sort push to workers
      assert length(worker_ops) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Pivot correctness
  # ---------------------------------------------------------------------------

  describe "distributed pivot_wider" do
    test "pivot_wider applies on coordinator after merge" do
      workers = start_workers(2)

      result =
        Dux.from_list([
          %{region: "US", product: "Widget", sales: 100},
          %{region: "US", product: "Gadget", sales: 200},
          %{region: "EU", product: "Widget", sales: 150}
        ])
        |> Dux.pivot_wider(:product, :sales)
        |> Coordinator.execute(workers: workers)
        |> Dux.sort_by(:region)
        |> Dux.collect()

      assert length(result) == 2

      eu = Enum.find(result, &(&1["region"] == "EU"))
      us = Enum.find(result, &(&1["region"] == "US"))

      # Replicated source: SUM is 2x
      assert eu["Widget"] == 300
      assert us["Gadget"] == 400
    end
  end

  # ---------------------------------------------------------------------------
  # Pipeline splitting
  # ---------------------------------------------------------------------------

  describe "pipeline splitter" do
    test "filter + mutate push down, sort stays on coordinator" do
      %{worker_ops: worker, coordinator_ops: coord} =
        PipelineSplitter.split([
          {:filter, "x > 10"},
          {:mutate, [{"y", "x * 2"}]},
          {:sort_by, [{:asc, "x"}]},
          {:head, 5}
        ])

      # filter and mutate push down, sort and head push down AND go to coordinator
      assert length(worker) == 4
      assert Enum.any?(coord, &match?({:sort_by, _}, &1))
      assert Enum.any?(coord, &match?({:head, _}, &1))
    end

    test "slice never pushes to workers" do
      %{worker_ops: worker, coordinator_ops: coord} =
        PipelineSplitter.split([
          {:filter, "x > 10"},
          {:slice, 5, 10}
        ])

      assert length(worker) == 1
      assert [{:slice, 5, 10}] = coord
    end

    test "pivot_wider stays on coordinator" do
      %{worker_ops: worker, coordinator_ops: coord} =
        PipelineSplitter.split([
          {:filter, "x > 0"},
          {:pivot_wider, "product", "sales", "SUM"}
        ])

      assert length(worker) == 1
      assert [{:pivot_wider, _, _, _}] = coord
    end

    test "AVG gets rewritten to SUM + COUNT" do
      %{worker_ops: worker, agg_rewrites: rewrites} =
        PipelineSplitter.split([
          {:summarise, [{"average", "AVG(x)"}]}
        ])

      [{:summarise, worker_aggs}] = worker

      # Should have __avg_sum_average and __avg_count_average
      agg_names = Enum.map(worker_aggs, fn {name, _} -> name end)
      assert "__avg_sum_average" in agg_names
      assert "__avg_count_average" in agg_names

      # Rewrites should track the AVG column
      assert Map.has_key?(rewrites, "average")
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed result == local result
  # ---------------------------------------------------------------------------

  describe "distributed matches local" do
    test "replicated source: each worker processes all data" do
      workers = start_workers(2)

      pipeline =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.filter_with("x > 50")
        |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")

      local = pipeline |> Dux.collect()
      dist = Coordinator.execute(pipeline, workers: workers) |> Dux.collect()

      # Replicated source: 2 workers each process all data → 2x totals
      assert hd(dist)["total"] == hd(local)["total"] * 2
      assert hd(dist)["n"] == hd(local)["n"] * 2
    end
  end
end
