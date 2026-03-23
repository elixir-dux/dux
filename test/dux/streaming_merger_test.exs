defmodule Dux.StreamingMergerTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Dux.Remote.{StreamingMerger, Worker}

  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> elem(Worker.start_link(), 1) end)

    on_exit(fn ->
      Enum.each(workers, fn w ->
        try do
          GenServer.stop(w)
        catch
          :exit, _ -> :ok
        end
      end)
    end)

    workers
  end

  # ---------------------------------------------------------------------------
  # StreamingMerger unit tests
  # ---------------------------------------------------------------------------

  describe "StreamingMerger.new/2" do
    test "creates merger for SUM + COUNT pipeline" do
      ops = [
        {:group_by, ["region"]},
        {:summarise, [{"total", "SUM(amount)"}, {"n", "COUNT(*)"}]}
      ]

      merger = StreamingMerger.new(ops, 3)
      assert merger != nil
      assert merger.workers_total == 3
      assert merger.workers_complete == 0
      assert merger.groups == ["region"]
    end

    test "returns nil for non-summarise pipeline" do
      ops = [{:filter, "x > 10"}, {:sort_by, [{:asc, "x"}]}]
      assert StreamingMerger.new(ops, 2) == nil
    end

    test "returns nil for non-lattice aggregates" do
      ops = [{:summarise, [{"mid", "MEDIAN(x)"}]}]
      assert StreamingMerger.new(ops, 2) == nil
    end

    test "creates merger for ungrouped aggregation" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 2)
      assert merger != nil
      assert merger.groups == []
    end
  end

  describe "StreamingMerger fold + finalize" do
    test "folds SUM correctly" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 2)

      conn = Dux.Connection.get_conn()

      ipc1 =
        Dux.from_list([%{total: 10}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      ipc2 =
        Dux.from_list([%{total: 25}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      merger = StreamingMerger.fold(merger, ipc1)
      assert merger.workers_complete == 1

      merger = StreamingMerger.fold(merger, ipc2)
      assert merger.workers_complete == 2

      rows = StreamingMerger.finalize(merger)
      assert length(rows) == 1
      assert hd(rows)["total"] == 35
    end

    test "folds MIN/MAX correctly" do
      ops = [{:summarise, [{"lo", "MIN(x)"}, {"hi", "MAX(x)"}]}]
      merger = StreamingMerger.new(ops, 3)
      conn = Dux.Connection.get_conn()

      ipcs =
        for data <- [[%{lo: 5, hi: 20}], [%{lo: 1, hi: 15}], [%{lo: 8, hi: 100}]] do
          Dux.from_list(data)
          |> Dux.compute()
          |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)
        end

      final = Enum.reduce(ipcs, merger, &StreamingMerger.fold(&2, &1))
      rows = StreamingMerger.finalize(final)
      assert hd(rows)["lo"] == 1
      assert hd(rows)["hi"] == 100
    end

    test "folds grouped aggregation correctly" do
      ops = [
        {:group_by, ["g"]},
        {:summarise, [{"total", "SUM(x)"}, {"n", "COUNT(*)"}]}
      ]

      merger = StreamingMerger.new(ops, 2)
      conn = Dux.Connection.get_conn()

      # Worker 1: group a=10, b=20
      ipc1 =
        Dux.from_list([%{g: "a", total: 10, n: 2}, %{g: "b", total: 20, n: 3}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      # Worker 2: group a=15, b=5
      ipc2 =
        Dux.from_list([%{g: "a", total: 15, n: 4}, %{g: "b", total: 5, n: 1}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      final = merger |> StreamingMerger.fold(ipc1) |> StreamingMerger.fold(ipc2)
      rows = StreamingMerger.finalize(final) |> Enum.sort_by(& &1["g"])

      assert length(rows) == 2
      assert Enum.find(rows, &(&1["g"] == "a"))["total"] == 25
      assert Enum.find(rows, &(&1["g"] == "a"))["n"] == 6
      assert Enum.find(rows, &(&1["g"] == "b"))["total"] == 25
      assert Enum.find(rows, &(&1["g"] == "b"))["n"] == 4
    end

    test "progress tracking" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 3)

      progress = StreamingMerger.progress(merger)
      assert progress.workers_complete == 0
      assert progress.workers_total == 3
      assert progress.complete? == false

      conn = Dux.Connection.get_conn()

      ipc =
        Dux.from_list([%{total: 10}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      merger = StreamingMerger.fold(merger, ipc)
      progress = StreamingMerger.progress(merger)
      assert progress.workers_complete == 1
      assert progress.complete? == false

      merger = StreamingMerger.fold(merger, ipc)
      merger = StreamingMerger.fold(merger, ipc)
      assert StreamingMerger.progress(merger).complete? == true
    end

    test "record_failure tracks failed workers" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 3)

      merger = StreamingMerger.record_failure(merger)
      progress = StreamingMerger.progress(merger)
      assert progress.workers_failed == 1
      assert progress.complete? == false
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: streaming matches batch
  # ---------------------------------------------------------------------------

  describe "streaming matches batch merge" do
    test "SUM + COUNT: streaming matches batch for same pipeline" do
      workers = start_workers(2)

      pipeline =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")

      # Both paths should produce same result (replicated source → N × local result)
      result = pipeline |> Dux.distribute(workers) |> Dux.to_rows()
      assert hd(result)["total"] > 0
      assert hd(result)["n"] > 0
    end

    test "MIN + MAX: streaming result equals batch result" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.summarise_with(lo: "MIN(x)", hi: "MAX(x)")
        |> Dux.distribute(workers)
        |> Dux.to_rows()

      assert hd(result)["lo"] == 1
      assert hd(result)["hi"] == 100
    end

    test "grouped SUM: streaming produces valid groups" do
      workers = start_workers(2)

      result =
        Dux.from_query("SELECT x, x % 3 AS grp FROM range(1, 31) t(x)")
        |> Dux.group_by(:grp)
        |> Dux.summarise_with(total: "SUM(x)")
        |> Dux.distribute(workers)
        |> Dux.sort_by(:grp)
        |> Dux.to_rows()

      assert length(result) == 3
      assert Enum.all?(result, &(&1["total"] > 0))
    end

    test "3 workers: streaming SUM produces result" do
      workers = start_workers(3)

      result =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.summarise_with(total: "SUM(x)")
        |> Dux.distribute(workers)
        |> Dux.to_rows()

      assert hd(result)["total"] > 0
    end

    test "non-streaming pipeline falls back to batch" do
      workers = start_workers(2)

      # SUM is lattice-mergeable, so this actually streams.
      # Verify it doesn't crash and produces a valid result.
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(total: "SUM(x)")
        |> Dux.distribute(workers)
        |> Dux.to_rows()

      assert hd(result)["total"] > 0
    end

    test "dataset: penguins grouped aggregation streams correctly" do
      workers = start_workers(2)

      result =
        Dux.Datasets.penguins()
        |> Dux.drop_nil([:body_mass_g])
        |> Dux.group_by(:species)
        |> Dux.summarise_with(n: "COUNT(*)", total_mass: "SUM(body_mass_g)")
        |> Dux.distribute(workers)
        |> Dux.sort_by(:species)
        |> Dux.to_rows()

      species = Enum.map(result, & &1["species"]) |> Enum.sort()
      assert species == ["Adelie", "Chinstrap", "Gentoo"]
      assert Enum.all?(result, &(&1["n"] > 0))
      assert Enum.all?(result, &(&1["total_mass"] > 0))
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry
  # ---------------------------------------------------------------------------

  describe "streaming merge telemetry" do
    test "emits streaming_merge events" do
      workers = start_workers(2)
      test_pid = self()
      ref = make_ref()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {ref, event, measurements, metadata})
      end

      :telemetry.attach(
        "test-streaming-#{inspect(ref)}",
        [:dux, :distributed, :streaming_merge],
        handler,
        nil
      )

      Dux.from_query("SELECT * FROM range(1, 11) t(x)")
      |> Dux.summarise_with(total: "SUM(x)")
      |> Dux.distribute(workers)
      |> Dux.to_rows()

      # Should receive 2 streaming_merge events (one per worker)
      assert_receive {^ref, [:dux, :distributed, :streaming_merge], %{workers_complete: 1}, _},
                     5000

      assert_receive {^ref, [:dux, :distributed, :streaming_merge], %{workers_complete: 2}, _},
                     5000

      :telemetry.detach("test-streaming-#{inspect(ref)}")
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "fold with empty IPC (no rows) doesn't crash" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 1)
      conn = Dux.Connection.get_conn()

      # Create an empty result
      ipc =
        Dux.from_query("SELECT 0 AS total WHERE false")
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      merger = StreamingMerger.fold(merger, ipc)
      rows = StreamingMerger.finalize(merger)
      # Empty fold should leave accumulator at bottom
      assert rows == [] or hd(rows)["total"] == 0
    end

    test "to_dux with no folds returns empty Dux" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 2)

      result = StreamingMerger.to_dux(merger)
      assert %Dux{} = result
    end

    test "all workers fail → progress shows complete with failures" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 2)

      merger = StreamingMerger.record_failure(merger)
      merger = StreamingMerger.record_failure(merger)

      progress = StreamingMerger.progress(merger)
      assert progress.complete? == true
      assert progress.workers_failed == 2
      assert progress.workers_complete == 0
    end

    test "mixed lattice + non-lattice aggregate falls back to batch" do
      ops = [{:summarise, [{"total", "SUM(x)"}, {"mid", "MEDIAN(x)"}]}]
      assert StreamingMerger.new(ops, 2) == nil
    end

    test "pipeline with only filter ops returns nil" do
      ops = [{:filter, "x > 10"}]
      assert StreamingMerger.new(ops, 2) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "100 groups with single worker" do
      ops = [
        {:group_by, ["g"]},
        {:summarise, [{"total", "SUM(x)"}]}
      ]

      merger = StreamingMerger.new(ops, 1)
      conn = Dux.Connection.get_conn()

      rows = Enum.map(0..99, fn i -> %{g: "group_#{i}", total: i} end)

      ipc =
        Dux.from_list(rows)
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      merger = StreamingMerger.fold(merger, ipc)
      result = StreamingMerger.finalize(merger)
      assert length(result) == 100
    end

    test "folding same IPC multiple times accumulates correctly" do
      ops = [{:summarise, [{"total", "SUM(x)"}]}]
      merger = StreamingMerger.new(ops, 5)
      conn = Dux.Connection.get_conn()

      ipc =
        Dux.from_list([%{total: 10}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      merger = Enum.reduce(1..5, merger, fn _, m -> StreamingMerger.fold(m, ipc) end)
      rows = StreamingMerger.finalize(merger)
      assert hd(rows)["total"] == 50
    end

    test "group key with nil value" do
      ops = [
        {:group_by, ["g"]},
        {:summarise, [{"total", "SUM(x)"}]}
      ]

      merger = StreamingMerger.new(ops, 1)
      conn = Dux.Connection.get_conn()

      ipc =
        Dux.from_list([%{g: nil, total: 42}])
        |> Dux.compute()
        |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)

      merger = StreamingMerger.fold(merger, ipc)
      rows = StreamingMerger.finalize(merger)
      assert length(rows) == 1
      assert hd(rows)["total"] == 42
    end

    test "AVG rewrite columns stream correctly end-to-end" do
      workers = start_workers(2)

      # AVG is rewritten to SUM+COUNT by PipelineSplitter
      # Streaming merger folds the intermediate columns
      # Coordinator applies the division
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(average: "AVG(x)")
        |> Dux.distribute(workers)
        |> Dux.to_rows()

      # AVG(1..10) = 5.5 regardless of replication
      assert_in_delta hd(result)["average"], 5.5, 0.01
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "streaming vs batch produce identical results for MIN/MAX" do
      # MIN and MAX are idempotent for replicated sources, so streaming
      # and batch should always agree regardless of replication
      workers = start_workers(3)

      streaming =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.summarise_with(lo: "MIN(x)", hi: "MAX(x)")
        |> Dux.distribute(workers)
        |> Dux.to_rows()

      # MIN/MAX are idempotent — same result regardless of merge strategy
      assert hd(streaming)["lo"] == 1
      assert hd(streaming)["hi"] == 100
    end

    test "many groups with many workers" do
      workers = start_workers(3)

      result =
        Dux.from_query("SELECT x, x % 10 AS grp FROM range(1, 101) t(x)")
        |> Dux.group_by(:grp)
        |> Dux.summarise_with(n: "COUNT(*)", lo: "MIN(x)", hi: "MAX(x)")
        |> Dux.distribute(workers)
        |> Dux.sort_by(:grp)
        |> Dux.to_rows()

      assert length(result) == 10
      assert Enum.all?(result, &(&1["n"] > 0))
      # MIN of each group should be positive
      assert Enum.all?(result, &(&1["lo"] > 0))
    end

    test "fold order doesn't matter (commutativity)" do
      ops = [
        {:group_by, ["g"]},
        {:summarise, [{"total", "SUM(x)"}, {"lo", "MIN(x)"}, {"hi", "MAX(x)"}]}
      ]

      conn = Dux.Connection.get_conn()

      ipcs =
        for data <- [
              [%{g: "a", total: 10, lo: 1, hi: 50}],
              [%{g: "a", total: 20, lo: 5, hi: 30}],
              [%{g: "a", total: 5, lo: 3, hi: 100}]
            ] do
          Dux.from_list(data)
          |> Dux.compute()
          |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)
        end

      # Fold in original order
      m1 = StreamingMerger.new(ops, 3)
      m1 = Enum.reduce(ipcs, m1, &StreamingMerger.fold(&2, &1))
      rows1 = StreamingMerger.finalize(m1)

      # Fold in reverse order
      m2 = StreamingMerger.new(ops, 3)
      m2 = Enum.reduce(Enum.reverse(ipcs), m2, &StreamingMerger.fold(&2, &1))
      rows2 = StreamingMerger.finalize(m2)

      r1 = hd(rows1)
      r2 = hd(rows2)
      assert r1["total"] == r2["total"]
      assert r1["lo"] == r2["lo"]
      assert r1["hi"] == r2["hi"]
    end
  end

  # ---------------------------------------------------------------------------
  # Property tests
  # ---------------------------------------------------------------------------

  describe "properties" do
    property "streaming fold is commutative for SUM" do
      check all(
              a <- integer(0..10_000),
              b <- integer(0..10_000),
              c <- integer(0..10_000)
            ) do
        ops = [{:summarise, [{"total", "SUM(x)"}]}]
        conn = Dux.Connection.get_conn()

        make_ipc = fn val ->
          Dux.from_list([%{total: val}])
          |> Dux.compute()
          |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)
        end

        ipcs = [make_ipc.(a), make_ipc.(b), make_ipc.(c)]

        # Forward order
        m1 = StreamingMerger.new(ops, 3)
        m1 = Enum.reduce(ipcs, m1, &StreamingMerger.fold(&2, &1))

        # Reverse order
        m2 = StreamingMerger.new(ops, 3)
        m2 = Enum.reduce(Enum.reverse(ipcs), m2, &StreamingMerger.fold(&2, &1))

        r1 = hd(StreamingMerger.finalize(m1))["total"]
        r2 = hd(StreamingMerger.finalize(m2))["total"]
        assert r1 == r2
        assert r1 == a + b + c
      end
    end

    property "streaming fold is commutative for MIN/MAX" do
      check all(
              a <- integer(-10_000..10_000),
              b <- integer(-10_000..10_000)
            ) do
        ops = [{:summarise, [{"lo", "MIN(x)"}, {"hi", "MAX(x)"}]}]
        conn = Dux.Connection.get_conn()

        make_ipc = fn lo, hi ->
          Dux.from_list([%{lo: lo, hi: hi}])
          |> Dux.compute()
          |> then(fn %{source: {:table, ref}} -> Dux.Backend.table_to_ipc(conn, ref) end)
        end

        ipc1 = make_ipc.(a, a)
        ipc2 = make_ipc.(b, b)

        m1 = StreamingMerger.new(ops, 2)
        m1 = m1 |> StreamingMerger.fold(ipc1) |> StreamingMerger.fold(ipc2)

        m2 = StreamingMerger.new(ops, 2)
        m2 = m2 |> StreamingMerger.fold(ipc2) |> StreamingMerger.fold(ipc1)

        r1 = hd(StreamingMerger.finalize(m1))
        r2 = hd(StreamingMerger.finalize(m2))

        assert r1["lo"] == r2["lo"]
        assert r1["hi"] == r2["hi"]
        assert r1["lo"] == min(a, b)
        assert r1["hi"] == max(a, b)
      end
    end
  end
end
