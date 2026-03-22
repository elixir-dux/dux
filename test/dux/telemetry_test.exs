defmodule Dux.TelemetryTest do
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

  defp stop(w) do
    GenServer.stop(w)
  catch
    :exit, _ -> :ok
  end

  defp attach(event_name, test_pid \\ self()) do
    ref = make_ref()
    handler_id = "test-#{inspect(ref)}"

    :telemetry.attach(
      handler_id,
      event_name,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp attach_many(event_names, test_pid \\ self()) do
    ref = make_ref()
    handler_id = "test-#{inspect(ref)}"

    :telemetry.attach_many(
      handler_id,
      event_names,
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  # ---------------------------------------------------------------------------
  # Core query events
  # ---------------------------------------------------------------------------

  describe "[:dux, :query, :*]" do
    test "emits start and stop for local compute" do
      attach_many([[:dux, :query, :start], [:dux, :query, :stop]])

      Dux.from_list([%{x: 1}, %{x: 2}]) |> Dux.compute()

      assert_receive {:telemetry, [:dux, :query, :start], %{system_time: _},
                      %{n_ops: 0, distributed: false}}

      assert_receive {:telemetry, [:dux, :query, :stop], %{duration: d}, %{n_rows: 2}} when d > 0
    end

    test "emits stop with correct n_rows" do
      attach([:dux, :query, :stop])

      Dux.from_query("SELECT * FROM range(42) t(x)") |> Dux.compute()

      assert_receive {:telemetry, [:dux, :query, :stop], _, %{n_rows: 42}}
    end

    test "emits exception on failure" do
      attach([:dux, :query, :exception])

      assert_raise ArgumentError, fn ->
        Dux.from_query("INVALID SQL GARBAGE") |> Dux.compute()
      end

      assert_receive {:telemetry, [:dux, :query, :exception], %{duration: _},
                      %{kind: _, reason: _}}
    end

    test "distributed compute emits with distributed: true" do
      workers = start_workers(2)
      attach([:dux, :query, :stop])

      Dux.from_list([%{x: 1}])
      |> Dux.distribute(workers)
      |> Dux.compute()

      assert_receive {:telemetry, [:dux, :query, :stop], _, %{distributed: true}}
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed events
  # ---------------------------------------------------------------------------

  describe "distributed events" do
    test "fan_out start/stop" do
      workers = start_workers(2)
      attach_many([[:dux, :distributed, :fan_out, :start], [:dux, :distributed, :fan_out, :stop]])

      Dux.from_list([%{x: 1}])
      |> Dux.distribute(workers)
      |> Dux.compute()

      assert_receive {:telemetry, [:dux, :distributed, :fan_out, :start], _, %{n_workers: 2}}

      assert_receive {:telemetry, [:dux, :distributed, :fan_out, :stop], %{duration: _},
                      %{n_workers: 2}}
    end

    test "per-worker stop events" do
      workers = start_workers(2)
      attach([:dux, :distributed, :worker, :stop])

      Dux.from_list([%{x: 1}])
      |> Dux.distribute(workers)
      |> Dux.compute()

      assert_receive {:telemetry, [:dux, :distributed, :worker, :stop],
                      %{duration: _, ipc_bytes: _}, %{worker_index: 0, n_workers: 2}}

      assert_receive {:telemetry, [:dux, :distributed, :worker, :stop],
                      %{duration: _, ipc_bytes: _}, %{worker_index: 1, n_workers: 2}}
    end

    test "merge start/stop" do
      workers = start_workers(2)
      attach_many([[:dux, :distributed, :merge, :start], [:dux, :distributed, :merge, :stop]])

      Dux.from_list([%{x: 1}])
      |> Dux.distribute(workers)
      |> Dux.compute()

      assert_receive {:telemetry, [:dux, :distributed, :merge, :start], _, %{n_results: 2}}
      assert_receive {:telemetry, [:dux, :distributed, :merge, :stop], %{duration: _}, _}
    end

    test "broadcast event when joining local right" do
      workers = start_workers(1)

      attach_many([
        [:dux, :distributed, :broadcast, :start],
        [:dux, :distributed, :broadcast, :stop]
      ])

      left = Dux.from_list([%{id: 1}]) |> Dux.distribute(workers)
      right = Dux.from_list([%{id: 1, name: "x"}]) |> Dux.compute()

      left |> Dux.join(right, on: :id) |> Dux.compute()

      assert_receive {:telemetry, [:dux, :distributed, :broadcast, :start], _,
                      %{n_workers: 1, ipc_bytes: _}}

      assert_receive {:telemetry, [:dux, :distributed, :broadcast, :stop], %{duration: _}, _}
    end
  end

  # ---------------------------------------------------------------------------
  # Graph events
  # ---------------------------------------------------------------------------

  describe "graph algorithm events" do
    setup do
      edges = Dux.from_list([%{src: 1, dst: 2}, %{src: 2, dst: 3}, %{src: 3, dst: 1}])
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])
      graph = Dux.Graph.new(vertices: vertices, edges: edges)
      %{graph: graph}
    end

    test "pagerank emits algorithm start/stop", %{graph: graph} do
      attach_many([[:dux, :graph, :algorithm, :start], [:dux, :graph, :algorithm, :stop]])

      Dux.Graph.pagerank(graph)

      assert_receive {:telemetry, [:dux, :graph, :algorithm, :start], _,
                      %{algorithm: :pagerank, n_vertices: 3, n_edges: 3, distributed: false}}

      assert_receive {:telemetry, [:dux, :graph, :algorithm, :stop], %{duration: _},
                      %{algorithm: :pagerank}}
    end

    test "connected_components emits algorithm events", %{graph: graph} do
      attach([:dux, :graph, :algorithm, :stop])

      Dux.Graph.connected_components(graph)

      assert_receive {:telemetry, [:dux, :graph, :algorithm, :stop], _,
                      %{algorithm: :connected_components}}
    end

    test "triangle_count emits algorithm events", %{graph: graph} do
      attach([:dux, :graph, :algorithm, :stop])

      Dux.Graph.triangle_count(graph)

      assert_receive {:telemetry, [:dux, :graph, :algorithm, :stop], _,
                      %{algorithm: :triangle_count}}
    end

    test "shortest_paths emits algorithm events", %{graph: graph} do
      attach([:dux, :graph, :algorithm, :stop])

      Dux.Graph.shortest_paths(graph, 1)

      assert_receive {:telemetry, [:dux, :graph, :algorithm, :stop], _,
                      %{algorithm: :shortest_paths}}
    end
  end

  # ---------------------------------------------------------------------------
  # IO events
  # ---------------------------------------------------------------------------

  describe "IO events" do
    @tmp_dir System.tmp_dir!()

    test "write emits start/stop" do
      path = Path.join(@tmp_dir, "dux_telemetry_test_#{System.unique_integer([:positive])}.csv")
      attach_many([[:dux, :io, :write, :start], [:dux, :io, :write, :stop]])

      try do
        Dux.from_list([%{x: 1}]) |> Dux.to_csv(path)

        assert_receive {:telemetry, [:dux, :io, :write, :start], _, %{format: :csv, path: ^path}}
        assert_receive {:telemetry, [:dux, :io, :write, :stop], %{duration: _}, %{format: :csv}}
      after
        File.rm(path)
      end
    end
  end
end
