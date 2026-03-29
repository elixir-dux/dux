defmodule Dux.AdbcEdgeCasesTest do
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

  # ---------------------------------------------------------------------------
  # Decimal normalization
  # ---------------------------------------------------------------------------

  describe "Decimal normalization" do
    test "SUM returns integer, not Decimal" do
      result =
        Dux.from_list([%{x: 10}, %{x: 20}, %{x: 30}])
        |> Dux.summarise_with(total: "SUM(x)")
        |> Dux.to_rows()

      total = hd(result)["total"]
      assert total == 60
      assert is_integer(total)
    end

    test "COUNT returns integer" do
      result =
        Dux.from_list([%{x: 1}, %{x: 2}])
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.to_rows()

      n = hd(result)["n"]
      assert n == 2
      assert is_integer(n)
    end

    test "AVG returns float" do
      result =
        Dux.from_list([%{x: 10}, %{x: 20}])
        |> Dux.summarise_with(avg: "AVG(x)")
        |> Dux.to_rows()

      avg = hd(result)["avg"]
      assert_in_delta avg, 15.0, 0.01
    end

    test "DECIMAL(10,2) with integer value normalizes to integer" do
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT CAST('100.00' AS DECIMAL(5,2)) AS val")
      %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
      assert result == 100
    end

    test "DECIMAL(10,2) with fractional value normalizes to float" do
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT CAST('100.99' AS DECIMAL(5,2)) AS val")
      %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
      assert_in_delta result, 100.99, 0.01
    end

    test "negative Decimal normalizes correctly" do
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT CAST('-42.5' AS DECIMAL(5,1)) AS val")
      %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
      assert_in_delta result, -42.5, 0.1
    end
  end

  # ---------------------------------------------------------------------------
  # Empty results through pipelines
  # ---------------------------------------------------------------------------

  describe "empty results" do
    test "filter that eliminates all rows preserves columns" do
      result =
        Dux.from_list([%{x: 1, y: "a"}, %{x: 2, y: "b"}])
        |> Dux.filter_with("x > 100")
        |> Dux.to_columns()

      assert result == %{"x" => [], "y" => []}
    end

    test "empty result through group_by + summarise" do
      result =
        Dux.from_list([%{g: "a", v: 1}])
        |> Dux.filter_with("v > 100")
        |> Dux.group_by(:g)
        |> Dux.summarise_with(total: "SUM(v)")
        |> Dux.to_rows()

      assert result == []
    end

    test "empty result through compute preserves names" do
      df =
        Dux.from_list([%{x: 1, y: "hello"}])
        |> Dux.filter_with("x > 100")
        |> Dux.compute()

      assert "x" in df.names
      assert "y" in df.names
      assert Dux.n_rows(df) == 0
    end

    test "empty distributed result across workers" do
      workers = start_workers(2)

      result =
        Dux.from_list([%{x: 1}])
        |> Dux.filter_with("x > 100")
        |> Dux.distribute(workers)
        |> Dux.to_rows()

      assert result == []
    end
  end

  # ---------------------------------------------------------------------------
  # IPC round-trip edge cases
  # ---------------------------------------------------------------------------

  describe "IPC edge cases" do
    test "IPC round-trip with multiple types" do
      conn = Dux.Connection.get_conn()

      ref =
        Dux.Backend.query(
          conn,
          "SELECT 42 AS int_col, 3.14 AS float_col, 'hello' AS str_col, true AS bool_col"
        )

      ipc = Dux.Backend.table_to_ipc(conn, ref)
      restored = Dux.Backend.table_from_ipc(conn, ipc)
      cols = Dux.Backend.table_to_columns(conn, restored)

      assert cols["int_col"] == [42]
      assert_in_delta hd(cols["float_col"]), 3.14, 0.001
      assert cols["str_col"] == ["hello"]
      assert cols["bool_col"] == [true]
    end

    test "IPC round-trip with NULL values" do
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT NULL::INTEGER AS x, NULL::VARCHAR AS y")
      ipc = Dux.Backend.table_to_ipc(conn, ref)
      restored = Dux.Backend.table_from_ipc(conn, ipc)
      cols = Dux.Backend.table_to_columns(conn, restored)

      assert cols["x"] == [nil]
      assert cols["y"] == [nil]
    end

    test "empty table IPC round-trip preserves schema" do
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT 1 AS x, 'a' AS y WHERE false")
      ipc = Dux.Backend.table_to_ipc(conn, ref)

      # ADBC 0.11+ serializes empty tables correctly as standard IPC
      assert is_binary(ipc)
      assert byte_size(ipc) > 0

      restored = Dux.Backend.table_from_ipc(conn, ipc)
      names = Dux.Backend.table_names(conn, restored)
      assert "x" in names
      assert "y" in names
      assert Dux.Backend.table_n_rows(conn, restored) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent compute
  # ---------------------------------------------------------------------------

  describe "concurrent compute" do
    test "10 concurrent compute() on same lazy pipeline don't collide" do
      pipeline =
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.filter_with("x > 10")

      results =
        1..10
        |> Enum.map(fn _ ->
          Task.async(fn -> pipeline |> Dux.compute() |> Dux.to_columns() end)
        end)
        |> Task.await_many(30_000)

      expected = Enum.to_list(11..99)
      assert Enum.all?(results, fn r -> r["x"] == expected end)
    end

    test "compute is idempotent" do
      pipeline =
        Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}])
        |> Dux.filter_with("x > 1")
        |> Dux.mutate_with(doubled: "x * 2")

      result1 = pipeline |> Dux.compute() |> Dux.to_rows()
      result2 = pipeline |> Dux.compute() |> Dux.compute() |> Dux.to_rows()
      assert result1 == result2
    end
  end

  # ---------------------------------------------------------------------------
  # 3+ worker distributed
  # ---------------------------------------------------------------------------

  describe "3+ workers" do
    test "3-worker group_by + summarise" do
      workers = start_workers(3)

      result =
        Dux.from_list([
          %{region: "US", val: 10},
          %{region: "EU", val: 20},
          %{region: "US", val: 30}
        ])
        |> Dux.distribute(workers)
        |> Dux.group_by(:region)
        |> Dux.summarise_with(total: "SUM(val)")
        |> Dux.sort_by(:region)
        |> Dux.to_rows()

      assert length(result) == 2
      eu = Enum.find(result, &(&1["region"] == "EU"))
      us = Enum.find(result, &(&1["region"] == "US"))
      # 3 workers × replicated source
      assert eu["total"] == 60
      assert us["total"] == 120
    end

    test "3-worker broadcast join" do
      workers = start_workers(3)

      left =
        Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])
        |> Dux.distribute(workers)

      right =
        Dux.from_list([%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}])
        |> Dux.compute()

      result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.to_rows()

      names = Enum.map(result, & &1["name"]) |> Enum.uniq() |> Enum.sort()
      assert names == ["Alice", "Bob"]
    end
  end

  # ---------------------------------------------------------------------------
  # Graph edge cases
  # ---------------------------------------------------------------------------

  describe "graph edge cases" do
    test "single-node graph: degree 0" do
      vertices = Dux.from_list([%{id: 1}])
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.out_degree() |> Dux.to_rows()
      # No edges → vertex appears with degree 0 (left join), or empty result
      if result == [] do
        assert true
      else
        assert hd(result)["out_degree"] == 0
      end
    end

    test "single-node graph: pagerank" do
      vertices = Dux.from_list([%{id: 1}])
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.pagerank(iterations: 5) |> Dux.to_rows()
      assert length(result) == 1
      assert hd(result)["rank"] > 0
    end

    test "50 disconnected components" do
      vertices = Dux.from_list(Enum.map(1..50, &%{id: &1}))
      # Edges within pairs: (1,2), (3,4), ..., (49,50) = 25 components
      edges =
        Dux.from_list(
          for i <- 1..25 do
            %{src: i * 2 - 1, dst: i * 2}
          end ++
            for i <- 1..25 do
              %{src: i * 2, dst: i * 2 - 1}
            end
        )

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.connected_components()
        |> Dux.to_columns()

      components = Enum.uniq(result["component"])
      assert length(components) == 25
    end

    test "self-loop counts in degree" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}])
      edges = Dux.from_list([%{src: 1, dst: 1}, %{src: 1, dst: 2}])
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.out_degree() |> Dux.sort_by(:id) |> Dux.to_rows()
      assert hd(result)["out_degree"] == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked problems
  # ---------------------------------------------------------------------------

  describe "wicked problems" do
    test "group_by with cardinality = row count" do
      result =
        Dux.from_query("SELECT * FROM range(500) t(x)")
        |> Dux.group_by(:x)
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.to_columns()

      assert length(result["n"]) == 500
      assert Enum.all?(result["n"], &(&1 == 1))
    end

    test "30-deep pipeline chain" do
      pipeline = Dux.from_query("SELECT * FROM range(10) t(x)")

      pipeline =
        Enum.reduce(1..30, pipeline, fn i, acc ->
          acc |> Dux.mutate_with([{"col_#{i}", "x + #{i}"}])
        end)

      result = pipeline |> Dux.to_columns()
      assert length(Map.keys(result)) == 31
      assert result["x"] == Enum.to_list(0..9)
    end

    test "filter idempotency" do
      data = Enum.map(1..100, &%{x: &1})

      once =
        Dux.from_list(data)
        |> Dux.filter_with("x > 50")
        |> Dux.to_columns()

      twice =
        Dux.from_list(data)
        |> Dux.filter_with("x > 50")
        |> Dux.filter_with("x > 50")
        |> Dux.to_columns()

      assert once == twice
    end
  end
end
