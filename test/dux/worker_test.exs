defmodule Dux.WorkerTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "start_link/1" do
    test "starts a worker and joins pg group" do
      {:ok, pid} = Worker.start_link()
      assert Process.alive?(pid)
      assert pid in Worker.list()
      GenServer.stop(pid)
    end

    test "multiple workers register independently" do
      {:ok, w1} = Worker.start_link()
      {:ok, w2} = Worker.start_link()
      {:ok, w3} = Worker.start_link()

      workers = Worker.list()
      assert w1 in workers
      assert w2 in workers
      assert w3 in workers

      Enum.each([w1, w2, w3], &GenServer.stop/1)
    end
  end

  describe "execute/2" do
    setup do
      {:ok, pid} = Worker.start_link()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
      %{worker: pid}
    end

    test "executes a simple pipeline and returns IPC", %{worker: w} do
      pipeline = Dux.from_query("SELECT 42 AS answer")
      {:ok, ipc} = Worker.execute(w, pipeline)
      assert is_binary(ipc)
      assert byte_size(ipc) > 0

      # Decode the IPC to verify
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_to_columns(conn, ref) == %{"answer" => [42]}
    end

    test "executes pipeline with filter and mutate", %{worker: w} do
      pipeline =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.filter(x > 5)
        |> Dux.mutate(doubled: x * 2)

      {:ok, ipc} = Worker.execute(w, pipeline)
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      columns = Dux.Backend.table_to_columns(conn, ref)

      assert columns["x"] == [6, 7, 8, 9, 10]
      assert columns["doubled"] == [12, 14, 16, 18, 20]
    end

    test "executes group_by + summarise", %{worker: w} do
      pipeline =
        Dux.from_list([
          %{"g" => "a", "v" => 1},
          %{"g" => "a", "v" => 2},
          %{"g" => "b", "v" => 3}
        ])
        |> Dux.group_by(:g)
        |> Dux.summarise(total: sum(v))

      {:ok, ipc} = Worker.execute(w, pipeline)
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      rows = Dux.Backend.table_to_rows(conn, ref)

      totals = rows |> Enum.sort_by(& &1["g"]) |> Enum.map(& &1["total"])
      assert totals == [3, 3]
    end

    test "returns error on bad SQL", %{worker: w} do
      pipeline = Dux.from_query("THIS IS NOT SQL")
      assert {:error, _reason} = Worker.execute(w, pipeline)
    end
  end

  describe "register_table/3 and drop_table/2" do
    setup do
      {:ok, pid} = Worker.start_link()
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
      %{worker: pid}
    end

    test "registers an IPC binary as a named table", %{worker: w} do
      # Create some data and serialize to IPC
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT 1 AS x, 2 AS y")
      ipc = Dux.Backend.table_to_ipc(conn, ref)

      # Register on worker
      {:ok, "broadcast_dim"} = Worker.register_table(w, "broadcast_dim", ipc)

      # Query it through a pipeline
      pipeline = Dux.from_query(~s(SELECT * FROM "broadcast_dim"))
      {:ok, result_ipc} = Worker.execute(w, pipeline)
      result = Dux.Backend.table_from_ipc(conn, result_ipc)
      assert Dux.Backend.table_to_columns(conn, result) == %{"x" => [1], "y" => [2]}
    end

    test "drops a registered table", %{worker: w} do
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT 1 AS x")
      ipc = Dux.Backend.table_to_ipc(conn, ref)

      Worker.register_table(w, "to_drop", ipc)
      assert :ok = Worker.drop_table(w, "to_drop")

      # Querying should fail now
      pipeline = Dux.from_query(~s(SELECT * FROM "to_drop"))
      assert {:error, _} = Worker.execute(w, pipeline)
    end
  end

  describe "info/1" do
    test "returns worker info" do
      {:ok, pid} = Worker.start_link()

      info = Worker.info(pid)
      assert info.node == node()
      assert info.pid == pid
      assert info.tables == []

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "execute with non-existent source returns error" do
      {:ok, w} = Worker.start_link()

      pipeline = Dux.from_parquet("/nonexistent/file.parquet")
      assert {:error, _} = Worker.execute(w, pipeline)

      GenServer.stop(w)
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "concurrent executions on same worker" do
      {:ok, w} = Worker.start_link()

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            pipeline = Dux.from_query("SELECT #{i} AS val")
            {:ok, ipc} = Worker.execute(w, pipeline)
            conn = Dux.Connection.get_conn()
            ref = Dux.Backend.table_from_ipc(conn, ipc)
            %{"val" => [val]} = Dux.Backend.table_to_columns(conn, ref)
            val
          end)
        end

      results = Task.await_many(tasks, 10_000) |> Enum.sort()
      assert results == Enum.to_list(1..10)

      GenServer.stop(w)
    end

    test "worker survives after error and continues working" do
      {:ok, w} = Worker.start_link()

      # Cause an error
      assert {:error, _} = Worker.execute(w, Dux.from_query("BAD SQL"))

      # Should still work
      {:ok, ipc} = Worker.execute(w, Dux.from_query("SELECT 1 AS x"))
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_to_columns(conn, ref) == %{"x" => [1]}

      GenServer.stop(w)
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "register table then join against it in a pipeline" do
      {:ok, w} = Worker.start_link()

      # Register dimension table
      conn = Dux.Connection.get_conn()

      dim =
        Dux.Backend.query(conn, "SELECT 1 AS id, 'Widget' AS name UNION ALL SELECT 2, 'Gadget'")

      dim_ipc = Dux.Backend.table_to_ipc(conn, dim)
      Worker.register_table(w, "products", dim_ipc)

      # Execute a pipeline that joins against the broadcast table
      pipeline =
        Dux.from_list([
          %{"product_id" => 1, "qty" => 5},
          %{"product_id" => 2, "qty" => 3}
        ])
        |> Dux.join(
          Dux.from_query(~s(SELECT * FROM "products")),
          on: [{:product_id, :id}]
        )

      {:ok, result_ipc} = Worker.execute(w, pipeline)
      result = Dux.Backend.table_from_ipc(conn, result_ipc)
      rows = Dux.Backend.table_to_rows(conn, result) |> Enum.sort_by(& &1["product_id"])

      assert length(rows) == 2
      assert Enum.at(rows, 0)["name"] == "Widget"
      assert Enum.at(rows, 1)["name"] == "Gadget"

      GenServer.stop(w)
    end

    test "large result through worker" do
      {:ok, w} = Worker.start_link()

      pipeline = Dux.from_query("SELECT x, x * 2 AS doubled FROM range(10000) t(x)")
      {:ok, ipc} = Worker.execute(w, pipeline)
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_n_rows(conn, ref) == 10_000

      GenServer.stop(w)
    end
  end

  # ---------------------------------------------------------------------------
  # pg group lifecycle
  # ---------------------------------------------------------------------------

  describe "pg group" do
    test "worker leaves group on stop" do
      {:ok, w} = Worker.start_link()
      assert w in Worker.list()

      GenServer.stop(w)
      Process.sleep(50)

      refute w in Worker.list()
    end

    test "worker leaves group on crash" do
      Process.flag(:trap_exit, true)
      {:ok, w} = Worker.start_link()
      assert w in Worker.list()

      Process.exit(w, :kill)
      assert_receive {:EXIT, ^w, :killed}, 1000
      Process.sleep(50)

      refute w in Worker.list()
    end
  end
end
