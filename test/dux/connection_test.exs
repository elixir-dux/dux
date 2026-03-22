defmodule Dux.ConnectionTest do
  use ExUnit.Case, async: false

  setup do
    # Start a fresh connection for each test
    name = :"conn_#{System.unique_integer([:positive])}"
    {:ok, pid} = Dux.Connection.start_link(name: name)
    conn = Dux.Connection.get_conn(name)
    %{name: name, pid: pid, conn: conn}
  end

  # ---------- Happy path ----------

  describe "start_link/1" do
    test "opens an in-memory database", %{conn: conn} do
      assert is_pid(conn)
    end

    test "opens a file-backed database" do
      path = Path.join(System.tmp_dir!(), "dux_test_#{System.unique_integer([:positive])}.duckdb")
      name = :"conn_file_#{System.unique_integer([:positive])}"

      try do
        {:ok, _pid} = Dux.Connection.start_link(name: name, path: path)
        conn = Dux.Connection.get_conn(name)
        assert is_pid(conn)
        assert File.exists?(path)
      after
        File.rm(path)
        # DuckDB creates .wal files too
        File.rm(path <> ".wal")
      end
    end
  end

  describe "execute" do
    test "runs DDL statements", %{conn: conn} do
      assert :ok = Dux.Backend.execute(conn, "CREATE TABLE test (id INTEGER, name VARCHAR)")
    end

    test "runs DML statements", %{conn: conn} do
      Dux.Backend.execute(conn, "CREATE TABLE test (id INTEGER)")
      assert :ok = Dux.Backend.execute(conn, "INSERT INTO test VALUES (1), (2), (3)")
    end
  end

  describe "query" do
    test "returns a TableRef", %{conn: conn} do
      result = Dux.Backend.query(conn, "SELECT 1 AS x, 'hello' AS y")
      assert is_struct(result, Dux.TableRef)
    end

    test "handles empty results", %{conn: conn} do
      Dux.Backend.execute(conn, "CREATE TABLE empty (id INTEGER)")
      result = Dux.Backend.query(conn, "SELECT * FROM empty")
      assert is_struct(result, Dux.TableRef)
      assert Dux.Backend.table_n_rows(conn, result) == 0
    end
  end

  describe "load_extension/2" do
    test "loads a built-in extension", %{name: name} do
      # json is a built-in autoloadable extension
      assert :ok = Dux.Connection.load_extension(:json, name)
    end
  end

  # ---------- Sad path ----------

  describe "error handling" do
    test "execute raises on invalid SQL", %{conn: conn} do
      assert_raise ArgumentError, fn ->
        Dux.Backend.execute(conn, "NOT VALID SQL HERE!!!")
      end
    end

    test "query raises on invalid SQL", %{conn: conn} do
      assert_raise ArgumentError, fn ->
        Dux.Backend.query(conn, "SELECT FROM WHERE")
      end
    end

    test "query raises on non-existent table", %{conn: conn} do
      assert_raise ArgumentError, fn ->
        Dux.Backend.query(conn, "SELECT * FROM this_table_does_not_exist")
      end
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial" do
    test "concurrent queries don't corrupt state", %{conn: conn} do
      Dux.Backend.execute(conn, "CREATE TABLE concurrent_test (id INTEGER)")

      Dux.Backend.execute(
        conn,
        "INSERT INTO concurrent_test SELECT * FROM range(1000)"
      )

      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            result = Dux.Backend.query(conn, "SELECT COUNT(*) AS cnt FROM concurrent_test")
            columns = Dux.Backend.table_to_columns(conn, result)
            assert columns["cnt"] == [1000]
            i
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert length(results) == 10
    end

    test "handles very long SQL", %{conn: conn} do
      # Generate a long UNION ALL query
      unions = Enum.map_join(1..100, " UNION ALL ", fn i -> "SELECT #{i} AS x" end)
      result = Dux.Backend.query(conn, unions)
      assert Dux.Backend.table_n_rows(conn, result) == 100
    end
  end
end
