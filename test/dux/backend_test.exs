defmodule Dux.BackendTest do
  use ExUnit.Case, async: false

  setup do
    {db, conn} = Dux.Backend.open()
    %{db: db, conn: conn}
  end

  # ---------- Happy path ----------

  describe "open/0 and open/1" do
    test "opens in-memory database" do
      {db, conn} = Dux.Backend.open()
      assert is_pid(db)
      assert is_pid(conn)
    end

    test "opens file-backed database" do
      path =
        Path.join(
          System.tmp_dir!(),
          "dux_backend_test_#{System.unique_integer([:positive])}.duckdb"
        )

      try do
        {db, conn} = Dux.Backend.open(path: path)
        assert is_pid(db)
        assert is_pid(conn)
        # DuckDB creates the file lazily — write data to trigger
        Dux.Backend.execute(conn, "CREATE TABLE test_persist (x INTEGER)")
        Dux.Backend.execute(conn, "INSERT INTO test_persist VALUES (1)")
        assert File.exists?(path)
      after
        File.rm(path)
        File.rm(path <> ".wal")
      end
    end
  end

  describe "execute/2" do
    test "executes DDL", %{conn: conn} do
      assert :ok = Dux.Backend.execute(conn, "CREATE TABLE t (x INTEGER)")
    end
  end

  describe "query/2" do
    test "returns TableRef for simple query", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 42 AS answer")
      assert is_struct(ref, Dux.TableRef)
    end

    test "returns correct column names", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 1 AS a, 2 AS b, 3 AS c")
      assert Dux.Backend.table_names(conn, ref) == ["a", "b", "c"]
    end

    test "returns correct dtypes", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 1::INTEGER AS i, 'hello'::VARCHAR AS s, true AS b")
      dtypes = Dux.Backend.table_dtypes(conn, ref)
      assert [{"i", {:s, 32}}, {"s", :string}, {"b", :boolean}] = dtypes
    end

    test "returns correct row count", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT * FROM range(100)")
      assert Dux.Backend.table_n_rows(conn, ref) == 100
    end
  end

  describe "table_to_columns/2" do
    test "returns map of column name to values", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'")
      columns = Dux.Backend.table_to_columns(conn, ref)
      assert %{"x" => [1, 2], "y" => ["a", "b"]} = columns
    end

    test "handles null values", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 1 AS x UNION ALL SELECT NULL")
      columns = Dux.Backend.table_to_columns(conn, ref)
      assert %{"x" => [1, nil]} = columns
    end

    test "handles empty result", %{conn: conn} do
      Dux.Backend.execute(conn, "CREATE TABLE empty (x INTEGER)")
      ref = Dux.Backend.query(conn, "SELECT * FROM empty")
      columns = Dux.Backend.table_to_columns(conn, ref)
      assert %{"x" => []} = columns
    end

    test "handles multiple types", %{conn: conn} do
      ref =
        Dux.Backend.query(conn, """
          SELECT
            42::BIGINT AS i,
            3.14::DOUBLE AS f,
            'hello'::VARCHAR AS s,
            true AS b
        """)

      columns = Dux.Backend.table_to_columns(conn, ref)
      assert %{"i" => [42], "f" => [f], "s" => ["hello"], "b" => [true]} = columns
      assert_in_delta f, 3.14, 0.001
    end
  end

  describe "table_to_rows/2" do
    test "returns list of maps", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'")
      rows = Dux.Backend.table_to_rows(conn, ref)
      assert [%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}] = rows
    end
  end

  describe "TableRef.name (replaces table_ensure)" do
    test "ref has a table name that can be queried", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 1 AS x, 2 AS y")
      assert is_binary(ref.name)

      # Can query the temp table by name
      result = Dux.Backend.query(conn, "SELECT * FROM \"#{ref.name}\"")
      assert Dux.Backend.table_n_rows(conn, result) == 1
    end

    test "handles large datasets (>2048 rows)", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT * FROM range(5000) t(x)")

      result = Dux.Backend.query(conn, "SELECT COUNT(*) AS cnt FROM \"#{ref.name}\"")
      columns = Dux.Backend.table_to_columns(conn, result)
      assert columns["cnt"] == [5000]
    end
  end

  # ---------- Arrow IPC round-trip ----------

  describe "Arrow IPC serialization" do
    test "round-trips through IPC", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT 42 AS x, 'hello' AS y")
      ipc = Dux.Backend.table_to_ipc(conn, ref)
      assert is_binary(ipc)
      assert byte_size(ipc) > 0

      restored = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_names(conn, restored) == ["x", "y"]
      assert Dux.Backend.table_to_columns(conn, restored) == %{"x" => [42], "y" => ["hello"]}
    end

    test "round-trips empty table", %{conn: conn} do
      Dux.Backend.execute(conn, "CREATE TABLE empty_ipc (a INTEGER, b VARCHAR)")
      ref = Dux.Backend.query(conn, "SELECT * FROM empty_ipc")
      ipc = Dux.Backend.table_to_ipc(conn, ref)
      restored = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_names(conn, restored) == ["a", "b"]
      assert Dux.Backend.table_n_rows(conn, restored) == 0
    end

    test "round-trips large dataset", %{conn: conn} do
      ref = Dux.Backend.query(conn, "SELECT x, x * 2 AS doubled FROM range(10_000) t(x)")
      ipc = Dux.Backend.table_to_ipc(conn, ref)
      restored = Dux.Backend.table_from_ipc(conn, ipc)
      assert Dux.Backend.table_n_rows(conn, restored) == 10_000
    end
  end

  # ---------- Sad path ----------

  describe "error handling" do
    test "query raises on invalid SQL", %{conn: conn} do
      assert_raise ArgumentError, fn ->
        Dux.Backend.query(conn, "THIS IS NOT SQL")
      end
    end

    test "execute raises on invalid SQL", %{conn: conn} do
      assert_raise ArgumentError, fn ->
        Dux.Backend.execute(conn, "DEFINITELY NOT SQL")
      end
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial inputs" do
    test "handles unicode column names", %{conn: conn} do
      ref = Dux.Backend.query(conn, ~s{SELECT 1 AS "日本語", 2 AS "émoji🎉"})
      names = Dux.Backend.table_names(conn, ref)
      assert "日本語" in names
      assert "émoji🎉" in names
    end

    test "handles very wide tables", %{conn: conn} do
      cols = Enum.map_join(1..200, ", ", fn i -> "#{i} AS c#{i}" end)
      ref = Dux.Backend.query(conn, "SELECT #{cols}")
      assert length(Dux.Backend.table_names(conn, ref)) == 200
    end

    test "handles column names with special characters", %{conn: conn} do
      ref = Dux.Backend.query(conn, ~s{SELECT 1 AS "col with spaces", 2 AS "col-with-dashes"})
      names = Dux.Backend.table_names(conn, ref)
      assert "col with spaces" in names
      assert "col-with-dashes" in names
    end
  end

  # ---------- Wicked / pathological ----------

  describe "pathological cases" do
    test "handles cartesian product result", %{conn: conn} do
      Dux.Backend.execute(conn, "CREATE TABLE small AS SELECT * FROM range(100) t(x)")
      # 100 x 100 = 10,000 rows
      ref = Dux.Backend.query(conn, "SELECT a.x, b.x AS y FROM small a, small b")
      assert Dux.Backend.table_n_rows(conn, ref) == 10_000
    end

    test "handles deeply nested query", %{conn: conn} do
      # 50-deep CTE chain
      ctes =
        Enum.reduce(1..50, "SELECT 1 AS x", fn i, acc ->
          "SELECT x + 1 AS x FROM (#{acc}) s#{i}"
        end)

      ref = Dux.Backend.query(conn, ctes)
      columns = Dux.Backend.table_to_columns(conn, ref)
      assert columns["x"] == [51]
    end

    test "temp table name is accessible via ref" do
      {_db, conn} = Dux.Backend.open()
      ref = Dux.Backend.query(conn, "SELECT 1 AS x")

      # Verify table exists and is queryable
      result = Dux.Backend.query(conn, "SELECT * FROM \"#{ref.name}\"")
      assert Dux.Backend.table_n_rows(conn, result) == 1

      # Table name is a string
      assert is_binary(ref.name)
    end
  end
end
