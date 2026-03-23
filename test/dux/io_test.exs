defmodule Dux.IOTest do
  use ExUnit.Case, async: false

  @tmp_dir System.tmp_dir!()

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_io_test_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # CSV
  # ---------------------------------------------------------------------------

  describe "CSV round-trip" do
    test "write and read CSV" do
      path = tmp_path("basic.csv")

      try do
        Dux.from_list([%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}])
        |> Dux.to_csv(path)

        assert File.exists?(path)

        result =
          Dux.from_csv(path)
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert result["x"] == [1, 2]
        assert result["y"] == ["a", "b"]
      after
        File.rm(path)
      end
    end

    test "write CSV with custom delimiter" do
      path = tmp_path("tab.csv")

      try do
        Dux.from_list([%{"a" => 1, "b" => 2}])
        |> Dux.to_csv(path, delimiter: "\t")

        content = File.read!(path)
        assert content =~ "\t"

        result =
          Dux.from_csv(path, delimiter: "\t")
          |> Dux.to_rows()

        assert [%{"a" => 1, "b" => 2}] = result
      after
        File.rm(path)
      end
    end

    test "CSV preserves many rows" do
      rows = Enum.map(1..1000, &%{"x" => &1, "label" => "row_#{&1}"})
      path = tmp_path("large.csv")

      try do
        Dux.from_list(rows)
        |> Dux.to_csv(path)

        assert Dux.from_csv(path) |> Dux.n_rows() == 1000
      after
        File.rm(path)
      end
    end

    test "CSV with pipeline before write" do
      path = tmp_path("filtered.csv")

      try do
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.filter_with("x > 5")
        |> Dux.mutate_with(doubled: "x * 2")
        |> Dux.to_csv(path)

        result =
          Dux.from_csv(path)
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert result["x"] == [6, 7, 8, 9, 10]
        assert result["doubled"] == [12, 14, 16, 18, 20]
      after
        File.rm(path)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Parquet
  # ---------------------------------------------------------------------------

  describe "Parquet round-trip" do
    test "write and read Parquet" do
      path = tmp_path("basic.parquet")

      try do
        Dux.from_list([%{"x" => 1, "y" => "hello"}, %{"x" => 2, "y" => "world"}])
        |> Dux.to_parquet(path)

        assert File.exists?(path)

        result =
          Dux.from_parquet(path)
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert result["x"] == [1, 2]
        assert result["y"] == ["hello", "world"]
      after
        File.rm(path)
      end
    end

    test "Parquet with zstd compression" do
      path = tmp_path("compressed.parquet")

      try do
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.to_parquet(path, compression: :zstd)

        assert File.exists?(path)

        result = Dux.from_parquet(path) |> Dux.n_rows()
        assert result == 100
      after
        File.rm(path)
      end
    end

    test "Parquet preserves types" do
      path = tmp_path("types.parquet")

      try do
        Dux.from_query("""
          SELECT
            42::BIGINT AS i,
            3.14::DOUBLE AS f,
            true AS b,
            'hello'::VARCHAR AS s
        """)
        |> Dux.to_parquet(path)

        result = Dux.from_parquet(path) |> Dux.to_rows()
        row = hd(result)
        assert row["i"] == 42
        assert_in_delta row["f"], 3.14, 0.001
        assert row["b"] == true
        assert row["s"] == "hello"
      after
        File.rm(path)
      end
    end

    test "Parquet glob pattern" do
      dir = tmp_path("parquet_dir")
      File.mkdir_p!(dir)

      try do
        for i <- 1..3 do
          Dux.from_list([%{"x" => i, "part" => i}])
          |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert result["x"] == [1, 2, 3]
      after
        File.rm_rf!(dir)
      end
    end

    test "Parquet large dataset" do
      path = tmp_path("large.parquet")

      try do
        Dux.from_query("SELECT x, x * 2 AS doubled FROM range(10000) t(x)")
        |> Dux.to_parquet(path)

        assert Dux.from_parquet(path) |> Dux.n_rows() == 10_000
      after
        File.rm(path)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # NDJSON
  # ---------------------------------------------------------------------------

  describe "NDJSON round-trip" do
    test "write and read NDJSON" do
      path = tmp_path("basic.ndjson")

      try do
        Dux.from_list([%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}])
        |> Dux.to_ndjson(path)

        assert File.exists?(path)

        result =
          Dux.from_ndjson(path)
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert result["x"] == [1, 2]
        assert result["y"] == ["a", "b"]
      after
        File.rm(path)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Extension loading
  # ---------------------------------------------------------------------------

  describe "extension loading" do
    test "load_extension works for built-in extensions" do
      assert :ok = Dux.Connection.load_extension(:json)
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "from_csv with non-existent file raises on compute" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_csv("/nonexistent/path/file.csv")
        |> Dux.compute()
      end
    end

    test "from_parquet with non-existent file raises on compute" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_parquet("/nonexistent/path/file.parquet")
        |> Dux.compute()
      end
    end

    test "to_csv with invalid path raises" do
      assert_raise ArgumentError, ~r/DuckDB write failed/, fn ->
        Dux.from_list([%{"x" => 1}])
        |> Dux.to_csv("/nonexistent/dir/file.csv")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "CSV with single quotes in data" do
      path = tmp_path("quotes.csv")

      try do
        Dux.from_list([%{"name" => "it's a test"}, %{"name" => "they're here"}])
        |> Dux.to_csv(path)

        result =
          Dux.from_csv(path)
          |> Dux.to_columns()

        assert "it's a test" in result["name"]
        assert "they're here" in result["name"]
      after
        File.rm(path)
      end
    end

    test "CSV with newlines in data" do
      path = tmp_path("newlines.csv")

      try do
        Dux.from_list([%{"text" => "line1\nline2"}, %{"text" => "no newline"}])
        |> Dux.to_csv(path)

        result =
          Dux.from_csv(path)
          |> Dux.to_columns()

        assert "line1\nline2" in result["text"]
      after
        File.rm(path)
      end
    end

    test "Parquet with null values" do
      path = tmp_path("nulls.parquet")

      try do
        Dux.from_query("SELECT 1 AS x, NULL AS y UNION ALL SELECT NULL, 'hello'")
        |> Dux.to_parquet(path)

        result =
          Dux.from_parquet(path)
          |> Dux.sort_by(:x)
          |> Dux.to_columns()

        assert nil in result["x"]
        assert nil in result["y"]
      after
        File.rm(path)
      end
    end

    test "empty dataframe round-trips through Parquet" do
      path = tmp_path("empty.parquet")

      try do
        Dux.from_query("SELECT 1 AS x WHERE false")
        |> Dux.to_parquet(path)

        assert Dux.from_parquet(path) |> Dux.n_rows() == 0
      after
        File.rm(path)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "write → read → filter → write → read preserves data" do
      path1 = tmp_path("step1.parquet")
      path2 = tmp_path("step2.parquet")

      try do
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.to_parquet(path1)

        Dux.from_parquet(path1)
        |> Dux.filter_with("x > 50")
        |> Dux.to_parquet(path2)

        result = Dux.from_parquet(path2) |> Dux.n_rows()
        assert result == 49
      after
        File.rm(path1)
        File.rm(path2)
      end
    end

    test "many small Parquet files via glob" do
      dir = tmp_path("many_parts")
      File.mkdir_p!(dir)

      try do
        for i <- 1..20 do
          Dux.from_list([%{"x" => i}])
          |> Dux.to_parquet(
            Path.join(dir, "part_#{String.pad_leading(to_string(i), 3, "0")}.parquet")
          )
        end

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.n_rows()

        assert result == 20
      after
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # insert_into
  # ---------------------------------------------------------------------------

  describe "insert_into" do
    test "create: true creates a new table from pipeline" do
      require Dux

      Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}])
      |> Dux.filter(x > 1)
      |> Dux.insert_into("__dux_insert_test_create", create: true)

      result =
        Dux.from_query("SELECT * FROM __dux_insert_test_create ORDER BY x")
        |> Dux.to_columns()

      assert result == %{"x" => [2, 3]}
    after
      conn = Dux.Connection.get_conn()
      Adbc.Connection.query(conn, "DROP TABLE IF EXISTS __dux_insert_test_create")
    end

    test "inserts into an existing table" do
      conn = Dux.Connection.get_conn()
      Adbc.Connection.query!(conn, "CREATE TABLE __dux_insert_test_existing (x BIGINT)")
      Adbc.Connection.query!(conn, "INSERT INTO __dux_insert_test_existing VALUES (0)")

      Dux.from_list([%{"x" => 1}, %{"x" => 2}])
      |> Dux.insert_into("__dux_insert_test_existing")

      result =
        Dux.from_query("SELECT * FROM __dux_insert_test_existing ORDER BY x")
        |> Dux.to_columns()

      assert result == %{"x" => [0, 1, 2]}
    after
      conn = Dux.Connection.get_conn()
      Adbc.Connection.query(conn, "DROP TABLE IF EXISTS __dux_insert_test_existing")
    end

    test "insert_into with attached DuckDB database" do
      path = tmp_path("insert_attached.duckdb")

      try do
        Dux.attach(:insert_test_db, path, type: :duckdb, read_only: false)

        Dux.from_list([%{"a" => 10, "b" => "hello"}])
        |> Dux.insert_into("insert_test_db.main.insert_target", create: true)

        result = Dux.from_attached(:insert_test_db, "insert_target") |> Dux.to_rows()
        assert result == [%{"a" => 10, "b" => "hello"}]
      after
        Dux.detach(:insert_test_db)
        File.rm(path)
      end
    end
  end
end
