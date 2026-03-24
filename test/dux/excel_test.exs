defmodule Dux.ExcelTest do
  use ExUnit.Case, async: false

  @tmp_dir System.tmp_dir!()

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_excel_#{System.unique_integer([:positive])}_#{name}")
  end

  # Helper to create a test xlsx file via DuckDB
  defp write_xlsx(path, sql) do
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, "INSTALL excel; LOAD excel;")
    escaped = String.replace(path, "'", "''")
    Adbc.Connection.query!(conn, "COPY (#{sql}) TO '#{escaped}' (FORMAT XLSX, HEADER true)")
  end

  # ---------------------------------------------------------------------------
  # Happy path — reading
  # ---------------------------------------------------------------------------

  describe "from_excel" do
    test "reads basic xlsx file" do
      path = tmp_path("basic.xlsx")

      try do
        write_xlsx(path, "SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'")

        result =
          Dux.from_excel(path)
          |> Dux.sort_by(:id)
          |> Dux.to_rows()

        assert length(result) == 2
        assert hd(result)["name"] == "Alice"
      after
        File.rm(path)
      end
    end

    test "reads with all_varchar option" do
      path = tmp_path("varchar.xlsx")

      try do
        write_xlsx(path, "SELECT 42 AS val, 'text' AS label")

        result =
          Dux.from_excel(path, all_varchar: true)
          |> Dux.to_rows()

        row = hd(result)
        # With all_varchar, numeric values come back as strings
        assert is_binary(row["val"])
      after
        File.rm(path)
      end
    end

    test "pipelines normally after read" do
      require Dux
      path = tmp_path("pipeline.xlsx")

      try do
        write_xlsx(
          path,
          "SELECT * FROM (VALUES (1, 'US'), (2, 'EU'), (3, 'US'), (4, 'APAC'), (5, 'EU')) t(id, region)"
        )

        result =
          Dux.from_excel(path)
          |> Dux.filter(region == "US")
          |> Dux.to_columns()

        assert result["id"] == [1, 3]
      after
        File.rm(path)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path — writing
  # ---------------------------------------------------------------------------

  describe "to_excel" do
    test "writes basic xlsx file" do
      path = tmp_path("write_basic.xlsx")

      try do
        Dux.from_list([%{"x" => 1, "y" => "hello"}, %{"x" => 2, "y" => "world"}])
        |> Dux.to_excel(path)

        assert File.exists?(path)
        assert File.stat!(path).size > 0
      after
        File.rm(path)
      end
    end

    test "round-trip: write then read" do
      path = tmp_path("roundtrip.xlsx")

      try do
        Dux.from_list([
          %{"id" => 1, "name" => "Alice", "score" => 95.5},
          %{"id" => 2, "name" => "Bob", "score" => 87.0},
          %{"id" => 3, "name" => "Carol", "score" => 92.3}
        ])
        |> Dux.to_excel(path)

        result =
          Dux.from_excel(path)
          |> Dux.sort_by(:id)
          |> Dux.to_rows()

        assert length(result) == 3
        assert hd(result)["name"] == "Alice"
        assert_in_delta hd(result)["score"], 95.5, 0.1
      after
        File.rm(path)
      end
    end

    test "write with pipeline before" do
      require Dux
      path = tmp_path("pipeline_write.xlsx")

      try do
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.filter(x > 5)
        |> Dux.mutate(doubled: x * 2)
        |> Dux.to_excel(path)

        result =
          Dux.from_excel(path)
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
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "from_excel with non-existent file raises on compute" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_excel("/nonexistent/path/file.xlsx")
        |> Dux.compute()
      end
    end

    test "to_excel with invalid path raises" do
      assert_raise ArgumentError, ~r/DuckDB write failed/, fn ->
        Dux.from_list([%{"x" => 1}])
        |> Dux.to_excel("/nonexistent/dir/file.xlsx")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "round-trip preserves special characters" do
      path = tmp_path("special_chars.xlsx")

      try do
        Dux.from_list([
          %{"text" => "it's a test"},
          %{"text" => "has \"quotes\""},
          %{"text" => "line1\nline2"}
        ])
        |> Dux.to_excel(path)

        result = Dux.from_excel(path) |> Dux.to_columns()
        assert "it's a test" in result["text"]
      after
        File.rm(path)
      end
    end

    test "round-trip preserves nulls" do
      path = tmp_path("nulls.xlsx")

      try do
        # Put non-null string first so type inference gets VARCHAR, not DOUBLE
        Dux.from_query("SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, NULL")
        |> Dux.to_excel(path)

        result =
          Dux.from_excel(path)
          |> Dux.sort_by(:id)
          |> Dux.to_rows()

        assert length(result) == 2
        assert hd(result)["name"] == "Alice"
        # NULL may come back as nil or empty string depending on Excel handling
        second_name = Enum.at(result, 1)["name"]
        assert second_name == nil or second_name == ""
      after
        File.rm(path)
      end
    end

    test "empty dataframe writes valid xlsx" do
      path = tmp_path("empty.xlsx")

      try do
        Dux.from_query("SELECT 1 AS x WHERE false")
        |> Dux.to_excel(path)

        assert File.exists?(path)
      after
        File.rm(path)
      end
    end

    test "many columns" do
      path = tmp_path("wide.xlsx")

      try do
        # 50 columns
        cols = for i <- 1..50, into: %{}, do: {"col_#{i}", i}

        Dux.from_list([cols])
        |> Dux.to_excel(path)

        result = Dux.from_excel(path) |> Dux.to_rows()
        assert length(result) == 1
        assert hd(result)["col_1"] == 1
        assert hd(result)["col_50"] == 50
      after
        File.rm(path)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Scale
  # ---------------------------------------------------------------------------

  describe "scale" do
    test "round-trip 1000 rows" do
      path = tmp_path("scale.xlsx")

      try do
        Dux.from_query("SELECT x, x * 2 AS doubled FROM range(1, 1001) t(x)")
        |> Dux.to_excel(path)

        result =
          Dux.from_excel(path)
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(x)")
          |> Dux.to_rows()

        row = hd(result)
        assert row["n"] == 1000
        assert row["total"] == div(1000 * 1001, 2)
      after
        File.rm(path)
      end
    end
  end
end
