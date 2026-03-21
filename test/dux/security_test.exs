defmodule Dux.SecurityTest do
  use ExUnit.Case, async: false
  require Dux

  @moduledoc """
  SQL injection and security tests.
  Validates that user-controlled inputs cannot break out of SQL contexts.
  """

  # ---------------------------------------------------------------------------
  # Pin interpolation (already safe — parameter bindings)
  # ---------------------------------------------------------------------------

  describe "pin interpolation prevents SQL injection" do
    test "malicious string via ^pin is treated as literal value" do
      malicious = "'; DROP TABLE users; --"

      result =
        Dux.from_list([%{"name" => "safe"}, %{"name" => "'; DROP TABLE users; --"}])
        |> Dux.filter(name == ^malicious)
        |> Dux.to_columns()

      assert result["name"] == ["'; DROP TABLE users; --"]
    end

    test "SQL keyword in pin value is safe" do
      val = "OR 1=1"

      result =
        Dux.from_list([%{"x" => "hello"}, %{"x" => "OR 1=1"}])
        |> Dux.filter(x == ^val)
        |> Dux.to_columns()

      assert result["x"] == ["OR 1=1"]
    end

    test "NULL injection via pin" do
      val = "NULL"

      result =
        Dux.from_list([%{"x" => "NULL"}, %{"x" => "real"}])
        |> Dux.filter(x == ^val)
        |> Dux.to_columns()

      assert result["x"] == ["NULL"]
    end
  end

  # ---------------------------------------------------------------------------
  # Column name injection (fixed via quote_ident escaping)
  # ---------------------------------------------------------------------------

  describe "column name identifier escaping" do
    test "column name with double quotes is escaped" do
      # DuckDB should handle this as a literal column name, not injection
      data = [%{~s(col"name) => 1}]

      result =
        Dux.from_list(data)
        |> Dux.to_columns()

      assert Map.has_key?(result, ~s(col"name))
      assert result[~s(col"name)] == [1]
    end

    test "column name with SQL injection attempt" do
      # The column name should be treated as a literal identifier
      data = [%{~s(id" OR "1"="1) => 42}]

      result =
        Dux.from_list(data)
        |> Dux.to_columns()

      assert result[~s(id" OR "1"="1)] == [42]
    end

    test "select with special character column names" do
      data = [%{"normal" => 1, ~s(tricky") => 2}]

      result =
        Dux.from_list(data)
        |> Dux.select([~s(tricky")])
        |> Dux.to_columns()

      assert result[~s(tricky")] == [2]
    end

    test "col() with dynamic injection attempt" do
      col_name = ~s(id" OR "1"="1)

      # Should be treated as a literal column name, causing a "not found" error
      # rather than SQL injection
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_list([%{"x" => 1}])
        |> Dux.filter(col(^col_name) > 0)
        |> Dux.compute()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # String value injection in from_list
  # ---------------------------------------------------------------------------

  describe "from_list string value escaping" do
    test "single quotes in string values" do
      result =
        Dux.from_list([%{"x" => "it's a test"}])
        |> Dux.to_columns()

      assert result["x"] == ["it's a test"]
    end

    test "backslash in string values" do
      result =
        Dux.from_list([%{"x" => "path\\to\\file"}])
        |> Dux.to_columns()

      assert result["x"] == ["path\\to\\file"]
    end

    test "SQL keywords as string values" do
      result =
        Dux.from_list([%{"x" => "DROP TABLE; --"}])
        |> Dux.to_columns()

      assert result["x"] == ["DROP TABLE; --"]
    end
  end

  # ---------------------------------------------------------------------------
  # filter_with raw SQL (intentionally unsafe — documented)
  # ---------------------------------------------------------------------------

  describe "filter_with raw SQL is intentionally pass-through" do
    test "raw SQL is executed as-is (documented behavior)" do
      # This is by design — filter_with accepts raw SQL
      # Users must sanitize their own input if using filter_with with untrusted data
      result =
        Dux.from_query("SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3")
        |> Dux.filter_with("x > 1")
        |> Dux.to_columns()

      assert result["x"] == [2, 3]
    end
  end

  # ---------------------------------------------------------------------------
  # Graph with unusual column names
  # ---------------------------------------------------------------------------

  describe "graph column name safety" do
    test "graph with special character column names" do
      vertices = Dux.from_list([%{"v id" => 1}, %{"v id" => 2}, %{"v id" => 3}])

      edges =
        Dux.from_list([
          %{"from node" => 1, "to node" => 2},
          %{"from node" => 2, "to node" => 3}
        ])

      graph =
        Dux.Graph.new(
          vertices: vertices,
          edges: edges,
          vertex_id: "v id",
          edge_src: "from node",
          edge_dst: "to node"
        )

      assert Dux.Graph.vertex_count(graph) == 3
      assert Dux.Graph.edge_count(graph) == 2

      result =
        Dux.Graph.out_degree(graph)
        |> Dux.sort_by(:"v id")
        |> Dux.to_columns()

      assert result["v id"] == [1, 2]
      assert result["out_degree"] == [1, 1]
    end
  end

  # ---------------------------------------------------------------------------
  # Mutate/Summarise with injection attempts
  # ---------------------------------------------------------------------------

  describe "mutate with pin injection" do
    test "malicious pin value in mutate is safe" do
      bad_val = "1; DROP TABLE users; --"

      result =
        Dux.from_list([%{"x" => 1}])
        |> Dux.mutate(y: ^bad_val)
        |> Dux.to_rows()

      row = hd(result)
      assert row["y"] == "1; DROP TABLE users; --"
    end
  end
end
