defmodule Dux.QueryTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Query.Compiler

  # ---------------------------------------------------------------------------
  # Happy path — compiler output
  # ---------------------------------------------------------------------------

  describe "Dux.Query compilation" do
    test "column reference" do
      {ast, []} = Dux.Query.traverse_public(quote(do: x), [])
      assert {"\"x\"", []} = Compiler.to_sql(ast, [])
    end

    test "literal integer" do
      {ast, []} = Dux.Query.traverse_public(quote(do: 42), [])
      assert {"42", []} = Compiler.to_sql(ast, [])
    end

    test "literal string" do
      {ast, []} = Dux.Query.traverse_public(quote(do: "hello"), [])
      assert {"'hello'", []} = Compiler.to_sql(ast, [])
    end

    test "literal boolean" do
      {ast_t, []} = Dux.Query.traverse_public(quote(do: true), [])
      {ast_f, []} = Dux.Query.traverse_public(quote(do: false), [])
      assert {"true", []} = Compiler.to_sql(ast_t, [])
      assert {"false", []} = Compiler.to_sql(ast_f, [])
    end

    test "comparison operators" do
      {ast, []} = Dux.Query.traverse_public(quote(do: x > 10), [])
      assert {"(\"x\" > 10)", []} = Compiler.to_sql(ast, [])
    end

    test "arithmetic operators" do
      {ast, []} = Dux.Query.traverse_public(quote(do: x + y), [])
      assert {"(\"x\" + \"y\")", []} = Compiler.to_sql(ast, [])
    end

    test "logical operators" do
      {ast, []} = Dux.Query.traverse_public(quote(do: x and y), [])
      assert {"(\"x\" AND \"y\")", []} = Compiler.to_sql(ast, [])
    end

    test "function calls" do
      {ast, []} = Dux.Query.traverse_public(quote(do: sum(x)), [])
      assert {"SUM(\"x\")", []} = Compiler.to_sql(ast, [])
    end

    test "count_distinct" do
      {ast, []} = Dux.Query.traverse_public(quote(do: count_distinct(x)), [])
      assert {"COUNT(DISTINCT \"x\")", []} = Compiler.to_sql(ast, [])
    end

    test "DuckDB functions pass through" do
      {ast, []} = Dux.Query.traverse_public(quote(do: upper(name)), [])
      assert {"UPPER(\"name\")", []} = Compiler.to_sql(ast, [])
    end

    test "col() for unusual column names" do
      {ast, []} = Dux.Query.traverse_public(quote(do: col("col with spaces")), [])
      assert {"\"col with spaces\"", []} = Compiler.to_sql(ast, [])
    end
  end

  # ---------------------------------------------------------------------------
  # Pin interpolation
  # ---------------------------------------------------------------------------

  describe "pin interpolation" do
    test "integer pin" do
      min_val = 10

      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter(x > ^min_val)
        |> Dux.to_columns()

      assert result == %{"x" => Enum.to_list(11..20)}
    end

    test "string pin" do
      target = "Alice"

      result =
        Dux.from_list([
          %{"name" => "Alice", "age" => 30},
          %{"name" => "Bob", "age" => 25}
        ])
        |> Dux.filter(name == ^target)
        |> Dux.to_columns()

      assert result["name"] == ["Alice"]
    end

    test "multiple pins" do
      lo = 5
      hi = 15

      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter(x > ^lo and x < ^hi)
        |> Dux.to_columns()

      assert result == %{"x" => Enum.to_list(6..14)}
    end

    test "pin in mutate" do
      factor = 5

      result =
        Dux.from_query("SELECT 10 AS x")
        |> Dux.mutate(scaled: x * ^factor)
        |> Dux.to_rows()

      assert [%{"scaled" => 50}] = result
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: macros with Dux verbs
  # ---------------------------------------------------------------------------

  describe "filter macro" do
    test "basic expression" do
      result =
        Dux.from_query("SELECT * FROM range(1, 6) t(x)")
        |> Dux.filter(x > 3)
        |> Dux.to_columns()

      assert result == %{"x" => [4, 5]}
    end

    test "compound expression" do
      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter(x > 5 and x < 10)
        |> Dux.to_columns()

      assert result == %{"x" => [6, 7, 8, 9]}
    end

    test "chained macro and string filters" do
      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter(x > 5)
        |> Dux.filter_with("x < 15")
        |> Dux.filter(x != 10)
        |> Dux.to_columns()

      expected = Enum.to_list(6..14) -- [10]
      assert result["x"] == expected
    end
  end

  describe "mutate macro" do
    test "basic expression" do
      result =
        Dux.from_query("SELECT 10 AS price, 5 AS qty")
        |> Dux.mutate(revenue: price * qty)
        |> Dux.to_rows()

      assert [%{"revenue" => 50}] = result
    end

    test "with function call" do
      result =
        Dux.from_list([%{"name" => "alice"}])
        |> Dux.mutate(upper_name: upper(name))
        |> Dux.to_columns()

      assert result["upper_name"] == ["ALICE"]
    end

    test "multiple expressions" do
      result =
        Dux.from_query("SELECT 1 AS x, 2 AS y")
        |> Dux.mutate(z: x + y, w: x * 10)
        |> Dux.to_rows()

      assert [%{"w" => 10, "x" => 1, "y" => 2, "z" => 3}] = result
    end
  end

  describe "summarise macro" do
    test "basic aggregation" do
      result =
        Dux.from_list([
          %{"g" => "a", "v" => 1},
          %{"g" => "a", "v" => 2},
          %{"g" => "b", "v" => 3}
        ])
        |> Dux.group_by(:g)
        |> Dux.summarise(total: sum(v))
        |> Dux.sort_by(:g)
        |> Dux.to_columns()

      assert result["g"] == ["a", "b"]
      assert result["total"] == [3, 3]
    end

    test "multiple aggregations" do
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise(total: sum(x), average: avg(x), n: count(x))
        |> Dux.to_rows()

      row = hd(result)
      assert row["total"] == 55
      assert_in_delta row["average"], 5.5, 0.01
      assert row["n"] == 10
    end

    test "global aggregation without group_by" do
      result =
        Dux.from_query("SELECT * FROM range(1, 4) t(x)")
        |> Dux.summarise(total: sum(x))
        |> Dux.to_rows()

      assert [%{"total" => 6}] = result
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "query with unknown column produces DuckDB error" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_query("SELECT 1 AS x")
        |> Dux.filter(nonexistent_column > 0)
        |> Dux.compute()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "SQL injection via pin is prevented" do
      malicious = "'; DROP TABLE users; --"

      result =
        Dux.from_list([%{"name" => "safe"}, %{"name" => "'; DROP TABLE users; --"}])
        |> Dux.filter(name == ^malicious)
        |> Dux.to_columns()

      assert result["name"] == ["'; DROP TABLE users; --"]
    end

    test "deeply nested expression compiles and runs" do
      result =
        Dux.from_query("SELECT 1 AS x")
        |> Dux.filter(x + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 > 5)
        |> Dux.n_rows()

      assert result == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "expression with mixed types" do
      tax = 0.08

      result =
        Dux.from_list([%{"price" => 100, "name" => "widget"}])
        |> Dux.mutate(with_tax: price * (1 + ^tax), label: upper(name))
        |> Dux.to_rows()

      row = hd(result)
      assert_in_delta row["with_tax"], 108.0, 0.01
      assert row["label"] == "WIDGET"
    end

    test "full pipeline with macros end-to-end" do
      min_amount = 100

      result =
        Dux.from_list([
          %{"region" => "US", "amount" => 50},
          %{"region" => "US", "amount" => 200},
          %{"region" => "EU", "amount" => 150},
          %{"region" => "EU", "amount" => 75}
        ])
        |> Dux.filter(amount >= ^min_amount)
        |> Dux.group_by(:region)
        |> Dux.summarise(total: sum(amount), n: count(amount))
        |> Dux.sort_by(:region)
        |> Dux.to_rows()

      assert [
               %{"region" => "EU", "total" => 150, "n" => 1},
               %{"region" => "US", "total" => 200, "n" => 1}
             ] = result
    end
  end
end
