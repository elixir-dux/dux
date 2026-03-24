# credo:disable-for-this-file Credo.Check.Refactor.CondStatements
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

  # ---------------------------------------------------------------------------
  # CASE WHEN (cond/if)
  # ---------------------------------------------------------------------------

  describe "cond → CASE WHEN" do
    test "basic cond with else" do
      result =
        Dux.from_list([%{"x" => 10}, %{"x" => 200}, %{"x" => 1500}])
        |> Dux.mutate(
          tier:
            cond do
              x > 1000 -> "gold"
              x > 100 -> "silver"
              true -> "bronze"
            end
        )
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result["tier"] == ["bronze", "silver", "gold"]
    end

    test "cond without else (no true branch)" do
      result =
        Dux.from_list([%{"x" => 5}, %{"x" => 15}])
        |> Dux.mutate(
          label:
            cond do
              x > 10 -> "big"
            end
        )
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      # No else → NULL for non-matching rows
      assert result["label"] == [nil, "big"]
    end

    test "cond with pins" do
      threshold = 100

      result =
        Dux.from_list([%{"x" => 50}, %{"x" => 150}])
        |> Dux.mutate(
          over:
            cond do
              x > ^threshold -> "yes"
              true -> "no"
            end
        )
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result["over"] == ["no", "yes"]
    end

    test "cond in filter" do
      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}])
        |> Dux.filter(
          cond do
            x > 2 -> true
            true -> false
          end
        )
        |> Dux.to_columns()

      assert result["x"] == [3]
    end

    test "nested cond with arithmetic" do
      result =
        Dux.from_list([%{"amount" => 50}, %{"amount" => 500}, %{"amount" => 5000}])
        |> Dux.mutate(
          discount:
            cond do
              amount > 1000 -> amount * 0.2
              amount > 100 -> amount * 0.1
              true -> 0
            end
        )
        |> Dux.sort_by(:amount)
        |> Dux.to_rows()

      assert Enum.map(result, & &1["discount"]) == [0, 50.0, 1000.0]
    end
  end

  describe "if/else → CASE WHEN" do
    test "basic if/else" do
      result =
        Dux.from_list([%{"x" => -5}, %{"x" => 10}])
        |> Dux.mutate(sign: if(x > 0, do: "positive", else: "non-positive"))
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result["sign"] == ["non-positive", "positive"]
    end

    test "if without else (NULL)" do
      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 5}])
        |> Dux.mutate(big: if(x > 3, do: "yes"))
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result["big"] == [nil, "yes"]
    end
  end

  # ---------------------------------------------------------------------------
  # IN operator
  # ---------------------------------------------------------------------------

  describe "in operator" do
    test "literal list" do
      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}])
        |> Dux.filter(x in [2, 4])
        |> Dux.to_columns()

      assert result["x"] == [2, 4]
    end

    test "pinned list" do
      allowed = [1, 3]

      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}])
        |> Dux.filter(x in ^allowed)
        |> Dux.to_columns()

      assert result["x"] == [1, 3]
    end

    test "string values" do
      result =
        Dux.from_list([
          %{"status" => "active"},
          %{"status" => "pending"},
          %{"status" => "deleted"}
        ])
        |> Dux.filter(status in ["active", "pending"])
        |> Dux.sort_by(:status)
        |> Dux.to_columns()

      assert result["status"] == ["active", "pending"]
    end

    test "pinned string list" do
      allowed = ["active", "pending"]

      result =
        Dux.from_list([
          %{"status" => "active"},
          %{"status" => "pending"},
          %{"status" => "deleted"}
        ])
        |> Dux.filter(status in ^allowed)
        |> Dux.sort_by(:status)
        |> Dux.to_columns()

      assert result["status"] == ["active", "pending"]
    end

    test "single value" do
      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 2}])
        |> Dux.filter(x in [1])
        |> Dux.to_columns()

      assert result["x"] == [1]
    end
  end

  # ---------------------------------------------------------------------------
  # CASE WHEN / IN — adversarial
  # ---------------------------------------------------------------------------

  describe "cond/in adversarial" do
    test "cond with SQL-injection-like strings" do
      result =
        Dux.from_list([%{"x" => 1}])
        |> Dux.mutate(
          label:
            cond do
              x > 0 -> "it's a 'test'"
              true -> "other"
            end
        )
        |> Dux.to_rows()

      assert hd(result)["label"] == "it's a 'test'"
    end

    test "cond with many branches" do
      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}, %{"x" => 5}])
        |> Dux.mutate(
          label:
            cond do
              x == 1 -> "one"
              x == 2 -> "two"
              x == 3 -> "three"
              x == 4 -> "four"
              true -> "other"
            end
        )
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result["label"] == ["one", "two", "three", "four", "other"]
    end

    test "in with empty result" do
      result =
        Dux.from_list([%{"x" => 1}, %{"x" => 2}])
        |> Dux.filter(x in [99, 100])
        |> Dux.to_columns()

      assert result["x"] == []
    end

    test "in combined with cond" do
      result =
        Dux.from_list([
          %{"status" => "active", "amount" => 50},
          %{"status" => "pending", "amount" => 200},
          %{"status" => "deleted", "amount" => 100}
        ])
        |> Dux.filter(status in ["active", "pending"])
        |> Dux.mutate(
          tier:
            cond do
              amount > 100 -> "high"
              true -> "low"
            end
        )
        |> Dux.sort_by(:amount)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.map(result, & &1["tier"]) == ["low", "high"]
    end

    test "cond with null values" do
      result =
        Dux.from_query("SELECT NULL AS x UNION ALL SELECT 5 AS x")
        |> Dux.mutate(
          label:
            cond do
              x > 3 -> "big"
              true -> "small"
            end
        )
        |> Dux.sort_by(:label)
        |> Dux.to_columns()

      # NULL > 3 is NULL (falsy), so falls to else
      assert result["label"] == ["big", "small"]
    end

    test "pinned in with special characters in strings" do
      targets = ["it's", "a \"test\""]

      result =
        Dux.from_list([
          %{"name" => "it's"},
          %{"name" => "a \"test\""},
          %{"name" => "other"}
        ])
        |> Dux.filter(name in ^targets)
        |> Dux.sort_by(:name)
        |> Dux.to_columns()

      assert length(result["name"]) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # DuckDB function pass-through
  # ---------------------------------------------------------------------------

  describe "DuckDB function pass-through" do
    test "date functions" do
      result =
        Dux.from_query("SELECT DATE '2024-06-15' AS d")
        |> Dux.mutate(y: year(d), m: month(d), day_of: day(d))
        |> Dux.to_rows()

      row = hd(result)
      assert row["y"] == 2024
      assert row["m"] == 6
      assert row["day_of"] == 15
    end

    test "string functions" do
      result =
        Dux.from_list([%{"name" => "Hello World"}])
        |> Dux.mutate(low: lower(name), up: upper(name), len: length(name))
        |> Dux.to_rows()

      row = hd(result)
      assert row["low"] == "hello world"
      assert row["up"] == "HELLO WORLD"
      assert row["len"] == 11
    end

    test "math functions" do
      result =
        Dux.from_list([%{"x" => 100}])
        |> Dux.mutate(sq: sqrt(x), lg: log2(x), ab: abs(-1 * x))
        |> Dux.to_rows()

      row = hd(result)
      assert row["sq"] == 10.0
      assert_in_delta row["lg"], 6.644, 0.01
      assert row["ab"] == 100
    end

    test "coalesce" do
      result =
        Dux.from_query("SELECT NULL AS x, 42 AS y")
        |> Dux.mutate(z: coalesce(x, y))
        |> Dux.to_rows()

      assert hd(result)["z"] == 42
    end

    test "regexp_matches" do
      result =
        Dux.from_list([%{"email" => "a@gmail.com"}, %{"email" => "b@yahoo.com"}])
        |> Dux.filter(regexp_matches(email, ".*@gmail\\.com"))
        |> Dux.to_columns()

      assert result["email"] == ["a@gmail.com"]
    end
  end
end
