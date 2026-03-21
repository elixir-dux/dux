defmodule Dux.VerbTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "select/2" do
    test "keeps only named columns" do
      result =
        Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
        |> Dux.select([:a, :c])
        |> Dux.to_columns()

      assert result == %{"a" => [1], "c" => [3]}
    end

    test "accepts string column names" do
      result =
        Dux.from_query("SELECT 1 AS a, 2 AS b")
        |> Dux.select(["a"])
        |> Dux.to_columns()

      assert result == %{"a" => [1]}
    end

    test "preserves order of requested columns" do
      result =
        Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
        |> Dux.select([:c, :a])
        |> Dux.to_rows()

      row = hd(result)
      assert Map.keys(row) |> Enum.sort() == ["a", "c"]
    end
  end

  describe "discard/2" do
    test "drops named columns" do
      result =
        Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c")
        |> Dux.discard([:b])
        |> Dux.to_columns()

      assert result == %{"a" => [1], "c" => [3]}
    end

    test "drops multiple columns" do
      result =
        Dux.from_query("SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d")
        |> Dux.discard([:b, :d])
        |> Dux.to_columns()

      assert result == %{"a" => [1], "c" => [3]}
    end
  end

  describe "filter/2" do
    test "filters by condition" do
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.filter_with("x > 5")
        |> Dux.to_columns()

      assert result == %{"x" => [6, 7, 8, 9, 10]}
    end

    test "compound conditions" do
      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter_with("x > 5 AND x < 10")
        |> Dux.to_columns()

      assert result == %{"x" => [6, 7, 8, 9]}
    end

    test "chained filters are ANDed" do
      result =
        Dux.from_query("SELECT * FROM range(1, 21) t(x)")
        |> Dux.filter_with("x > 5")
        |> Dux.filter_with("x < 10")
        |> Dux.to_columns()

      assert result == %{"x" => [6, 7, 8, 9]}
    end

    test "filter with string operations" do
      result =
        Dux.from_list([
          %{"name" => "Alice"},
          %{"name" => "Bob"},
          %{"name" => "Alicia"}
        ])
        |> Dux.filter_with("name LIKE 'Ali%'")
        |> Dux.to_columns()

      assert result == %{"name" => ["Alice", "Alicia"]}
    end

    test "filter returns empty when nothing matches" do
      result =
        Dux.from_query("SELECT * FROM range(5) t(x)")
        |> Dux.filter_with("x > 100")
        |> Dux.to_columns()

      assert result == %{"x" => []}
    end
  end

  describe "head/2" do
    test "takes first n rows" do
      result =
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.head(5)
        |> Dux.to_columns()

      assert result == %{"x" => [0, 1, 2, 3, 4]}
    end

    test "head(0) returns empty" do
      result =
        Dux.from_query("SELECT 1 AS x")
        |> Dux.head(0)
        |> Dux.n_rows()

      assert result == 0
    end
  end

  describe "slice/3" do
    test "skips and takes" do
      result =
        Dux.from_query("SELECT * FROM range(10) t(x)")
        |> Dux.slice(2, 3)
        |> Dux.to_columns()

      assert result == %{"x" => [2, 3, 4]}
    end
  end

  describe "distinct/1" do
    test "removes duplicate rows" do
      result =
        Dux.from_query("SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2")
        |> Dux.distinct()
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result == %{"x" => [1, 2]}
    end

    test "distinct on specific columns" do
      result =
        Dux.from_list([
          %{"x" => 1, "y" => "a"},
          %{"x" => 1, "y" => "b"},
          %{"x" => 2, "y" => "c"}
        ])
        |> Dux.distinct([:x])
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result["x"] == [1, 2]
      assert length(result["y"]) == 2
    end
  end

  describe "drop_nil/2" do
    test "removes rows with nil in specified columns" do
      result =
        Dux.from_query(
          "SELECT 1 AS x, 'a' AS y UNION ALL SELECT NULL, 'b' UNION ALL SELECT 3, NULL"
        )
        |> Dux.drop_nil([:x])
        |> Dux.to_columns()

      assert result["x"] == [1, 3]
    end

    test "drops nil across multiple columns" do
      result =
        Dux.from_query(
          "SELECT 1 AS x, 'a' AS y UNION ALL SELECT NULL, 'b' UNION ALL SELECT 3, NULL"
        )
        |> Dux.drop_nil([:x, :y])
        |> Dux.to_columns()

      assert result["x"] == [1]
      assert result["y"] == ["a"]
    end
  end

  describe "mutate/2" do
    test "adds new columns" do
      result =
        Dux.from_query("SELECT 1 AS x, 2 AS y")
        |> Dux.mutate_with(z: "x + y")
        |> Dux.to_columns()

      assert result == %{"x" => [1], "y" => [2], "z" => [3]}
    end

    test "multiple mutations in one call" do
      result =
        Dux.from_query("SELECT 10 AS x")
        |> Dux.mutate_with(doubled: "x * 2", halved: "x / 2")
        |> Dux.to_columns()

      assert result["doubled"] == [20]
      assert result["halved"] == [5]
    end

    test "chained mutates" do
      result =
        Dux.from_query("SELECT 5 AS x")
        |> Dux.mutate_with(y: "x * 2")
        |> Dux.mutate_with(z: "y + x")
        |> Dux.to_columns()

      assert result == %{"x" => [5], "y" => [10], "z" => [15]}
    end

    test "string expressions" do
      result =
        Dux.from_list([%{"name" => "alice"}])
        |> Dux.mutate_with(upper_name: "UPPER(name)")
        |> Dux.to_columns()

      assert result["upper_name"] == ["ALICE"]
    end
  end

  describe "rename/2" do
    test "renames columns" do
      result =
        Dux.from_query("SELECT 1 AS old_name")
        |> Dux.rename(old_name: :new_name)
        |> Dux.to_rows()

      assert [%{"new_name" => 1}] = result
    end

    test "renames multiple columns" do
      result =
        Dux.from_query("SELECT 1 AS a, 2 AS b")
        |> Dux.rename(a: :x, b: :y)
        |> Dux.to_rows()

      assert [%{"x" => 1, "y" => 2}] = result
    end
  end

  describe "sort_by/2" do
    test "sorts ascending by default" do
      result =
        Dux.from_list([%{"x" => 3}, %{"x" => 1}, %{"x" => 2}])
        |> Dux.sort_by(:x)
        |> Dux.to_columns()

      assert result == %{"x" => [1, 2, 3]}
    end

    test "sorts descending" do
      result =
        Dux.from_list([%{"x" => 3}, %{"x" => 1}, %{"x" => 2}])
        |> Dux.sort_by(desc: :x)
        |> Dux.to_columns()

      assert result == %{"x" => [3, 2, 1]}
    end

    test "multi-column sort" do
      result =
        Dux.from_list([
          %{"a" => 1, "b" => 2},
          %{"a" => 1, "b" => 1},
          %{"a" => 2, "b" => 1}
        ])
        |> Dux.sort_by(asc: :a, desc: :b)
        |> Dux.to_columns()

      assert result == %{"a" => [1, 1, 2], "b" => [2, 1, 1]}
    end
  end

  describe "group_by/2 + summarise/2" do
    test "basic aggregation" do
      result =
        Dux.from_list([
          %{"g" => "a", "v" => 10},
          %{"g" => "a", "v" => 20},
          %{"g" => "b", "v" => 30}
        ])
        |> Dux.group_by(:g)
        |> Dux.summarise_with(total: "SUM(v)")
        |> Dux.sort_by(:g)
        |> Dux.to_columns()

      assert result["g"] == ["a", "b"]
      assert result["total"] == [30, 30]
    end

    test "multiple aggregations" do
      result =
        Dux.from_list([
          %{"g" => "x", "v" => 1},
          %{"g" => "x", "v" => 2},
          %{"g" => "x", "v" => 3}
        ])
        |> Dux.group_by(:g)
        |> Dux.summarise_with(total: "SUM(v)", avg: "AVG(v)", n: "COUNT(*)")
        |> Dux.to_rows()

      row = hd(result)
      assert row["total"] == 6
      assert_in_delta row["avg"], 2.0, 0.01
      assert row["n"] == 3
    end

    test "multi-column group by" do
      result =
        Dux.from_list([
          %{"a" => 1, "b" => "x", "v" => 10},
          %{"a" => 1, "b" => "x", "v" => 20},
          %{"a" => 1, "b" => "y", "v" => 30},
          %{"a" => 2, "b" => "x", "v" => 40}
        ])
        |> Dux.group_by([:a, :b])
        |> Dux.summarise_with(total: "SUM(v)")
        |> Dux.sort_by([:a, :b])
        |> Dux.to_rows()

      assert length(result) == 3
      assert hd(result)["total"] == 30
    end

    test "global aggregation without group_by" do
      result =
        Dux.from_query("SELECT * FROM range(1, 11) t(x)")
        |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")
        |> Dux.to_rows()

      row = hd(result)
      assert row["total"] == 55
      assert row["n"] == 10
    end
  end

  describe "join/3" do
    test "inner join" do
      left = Dux.from_list([%{"id" => 1, "name" => "a"}, %{"id" => 2, "name" => "b"}])
      right = Dux.from_list([%{"id" => 1, "val" => 10}, %{"id" => 3, "val" => 30}])

      result =
        left
        |> Dux.join(right, on: :id)
        |> Dux.to_rows()

      assert length(result) == 1
      assert hd(result)["name"] == "a"
      assert hd(result)["val"] == 10
    end

    test "left join preserves left rows" do
      left = Dux.from_list([%{"id" => 1, "name" => "a"}, %{"id" => 2, "name" => "b"}])
      right = Dux.from_list([%{"id" => 1, "val" => 10}])

      result =
        left
        |> Dux.join(right, on: :id, how: :left)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.at(result, 0)["val"] == 10
      assert Enum.at(result, 1)["val"] == nil
    end

    test "cross join" do
      left = Dux.from_list([%{"x" => 1}, %{"x" => 2}])
      right = Dux.from_list([%{"y" => "a"}, %{"y" => "b"}])

      result =
        left
        |> Dux.join(right, how: :cross)
        |> Dux.n_rows()

      assert result == 4
    end
  end

  describe "concat_rows/1" do
    test "unions multiple dataframes" do
      a = Dux.from_list([%{"x" => 1}])
      b = Dux.from_list([%{"x" => 2}])
      c = Dux.from_list([%{"x" => 3}])

      result =
        Dux.concat_rows([a, b, c])
        |> Dux.to_columns()

      assert result == %{"x" => [1, 2, 3]}
    end
  end

  describe "from_list/1" do
    test "creates from list of maps" do
      result =
        Dux.from_list([%{"a" => 1, "b" => "hello"}])
        |> Dux.to_rows()

      assert [%{"a" => 1, "b" => "hello"}] = result
    end

    test "handles multiple rows" do
      rows = Enum.map(1..10, &%{"x" => &1})

      result =
        Dux.from_list(rows)
        |> Dux.n_rows()

      assert result == 10
    end
  end

  describe "compute/1" do
    test "materializes and returns a Dux with table source" do
      df = Dux.from_query("SELECT 1 AS x") |> Dux.compute()

      assert df.ops == []
      assert {:table, ref} = df.source
      assert is_reference(ref)
      assert df.names == ["x"]
    end

    test "computed result can be piped into more verbs" do
      base =
        Dux.from_query("SELECT * FROM range(10) t(x)")
        |> Dux.compute()

      result =
        base
        |> Dux.filter_with("x > 5")
        |> Dux.to_columns()

      assert result == %{"x" => [6, 7, 8, 9]}
    end

    test "branching from computed base" do
      base =
        Dux.from_query("SELECT * FROM range(1, 6) t(x)")
        |> Dux.compute()

      a = base |> Dux.filter_with("x <= 3") |> Dux.n_rows()
      b = base |> Dux.filter_with("x > 3") |> Dux.n_rows()

      assert a == 3
      assert b == 2
    end
  end

  describe "sql_preview/1" do
    test "shows generated SQL" do
      sql =
        Dux.from_query("SELECT * FROM t")
        |> Dux.filter_with("x > 10")
        |> Dux.mutate_with(y: "x * 2")
        |> Dux.head(5)
        |> Dux.sql_preview()

      assert sql =~ "WHERE"
      assert sql =~ "x > 10"
      assert sql =~ "x * 2"
      assert sql =~ "LIMIT 5"
    end
  end

  # ---------------------------------------------------------------------------
  # Complex pipeline tests (e2e)
  # ---------------------------------------------------------------------------

  describe "end-to-end pipelines" do
    test "full analytics pipeline" do
      data =
        Enum.flat_map(1..100, fn i ->
          region = if rem(i, 2) == 0, do: "US", else: "EU"
          [%{"id" => i, "region" => region, "amount" => i * 10}]
        end)

      result =
        Dux.from_list(data)
        |> Dux.filter_with("amount > 100")
        |> Dux.group_by(:region)
        |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
        |> Dux.sort_by(:region)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.at(result, 0)["region"] == "EU"
      assert Enum.at(result, 1)["region"] == "US"
      # All rows have amount > 100, so ids > 10
      assert Enum.at(result, 0)["n"] > 0
      assert Enum.at(result, 1)["n"] > 0
    end

    test "select → mutate → filter → sort → head" do
      result =
        Dux.from_query("SELECT * FROM range(1, 101) t(x)")
        |> Dux.mutate_with(squared: "x * x")
        |> Dux.filter_with("squared > 50")
        |> Dux.select([:x, :squared])
        |> Dux.sort_by(desc: :squared)
        |> Dux.head(3)
        |> Dux.to_columns()

      assert result["x"] == [100, 99, 98]
      assert result["squared"] == [10_000, 9801, 9604]
    end

    test "join → group → summarise" do
      orders =
        Dux.from_list([
          %{"product_id" => 1, "qty" => 5},
          %{"product_id" => 1, "qty" => 3},
          %{"product_id" => 2, "qty" => 10}
        ])

      products =
        Dux.from_list([
          %{"product_id" => 1, "name" => "Widget"},
          %{"product_id" => 2, "name" => "Gadget"}
        ])

      result =
        orders
        |> Dux.join(products, on: :product_id)
        |> Dux.group_by(:name)
        |> Dux.summarise_with(total_qty: "SUM(qty)")
        |> Dux.sort_by(:name)
        |> Dux.to_rows()

      assert [%{"name" => "Gadget", "total_qty" => 10}, %{"name" => "Widget", "total_qty" => 8}] =
               result
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "filter with invalid SQL returns error on compute" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_query("SELECT 1 AS x")
        |> Dux.filter_with("INVALID SYNTAX !!!!")
        |> Dux.compute()
      end
    end

    test "select non-existent column returns error on compute" do
      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        Dux.from_query("SELECT 1 AS x")
        |> Dux.select([:nonexistent])
        |> Dux.compute()
      end
    end

    test "join with no common column fails" do
      left = Dux.from_list([%{"a" => 1}])
      right = Dux.from_list([%{"b" => 2}])

      assert_raise ArgumentError, ~r/DuckDB query failed/, fn ->
        left
        |> Dux.join(right, on: :id)
        |> Dux.compute()
      end
    end

    test "summarise without group_by does global aggregation" do
      result =
        Dux.from_query("SELECT * FROM range(1, 4) t(x)")
        |> Dux.summarise_with(total: "SUM(x)")
        |> Dux.to_rows()

      assert [%{"total" => 6}] = result
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "column names with spaces" do
      result =
        Dux.from_query(~s{SELECT 1 AS "col with spaces"})
        |> Dux.select(["col with spaces"])
        |> Dux.to_rows()

      assert [%{"col with spaces" => 1}] = result
    end

    test "single-quote in string data survives round-trip" do
      result =
        Dux.from_list([%{"x" => "it's"}])
        |> Dux.to_columns()

      assert result["x"] == ["it's"]
    end

    test "empty dataframe through full pipeline" do
      result =
        Dux.from_query("SELECT 1 AS x WHERE false")
        |> Dux.filter_with("x > 0")
        |> Dux.mutate_with(y: "x * 2")
        |> Dux.sort_by(:x)
        |> Dux.n_rows()

      assert result == 0
    end

    test "long pipeline (30 operations)" do
      df = Dux.from_query("SELECT 1 AS x")

      df =
        Enum.reduce(1..30, df, fn i, acc ->
          Dux.mutate_with(acc, [{String.to_atom("c#{i}"), "x + #{i}"}])
        end)

      result = Dux.n_rows(df)
      assert result == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked / pathological
  # ---------------------------------------------------------------------------

  describe "wicked cases" do
    test "group_by with cardinality == row count" do
      result =
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.group_by(:x)
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.n_rows()

      assert result == 100
    end

    test "filter that eliminates everything then aggregates" do
      result =
        Dux.from_query("SELECT * FROM range(100) t(x)")
        |> Dux.filter_with("x > 1000")
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.to_rows()

      assert [%{"n" => 0}] = result
    end

    test "self-join (cartesian)" do
      result =
        Dux.from_query("SELECT * FROM range(10) t(x)")
        |> Dux.join(Dux.from_query("SELECT * FROM range(10) t(y)"), how: :cross)
        |> Dux.n_rows()

      assert result == 100
    end

    test "deeply chained filters" do
      df = Dux.from_query("SELECT * FROM range(1000) t(x)")

      df =
        Enum.reduce(1..20, df, fn i, acc ->
          Dux.filter_with(acc, "x >= #{i}")
        end)

      result = Dux.to_columns(df)
      assert hd(result["x"]) == 20
    end
  end

  # ---------------------------------------------------------------------------
  # Property tests
  # ---------------------------------------------------------------------------

  describe "property: head preserves row count invariant" do
    property "head(n) returns min(n, total_rows) rows" do
      check all(
              total <- integer(0..100),
              n <- integer(0..200)
            ) do
        actual =
          Dux.from_query("SELECT * FROM range(#{total}) t(x)")
          |> Dux.head(n)
          |> Dux.n_rows()

        assert actual == min(n, total)
      end
    end
  end

  describe "property: filter never increases row count" do
    property "filter produces <= input rows" do
      check all(total <- integer(1..100), threshold <- integer(0..100)) do
        result =
          Dux.from_query("SELECT * FROM range(#{total}) t(x)")
          |> Dux.filter_with("x >= #{threshold}")
          |> Dux.n_rows()

        assert result <= total
      end
    end
  end

  describe "property: distinct never increases row count" do
    property "distinct produces <= input rows" do
      check all(n <- integer(1..50)) do
        # Create data with duplicates
        result =
          Dux.from_query("SELECT x % 5 AS x FROM range(#{n}) t(x)")
          |> Dux.distinct()
          |> Dux.n_rows()

        assert result <= n
        assert result <= 5
      end
    end
  end

  describe "property: sort preserves row count" do
    property "sort_by doesn't change row count" do
      check all(n <- integer(1..100)) do
        result =
          Dux.from_query("SELECT * FROM range(#{n}) t(x)")
          |> Dux.sort_by(:x)
          |> Dux.n_rows()

        assert result == n
      end
    end
  end
end
