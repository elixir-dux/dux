defmodule Dux.JSONTest do
  use ExUnit.Case, async: true

  # ---------------------------------------------------------------------------
  # extract
  # ---------------------------------------------------------------------------

  describe "Dux.JSON.extract/3" do
    test "extracts multiple fields from JSON column" do
      df = Dux.from_query(~s(SELECT '{"name": "Alice", "age": 30}' AS data))

      result =
        Dux.JSON.extract(df, :data, %{name: "$.name", age: "$.age"})
        |> Dux.to_rows()

      row = hd(result)
      assert row["name"] == "Alice"
      assert row["age"] == "30"
    end

    test "extracts nested fields" do
      df =
        Dux.from_query(~s(SELECT '{"user": {"name": "Bob", "email": "bob@ex.com"}}' AS payload))

      result =
        Dux.JSON.extract(df, :payload, %{
          user_name: "$.user.name",
          user_email: "$.user.email"
        })
        |> Dux.to_rows()

      row = hd(result)
      assert row["user_name"] == "Bob"
      assert row["user_email"] == "bob@ex.com"
    end

    test "missing JSON field returns nil" do
      df = Dux.from_query(~s(SELECT '{"name": "Alice"}' AS data))

      result =
        Dux.JSON.extract(df, :data, %{name: "$.name", missing: "$.nonexistent"})
        |> Dux.to_rows()

      row = hd(result)
      assert row["name"] == "Alice"
      assert row["missing"] == nil
    end

    test "extracts from multiple rows" do
      df =
        Dux.from_query("""
          SELECT '{"v": 1}' AS j
          UNION ALL SELECT '{"v": 2}'
          UNION ALL SELECT '{"v": 3}'
        """)

      result =
        Dux.JSON.extract(df, :j, %{v: "$.v"})
        |> Dux.to_rows()

      values = Enum.map(result, & &1["v"]) |> Enum.sort()
      assert values == ["1", "2", "3"]
    end

    test "works with string column name" do
      df = Dux.from_query(~s(SELECT '{"x": 42}' AS "my data"))

      result =
        Dux.JSON.extract(df, "my data", %{x: "$.x"})
        |> Dux.to_rows()

      assert hd(result)["x"] == "42"
    end
  end

  # ---------------------------------------------------------------------------
  # unnest
  # ---------------------------------------------------------------------------

  describe "Dux.JSON.unnest/3" do
    test "flattens JSON array to rows" do
      df = Dux.from_query(~s(SELECT 1 AS id, '[10, 20, 30]'::JSON AS vals))

      result =
        Dux.JSON.unnest(df, :vals)
        |> Dux.to_rows()

      assert length(result) == 3
      values = Enum.map(result, & &1["vals_value"]) |> Enum.sort()
      assert values == ["10", "20", "30"]
      assert Enum.all?(result, &(&1["id"] == 1))
    end

    test "unnest with custom output column name" do
      df = Dux.from_query(~s(SELECT '[1, 2]'::JSON AS arr))

      result =
        Dux.JSON.unnest(df, :arr, as: :element)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.all?(result, &Map.has_key?(&1, "element"))
    end

    test "unnest with path into nested array" do
      df = Dux.from_query(~s(SELECT '{"items": [1, 2, 3]}'::JSON AS data))

      result =
        Dux.JSON.unnest(df, :data, path: "$.items", as: :item)
        |> Dux.to_rows()

      assert length(result) == 3
      values = Enum.map(result, & &1["item"]) |> Enum.sort()
      assert values == ["1", "2", "3"]
    end

    test "unnest with multiple source rows" do
      df =
        Dux.from_query("""
          SELECT 'a' AS id, '[1, 2]'::JSON AS arr
          UNION ALL
          SELECT 'b', '[3, 4, 5]'::JSON
        """)

      result =
        Dux.JSON.unnest(df, :arr)
        |> Dux.to_rows()

      # 2 + 3 = 5 rows
      assert length(result) == 5
    end

    test "unnest empty array produces no rows for that source row" do
      df =
        Dux.from_query("""
          SELECT 'a' AS id, '[1]'::JSON AS arr
          UNION ALL
          SELECT 'b', '[]'::JSON
        """)

      result =
        Dux.JSON.unnest(df, :arr)
        |> Dux.to_rows()

      # Only 'a' row survives (empty array produces no rows)
      assert length(result) == 1
      assert hd(result)["id"] == "a"
    end
  end

  # ---------------------------------------------------------------------------
  # expand
  # ---------------------------------------------------------------------------

  describe "Dux.JSON.expand/2" do
    test "expands top-level JSON keys into columns" do
      df =
        Dux.from_query(~s(SELECT '{"x": 1, "y": 2, "z": 3}' AS data))
        |> Dux.compute()

      result = Dux.JSON.expand(df, :data) |> Dux.to_rows()

      row = hd(result)
      assert row["x"] == "1"
      assert row["y"] == "2"
      assert row["z"] == "3"
    end

    test "expand with null JSON returns no extra columns" do
      df =
        Dux.from_query("SELECT NULL::JSON AS data")
        |> Dux.compute()

      result = Dux.JSON.expand(df, :data) |> Dux.to_rows()
      row = hd(result)
      # Only the original data column
      assert Map.keys(row) == ["data"]
    end

    test "expand discovers keys from multiple rows" do
      df =
        Dux.from_query("""
          SELECT '{"a": 1}' AS data
          UNION ALL SELECT '{"a": 2, "b": 3}'
        """)
        |> Dux.compute()

      result = Dux.JSON.expand(df, :data) |> Dux.to_rows()

      # Should have columns a and b (discovered from both rows)
      assert Enum.all?(result, &Map.has_key?(&1, "a"))
      assert Enum.any?(result, &Map.has_key?(&1, "b"))
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "extract with special characters in key" do
      df = Dux.from_query(~s(SELECT '{"my-key": "fine", "key_2": "ok"}' AS data))

      result =
        Dux.JSON.extract(df, :data, %{val: "$.my-key", val2: "$.key_2"})
        |> Dux.to_rows()

      assert hd(result)["val"] == "fine"
      assert hd(result)["val2"] == "ok"
    end

    test "extract with deeply nested path" do
      df = Dux.from_query(~s(SELECT '{"a": {"b": {"c": {"d": 42}}}}' AS data))

      result =
        Dux.JSON.extract(df, :data, %{deep: "$.a.b.c.d"})
        |> Dux.to_rows()

      assert hd(result)["deep"] == "42"
    end

    test "unnest with array of objects" do
      df =
        Dux.from_query(~s(SELECT '[{"name": "Alice"}, {"name": "Bob"}]'::JSON AS people))

      result =
        Dux.JSON.unnest(df, :people)
        |> Dux.to_rows()

      assert length(result) == 2
    end

    test "extract composes with filter and sort" do
      df =
        Dux.from_query("""
          SELECT '{"score": 90}' AS data, 'a' AS id
          UNION ALL SELECT '{"score": 70}', 'b'
          UNION ALL SELECT '{"score": 85}', 'c'
        """)

      result =
        df
        |> Dux.JSON.extract(:data, %{score: "$.score"})
        |> Dux.filter_with("CAST(score AS INTEGER) > 75")
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 2
      ids = Enum.map(result, & &1["id"])
      assert ids == ["a", "c"]
    end
  end
end
