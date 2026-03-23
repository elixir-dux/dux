if Code.ensure_loaded?(Nx) do
  defmodule Dux.NxTest do
    use ExUnit.Case, async: false
    require Dux

    # ---------------------------------------------------------------------------
    # to_tensor/2
    # ---------------------------------------------------------------------------

    describe "to_tensor/2" do
      test "converts integer column to tensor" do
        df = Dux.from_list([%{"x" => 1}, %{"x" => 2}, %{"x" => 3}])
        tensor = Dux.to_tensor(df, :x)
        assert Nx.to_list(tensor) == [1, 2, 3]
        # DuckDB may infer s32 or s64 depending on value range
        assert Nx.type(tensor) in [{:s, 32}, {:s, 64}]
      end

      test "converts float column to tensor" do
        df = Dux.from_query("SELECT 1.5::DOUBLE AS x UNION ALL SELECT 2.5 UNION ALL SELECT 3.5")
        tensor = Dux.to_tensor(df, :x)
        assert Nx.type(tensor) == {:f, 64}
        assert_in_delta Nx.to_number(tensor[0]), 1.5, 0.01
      end

      test "converts date column to s32 tensor" do
        df = Dux.from_query("SELECT DATE '2024-01-01' AS d UNION ALL SELECT DATE '2024-01-02'")
        tensor = Dux.to_tensor(df, :d)
        assert Nx.type(tensor) == {:s, 32}
        assert Nx.shape(tensor) == {2}
        # date32 is days since epoch — just verify they're sequential
        [d1, d2] = Nx.to_list(tensor)
        assert d2 - d1 == 1
      end

      test "converts timestamp column to s64 tensor" do
        df = Dux.from_query("SELECT TIMESTAMP '2024-01-01 00:00:00' AS ts")
        tensor = Dux.to_tensor(df, :ts)
        assert Nx.type(tensor) == {:s, 64}
        assert Nx.shape(tensor) == {1}
      end

      test "raises on boolean column with cast hint" do
        df = Dux.from_list([%{"b" => true}, %{"b" => false}])

        assert_raise ArgumentError, ~r/cast to integer at the query level/, fn ->
          Dux.to_tensor(df, :b)
        end
      end

      test "raises on non-numeric column" do
        df = Dux.from_list([%{"name" => "hello"}])

        assert_raise ArgumentError, ~r/non-numeric type/, fn ->
          Dux.to_tensor(df, :name)
        end
      end

      test "works with pipeline output" do
        tensor =
          Dux.from_query("SELECT * FROM range(1, 6) t(x)")
          |> Dux.filter(x > 2)
          |> Dux.to_tensor(:x)

        assert Nx.to_list(tensor) == [3, 4, 5]
      end

      test "converts specific integer types" do
        df = Dux.from_query("SELECT 42::INTEGER AS x")
        tensor = Dux.to_tensor(df, :x)
        assert Nx.type(tensor) == {:s, 32}
      end
    end

    # ---------------------------------------------------------------------------
    # Nx.LazyContainer
    # ---------------------------------------------------------------------------

    describe "Nx.LazyContainer protocol" do
      test "traverses numeric columns" do
        # Use explicit DOUBLE cast to avoid decimal inference from Elixir floats
        df = Dux.from_query("SELECT 1 AS x, 2.0::DOUBLE AS y UNION ALL SELECT 3, 4.0")

        {result_map, count} =
          Nx.LazyContainer.traverse(df, 0, fn _template, fun, acc ->
            {fun.(), acc + 1}
          end)

        # Both x (integer) and y (float) should be traversed
        assert count == 2
        assert Map.has_key?(result_map, "x")
        assert Map.has_key?(result_map, "y")
        assert Nx.to_list(result_map["x"]) == [1, 3]
      end

      test "skips non-numeric columns" do
        df = Dux.from_list([%{"name" => "alice", "age" => 30}])

        {result_map, count} =
          Nx.LazyContainer.traverse(df, 0, fn _template, fun, acc ->
            {fun.(), acc + 1}
          end)

        # Only "age" should be traversed (integer), not "name" (string)
        assert count == 1
        assert Map.has_key?(result_map, "age")
        refute Map.has_key?(result_map, "name")
      end
    end

    # ---------------------------------------------------------------------------
    # Round-trip: Dux → Nx → verify
    # ---------------------------------------------------------------------------

    describe "round-trip" do
      test "pipeline → tensor → verify values match" do
        data = for i <- 1..100, do: %{"x" => i, "squared" => i * i}

        tensor_x =
          Dux.from_list(data)
          |> Dux.filter(x > 50)
          |> Dux.to_tensor(:x)

        tensor_sq =
          Dux.from_list(data)
          |> Dux.filter(x > 50)
          |> Dux.to_tensor(:squared)

        # Verify x values are 51..100
        x_list = Nx.to_list(tensor_x)
        assert length(x_list) == 50
        assert hd(x_list) == 51

        # Verify squared values match
        sq_list = Nx.to_list(tensor_sq)
        assert hd(sq_list) == 51 * 51
      end
    end
  end
end
