defmodule Dux.MacroTest do
  use ExUnit.Case, async: false

  require Dux

  setup do
    # Clean up any macros from previous tests
    for {name, _sql} <- Dux.list_macros() do
      Dux.undefine(name)
    end

    :ok
  end

  # ---------- Happy path ----------

  describe "define/3 scalar macros" do
    test "defines a simple scalar macro" do
      assert :ok = Dux.define(:double, [:x], "x * 2")
      rows = Dux.from_query("SELECT double(21) AS result") |> Dux.to_rows()
      assert [%{"result" => 42}] = rows
    end

    test "defines a multi-param macro" do
      Dux.define(:add, [:a, :b], "a + b")
      rows = Dux.from_query("SELECT add(10, 32) AS result") |> Dux.to_rows()
      assert [%{"result" => 42}] = rows
    end

    test "macro works in Dux.mutate" do
      Dux.define(:double, [:x], "x * 2")

      rows =
        Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}])
        |> Dux.mutate_with(doubled: "double(x)")
        |> Dux.to_rows()

      assert length(rows) == 3
      assert Enum.map(rows, & &1["doubled"]) == [2, 4, 6]
    end

    test "macro works in Dux.filter" do
      Dux.define(:is_big, [:x], "x > 10")

      rows =
        Dux.from_list([%{val: 5}, %{val: 15}, %{val: 25}])
        |> Dux.filter_with("is_big(val)")
        |> Dux.to_rows()

      assert length(rows) == 2
    end

    test "CASE expression macro" do
      Dux.define(:risk_bucket, [:score], """
        CASE
          WHEN score > 0.8 THEN 'high'
          WHEN score > 0.5 THEN 'medium'
          ELSE 'low'
        END
      """)

      rows =
        Dux.from_list([%{score: 0.9}, %{score: 0.6}, %{score: 0.2}])
        |> Dux.mutate_with(risk: "risk_bucket(score)")
        |> Dux.to_rows()

      risks = Enum.map(rows, & &1["risk"]) |> Enum.sort()
      assert risks == ["high", "low", "medium"]
    end

    test "redefine (CREATE OR REPLACE) updates the macro" do
      Dux.define(:double, [:x], "x * 2")
      rows = Dux.from_query("SELECT double(5) AS r") |> Dux.to_rows()
      assert [%{"r" => 10}] = rows

      # Redefine to triple
      Dux.define(:double, [:x], "x * 3")
      rows = Dux.from_query("SELECT double(5) AS r") |> Dux.to_rows()
      assert [%{"r" => 15}] = rows
    end
  end

  describe "define_table/3 table macros" do
    test "defines a table macro" do
      Dux.define_table(:my_range, [:n], "SELECT * FROM range(n) t(x)")

      rows = Dux.from_query("SELECT * FROM my_range(5)") |> Dux.to_rows()
      assert length(rows) == 5
    end

    test "table macro with multiple params" do
      Dux.define_table(:my_series, [:start_val, :end_val], """
        SELECT * FROM generate_series(start_val, end_val) t(x)
      """)

      rows = Dux.from_query("SELECT * FROM my_series(10, 15)") |> Dux.to_rows()
      assert length(rows) == 6
    end
  end

  describe "undefine/1" do
    test "removes a scalar macro" do
      Dux.define(:double, [:x], "x * 2")
      assert :ok = Dux.undefine(:double)

      assert_raise ArgumentError, fn ->
        Dux.from_query("SELECT double(1) AS r") |> Dux.to_rows()
      end
    end

    test "removes a table macro" do
      Dux.define_table(:my_range, [:n], "SELECT * FROM range(n) t(x)")
      assert :ok = Dux.undefine(:my_range)

      assert_raise ArgumentError, fn ->
        Dux.from_query("SELECT * FROM my_range(5)") |> Dux.to_rows()
      end
    end

    test "undefine on non-existent macro is ok" do
      assert :ok = Dux.undefine(:nonexistent)
    end
  end

  describe "list_macros/0" do
    test "returns empty list when no macros defined" do
      assert Dux.list_macros() == []
    end

    test "returns defined macros" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define(:triple, [:x], "x * 3")

      macros = Dux.list_macros()
      names = Enum.map(macros, &elem(&1, 0)) |> Enum.sort()
      assert names == [:double, :triple]
    end

    test "undefine removes from list" do
      Dux.define(:double, [:x], "x * 2")
      Dux.undefine(:double)
      assert Dux.list_macros() == []
    end
  end

  describe "macro_setup_sqls/0" do
    test "returns SQL for replay on workers" do
      Dux.define(:double, [:x], "x * 2")
      sqls = Dux.macro_setup_sqls()
      assert length(sqls) == 1
      assert hd(sqls) =~ "CREATE OR REPLACE MACRO"
      assert hd(sqls) =~ "double"
    end
  end

  # ---------- Sad path ----------

  describe "error handling" do
    test "define with invalid SQL raises" do
      assert_raise Adbc.Error, fn ->
        Dux.define(:bad, [:x], "INVALID SQL HERE!!!")
      end
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial" do
    test "macro with no params" do
      Dux.define(:pi_approx, [], "3.14159")
      rows = Dux.from_query("SELECT pi_approx() AS pi") |> Dux.to_rows()
      assert [%{"pi" => pi}] = rows
      assert_in_delta pi, 3.14159, 0.001
    end

    test "macro with many params" do
      params = Enum.map(1..10, &:"p#{&1}")
      body = Enum.map_join(params, " + ", &to_string/1)
      Dux.define(:sum10, params, body)

      args = Enum.map_join(1..10, ", ", &to_string/1)
      rows = Dux.from_query("SELECT sum10(#{args}) AS result") |> Dux.to_rows()
      assert [%{"result" => 55}] = rows
    end

    test "macro composing with other macros" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define(:inc, [:x], "x + 1")

      rows = Dux.from_query("SELECT double(inc(5)) AS result") |> Dux.to_rows()
      assert [%{"result" => 12}] = rows
    end

    test "macro used in a full pipeline" do
      Dux.define(:normalize, [:val, :lo, :hi], "(val - lo) / NULLIF(hi - lo, 0)")

      rows =
        Dux.from_list([%{x: 0}, %{x: 50}, %{x: 100}])
        |> Dux.mutate_with(normed: "normalize(x, 0, 100)")
        |> Dux.filter_with("normed > 0.25")
        |> Dux.sort_by(:normed)
        |> Dux.to_rows()

      assert length(rows) == 2
      assert hd(rows)["normed"] == 0.5
    end
  end
end
