defmodule Dux.MacroTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  require Dux

  alias Dux.Backend
  alias Dux.Remote.Worker

  setup do
    # Clean up any macros from previous tests
    for {name, _sql} <- Dux.list_macros() do
      Dux.undefine(name)
    end

    :ok
  end

  # ---------- Happy path ----------

  describe "define/3 scalar macros" do
    test "defines a simple scalar macro and returns correct result" do
      assert :ok = Dux.define(:double, [:x], "x * 2")
      rows = Dux.from_query("SELECT double(21) AS result") |> Dux.to_rows()
      assert [%{"result" => 42}] = rows
    end

    test "multi-param macro produces correct output" do
      Dux.define(:add, [:a, :b], "a + b")
      rows = Dux.from_query("SELECT add(10, 32) AS result") |> Dux.to_rows()
      assert [%{"result" => 42}] = rows
    end

    test "macro works in Dux.mutate pipeline" do
      Dux.define(:double, [:x], "x * 2")

      rows =
        Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}])
        |> Dux.mutate_with(doubled: "double(x)")
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["doubled"]) == [2, 4, 6]
    end

    test "macro works in Dux.filter pipeline" do
      Dux.define(:is_big, [:x], "x > 10")

      rows =
        Dux.from_list([%{val: 5}, %{val: 15}, %{val: 25}])
        |> Dux.filter_with("is_big(val)")
        |> Dux.to_rows()

      assert length(rows) == 2
      assert Enum.all?(rows, fn r -> r["val"] > 10 end)
    end

    test "CASE expression macro classifies correctly" do
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
        |> Dux.sort_by(:score)
        |> Dux.to_rows()

      assert Enum.map(rows, & &1["risk"]) == ["low", "medium", "high"]
    end

    test "redefine (CREATE OR REPLACE) updates the macro" do
      Dux.define(:double, [:x], "x * 2")
      assert [%{"r" => 10}] = Dux.from_query("SELECT double(5) AS r") |> Dux.to_rows()

      Dux.define(:double, [:x], "x * 3")
      assert [%{"r" => 15}] = Dux.from_query("SELECT double(5) AS r") |> Dux.to_rows()
    end

    test "macro with string operations" do
      Dux.define(:greet, [:name], "CONCAT('Hello, ', name, '!')")

      rows = Dux.from_query("SELECT greet('World') AS msg") |> Dux.to_rows()
      assert [%{"msg" => "Hello, World!"}] = rows
    end

    test "macro returning boolean" do
      Dux.define(:in_range, [:val, :lo, :hi], "val >= lo AND val <= hi")

      rows =
        Dux.from_list([%{x: 1}, %{x: 5}, %{x: 10}])
        |> Dux.filter_with("in_range(x, 3, 7)")
        |> Dux.to_rows()

      assert [%{"x" => 5}] = rows
    end
  end

  describe "define_table/3 table macros" do
    test "defines a table macro returning correct rows" do
      Dux.define_table(:my_range, [:n], "SELECT * FROM range(n) t(x)")

      rows = Dux.from_query("SELECT * FROM my_range(5)") |> Dux.to_rows()
      assert length(rows) == 5
      assert Enum.map(rows, & &1["x"]) == [0, 1, 2, 3, 4]
    end

    test "table macro with multiple params" do
      Dux.define_table(:my_series, [:start_val, :end_val], """
        SELECT * FROM generate_series(start_val, end_val) t(x)
      """)

      rows = Dux.from_query("SELECT * FROM my_series(10, 15)") |> Dux.to_rows()
      assert length(rows) == 6
      assert Enum.map(rows, & &1["x"]) == Enum.to_list(10..15)
    end

    test "table macro used in a join" do
      Dux.define_table(:dim, [:n], """
        SELECT x AS id, CONCAT('item_', x) AS label FROM range(n) t(x)
      """)

      rows =
        Dux.from_list([%{id: 0, val: 100}, %{id: 2, val: 200}])
        |> Dux.join(Dux.from_query("SELECT * FROM dim(5)"), on: :id)
        |> Dux.to_rows()

      assert length(rows) == 2
      assert Enum.all?(rows, fn r -> is_binary(r["label"]) end)
    end
  end

  describe "undefine/1" do
    test "removes a scalar macro — calling it raises" do
      Dux.define(:double, [:x], "x * 2")
      assert :ok = Dux.undefine(:double)

      assert_raise ArgumentError, fn ->
        Dux.from_query("SELECT double(1) AS r") |> Dux.to_rows()
      end
    end

    test "removes a table macro — calling it raises" do
      Dux.define_table(:my_range, [:n], "SELECT * FROM range(n) t(x)")
      assert :ok = Dux.undefine(:my_range)

      assert_raise ArgumentError, fn ->
        Dux.from_query("SELECT * FROM my_range(5)") |> Dux.to_rows()
      end
    end

    test "undefine on non-existent macro is ok" do
      assert :ok = Dux.undefine(:nonexistent)
    end

    test "undefine removes from registry" do
      Dux.define(:double, [:x], "x * 2")
      assert length(Dux.list_macros()) == 1
      Dux.undefine(:double)
      assert Dux.list_macros() == []
    end
  end

  describe "list_macros/0" do
    test "returns empty list when no macros defined" do
      assert Dux.list_macros() == []
    end

    test "returns all defined macros" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define(:triple, [:x], "x * 3")

      names = Dux.list_macros() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      assert names == [:double, :triple]
    end

    test "includes both scalar and table macros" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define_table(:my_range, [:n], "SELECT * FROM range(n) t(x)")

      names = Dux.list_macros() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      assert names == [:double, :my_range]
    end
  end

  describe "macro_setup_sqls/0" do
    test "returns CREATE OR REPLACE SQL for each macro" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define(:triple, [:x], "x * 3")
      sqls = Dux.macro_setup_sqls()

      assert length(sqls) == 2
      assert Enum.all?(sqls, &String.starts_with?(&1, "CREATE OR REPLACE MACRO"))
    end

    test "returns empty list when no macros" do
      assert Dux.macro_setup_sqls() == []
    end
  end

  # ---------- Sad path ----------

  describe "sad path" do
    test "define with invalid SQL body raises" do
      assert_raise Adbc.Error, fn ->
        Dux.define(:bad, [:x], "INVALID SQL HERE!!!")
      end
    end

    test "define with mismatched param names raises at define time" do
      # DuckDB validates body references against params at define time
      assert_raise Adbc.Error, fn ->
        Dux.define(:bad_ref, [:x], "y * 2")
      end
    end

    test "calling macro with wrong arity raises" do
      Dux.define(:double, [:x], "x * 2")

      assert_raise ArgumentError, fn ->
        Dux.from_query("SELECT double(1, 2) AS r") |> Dux.to_rows()
      end
    end

    test "calling macro with zero args when it expects one raises" do
      Dux.define(:double, [:x], "x * 2")

      assert_raise ArgumentError, fn ->
        Dux.from_query("SELECT double() AS r") |> Dux.to_rows()
      end
    end

    test "define_table with invalid SQL body raises" do
      assert_raise Adbc.Error, fn ->
        Dux.define_table(:bad_table, [:x], "NOT VALID SQL")
      end
    end

    test "invalid SQL does not leave partial macro in registry" do
      try do
        Dux.define(:bad, [:x], "INVALID SQL!!!")
      rescue
        _ -> :ok
      end

      assert Dux.list_macros() == []
    end
  end

  # ---------- Adversarial ----------

  describe "adversarial" do
    test "macro with no params (constant)" do
      Dux.define(:pi_approx, [], "3.14159")
      rows = Dux.from_query("SELECT pi_approx() AS pi") |> Dux.to_rows()
      assert [%{"pi" => pi}] = rows
      assert_in_delta pi, 3.14159, 0.001
    end

    test "macro with 10 params" do
      params = Enum.map(1..10, &:"p#{&1}")
      body = Enum.map_join(params, " + ", &to_string/1)
      Dux.define(:sum10, params, body)

      args = Enum.map_join(1..10, ", ", &to_string/1)
      rows = Dux.from_query("SELECT sum10(#{args}) AS result") |> Dux.to_rows()
      assert [%{"result" => 55}] = rows
    end

    test "macro handles NULL inputs" do
      Dux.define(:safe_add, [:a, :b], "COALESCE(a, 0) + COALESCE(b, 0)")

      rows =
        Dux.from_query(
          "SELECT safe_add(NULL, 5) AS r1, safe_add(3, NULL) AS r2, safe_add(NULL, NULL) AS r3"
        )
        |> Dux.to_rows()

      assert [%{"r1" => 5, "r2" => 3, "r3" => 0}] = rows
    end

    test "macro with NULL propagation" do
      Dux.define(:double, [:x], "x * 2")

      rows =
        Dux.from_query("SELECT double(NULL) AS result")
        |> Dux.to_rows()

      assert [%{"result" => nil}] = rows
    end

    test "macro composing with other macros" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define(:inc, [:x], "x + 1")

      rows = Dux.from_query("SELECT double(inc(5)) AS result") |> Dux.to_rows()
      assert [%{"result" => 12}] = rows
    end

    test "macro with SQL reserved word as param name" do
      # Reserved words as params require quoting in the body too
      Dux.define(:my_fn, [:select, :from], ~s("select" + "from"))

      rows = Dux.from_query("SELECT my_fn(10, 20) AS result") |> Dux.to_rows()
      assert [%{"result" => 30}] = rows
    end

    test "macro handles string values with single quotes" do
      Dux.define(:wrap, [:s], "CONCAT('[', s, ']')")

      rows = Dux.from_query("SELECT wrap('hello world') AS result") |> Dux.to_rows()
      assert [%{"result" => "[hello world]"}] = rows
    end

    test "macro handles Unicode" do
      Dux.define(:echo, [:s], "s")

      rows = Dux.from_query("SELECT echo('日本語テスト 🎉') AS result") |> Dux.to_rows()
      assert [%{"result" => "日本語テスト 🎉"}] = rows
    end

    test "macro handles empty string" do
      Dux.define(:echo, [:s], "s")

      rows = Dux.from_query("SELECT echo('') AS result") |> Dux.to_rows()
      assert [%{"result" => ""}] = rows
    end

    test "many macros defined simultaneously" do
      for i <- 1..50 do
        Dux.define(:"fn_#{i}", [:x], "x + #{i}")
      end

      assert length(Dux.list_macros()) == 50

      # Verify last one works
      rows = Dux.from_query("SELECT fn_50(0) AS r") |> Dux.to_rows()
      assert [%{"r" => 50}] = rows
    end
  end

  # ---------- Wicked (multi-step, scale) ----------

  describe "wicked" do
    test "macro in multi-step pipeline with 1000+ rows" do
      Dux.define(:normalize, [:val, :lo, :hi], "(val - lo) / NULLIF(hi - lo, 0)")

      data = for i <- 1..2000, do: %{x: i}

      rows =
        Dux.from_list(data)
        |> Dux.mutate_with(normed: "normalize(x, 1, 2000)")
        |> Dux.filter_with("normed > 0.5")
        |> Dux.sort_by(:normed)
        |> Dux.to_rows()

      # normalize(x, 1, 2000) > 0.5 means x > 1000.5, so x >= 1001
      # That's 2000 - 1001 + 1 = 1000 rows
      assert length(rows) == 1000
      assert hd(rows)["x"] == 1001
      assert List.last(rows)["x"] == 2000
    end

    test "macro in group_by + summarise pipeline" do
      Dux.define(:bucket, [:val], """
        CASE WHEN val > 50 THEN 'high' ELSE 'low' END
      """)

      data = for i <- 1..1000, do: %{id: i, val: i}

      rows =
        Dux.from_list(data)
        |> Dux.mutate_with(group: "bucket(val)")
        |> Dux.group_by("group")
        |> Dux.summarise_with(n: "COUNT(*)", avg_val: "AVG(val)")
        |> Dux.sort_by("group")
        |> Dux.to_rows()

      assert length(rows) == 2
      [high, low] = rows
      assert high["group"] == "high"
      assert high["n"] == 950
      assert low["group"] == "low"
      assert low["n"] == 50
    end

    test "chained macros in pipeline" do
      Dux.define(:double, [:x], "x * 2")
      Dux.define(:shift, [:x, :y], "x + y")
      Dux.define(:clamp, [:val, :lo, :hi], "GREATEST(lo, LEAST(hi, val))")

      rows =
        Dux.from_query("SELECT x FROM range(100) t(x)")
        |> Dux.mutate_with(
          d: "double(x)",
          shifted: "shift(double(x), -50)",
          clamped: "clamp(shift(double(x), -50), 0, 100)"
        )
        |> Dux.filter_with("clamped > 0 AND clamped < 100")
        |> Dux.to_rows()

      # Verify the chain: double(x) - 50, then clamp to [0, 100]
      # x=0: double=0, shifted=-50, clamped=0 → filtered out (not > 0)
      # x=25: double=50, shifted=0, clamped=0 → filtered out
      # x=26: double=52, shifted=2, clamped=2 → kept
      # x=75: double=150, shifted=100, clamped=100 → filtered out (not < 100)
      assert rows != []
      assert Enum.all?(rows, fn r -> r["clamped"] > 0 and r["clamped"] < 100 end)
    end

    test "table macro feeding into scalar macro pipeline" do
      Dux.define_table(:gen_data, [:n], """
        SELECT x AS id, x * 1.5 AS val FROM range(n) t(x)
      """)

      Dux.define(:classify, [:val], """
        CASE WHEN val > 50 THEN 'big' ELSE 'small' END
      """)

      rows =
        Dux.from_query("SELECT * FROM gen_data(100)")
        |> Dux.mutate_with(class: "classify(val)")
        |> Dux.group_by("class")
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.to_rows()

      total = rows |> Enum.map(& &1["n"]) |> Enum.sum()
      assert total == 100
    end

    test "define and undefine cycle" do
      # Define, use, undefine, redefine with different body
      Dux.define(:cycle, [:x], "x * 2")
      assert [%{"r" => 10}] = Dux.from_query("SELECT cycle(5) AS r") |> Dux.to_rows()

      Dux.undefine(:cycle)
      assert Dux.list_macros() == []

      Dux.define(:cycle, [:x], "x * 3")
      assert [%{"r" => 15}] = Dux.from_query("SELECT cycle(5) AS r") |> Dux.to_rows()
    end
  end

  # ---------- Property-based ----------

  describe "property-based" do
    property "macro result matches inline SQL for arithmetic" do
      check all(
              a <- integer(-1000..1000),
              b <- integer(-1000..1000)
            ) do
        Dux.define(:prop_add, [:x, :y], "x + y")

        macro_result =
          Dux.from_query("SELECT prop_add(#{a}, #{b}) AS r") |> Dux.to_rows() |> hd()

        inline_result =
          Dux.from_query("SELECT (#{a} + #{b}) AS r") |> Dux.to_rows() |> hd()

        assert macro_result["r"] == inline_result["r"]
      end
    end

    property "define then list always shows the macro" do
      check all(name_suffix <- positive_integer()) do
        name = :"prop_macro_#{name_suffix}"
        Dux.define(name, [:x], "x")

        names = Dux.list_macros() |> Enum.map(&elem(&1, 0))
        assert name in names

        Dux.undefine(name)
      end
    end

    property "undefine then list never shows the macro" do
      check all(name_suffix <- positive_integer()) do
        name = :"prop_gone_#{name_suffix}"
        Dux.define(name, [:x], "x")
        Dux.undefine(name)

        names = Dux.list_macros() |> Enum.map(&elem(&1, 0))
        refute name in names
      end
    end
  end

  # ---------- Distributed (worker replay) ----------

  describe "distributed macro replay" do
    test "macros replay on a single local worker" do
      Dux.define(:worker_double, [:x], "x * 2")

      {:ok, w1} = Worker.start_link()

      try do
        # Execute directly on the worker — verifies macro was replayed
        pipeline =
          Dux.from_query("SELECT x FROM range(100) t(x)")
          |> Dux.mutate_with(doubled: "worker_double(x)")
          |> Dux.summarise_with(total: "SUM(doubled)")

        {:ok, ipc} = Worker.execute(w1, pipeline)
        assert is_binary(ipc)

        # Deserialize and verify
        conn = Dux.Connection.get_conn()
        ref = Backend.table_from_ipc(conn, ipc)
        rows = Backend.table_to_rows(conn, ref)

        assert [%{"total" => 9900}] = rows
      after
        GenServer.stop(w1)
      end
    end

    test "macro_setup_sqls are replayable on a fresh connection" do
      Dux.define(:classify, [:x], """
        CASE WHEN x > 50 THEN 'high' ELSE 'low' END
      """)

      # Simulate what a worker does: start a fresh connection, replay macros
      {_db, fresh_conn} = Backend.open()

      Enum.each(Dux.macro_setup_sqls(), fn sql ->
        Backend.execute(fresh_conn, sql)
      end)

      # Verify the macro works on the fresh connection
      ref = Backend.query(fresh_conn, "SELECT classify(100) AS result")
      rows = Backend.table_to_rows(fresh_conn, ref)
      assert [%{"result" => "high"}] = rows
    end

    test "distributed pipeline with macros produces correct result (single worker)" do
      Dux.define(:classify, [:x], """
        CASE WHEN x > 50 THEN 'high' ELSE 'low' END
      """)

      # Single-worker distributed avoids the replication issue
      {:ok, w1} = Worker.start_link()

      try do
        local_rows =
          Dux.from_query("SELECT x FROM range(200) t(x)")
          |> Dux.mutate_with(class: "classify(x)")
          |> Dux.group_by("class")
          |> Dux.summarise_with(n: "COUNT(*)")
          |> Dux.to_rows()
          |> Enum.sort_by(& &1["class"])

        dist_rows =
          Dux.from_query("SELECT x FROM range(200) t(x)")
          |> Dux.mutate_with(class: "classify(x)")
          |> Dux.distribute([w1])
          |> Dux.group_by("class")
          |> Dux.summarise_with(n: "COUNT(*)")
          |> Dux.collect()
          |> Dux.to_rows()
          |> Enum.sort_by(& &1["class"])

        assert local_rows == dist_rows
      after
        GenServer.stop(w1)
      end
    end
  end
end
