defmodule Dux.SortingTest do
  use ExUnit.Case, async: false

  setup_all do
    conn = Dux.Connection.get_conn()

    %{"threads" => [threads]} =
      Adbc.Connection.query!(conn, "SELECT current_setting('threads') AS threads")
      |> Adbc.Result.to_map()

    Adbc.Connection.query!(conn, "SET threads = 4")
    on_exit(fn -> Adbc.Connection.query!(conn, "SET threads = #{threads}") end)
    :ok
  end

  # ---------- Wicked: parallel materialization boundaries ----------

  for n <- [122_880, 122_881, 245_761] do
    test "sorts every row across the #{n}-row boundary" do
      n = unquote(n)

      result =
        Dux.from_query("SELECT (#{n} - x)::DOUBLE / 8 AS a, x AS id FROM range(#{n}) t(x)")
        |> Dux.head(n)
        |> Dux.sort_by(:a)
        |> Dux.to_columns()

      assert result["a"] == Enum.map(1..n, &(&1 / 8))
      assert result["id"] == Enum.to_list((n - 1)..0//-1)
    end
  end

  # ---------- Adversarial: duplicate keys, nulls, quoted names ----------

  test "descending multi-column sorting keeps nulls and duplicate rows aligned" do
    n = 122_881

    result =
      Dux.from_query("""
      SELECT CASE WHEN x % 1024 = 0 THEN NULL ELSE floor(x / 8)::DOUBLE END AS "sort key",
             x AS id
      FROM range(#{n}) t(x)
      """)
      |> Dux.sort_by(desc: "sort key", asc: :id)
      |> Dux.to_rows()

    expected =
      0..(n - 1)
      |> Enum.map(fn id ->
        %{"sort key" => if(rem(id, 1024) == 0, do: nil, else: Float.floor(id / 8)), "id" => id}
      end)
      |> Enum.sort_by(fn
        %{"sort key" => nil, "id" => id} -> {1, 0, id}
        %{"sort key" => value, "id" => id} -> {0, -value, id}
      end)

    assert result == expected
  end
end
