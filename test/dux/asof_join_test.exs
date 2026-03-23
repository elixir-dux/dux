defmodule Dux.AsofJoinTest do
  use ExUnit.Case, async: true
  require Dux

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "basic ASOF JOIN" do
    test "match trades to most recent quotes with >=" do
      trades =
        Dux.from_list([
          %{symbol: "AAPL", ts: 10, price: 150.0},
          %{symbol: "AAPL", ts: 20, price: 152.0}
        ])

      quotes =
        Dux.from_list([
          %{symbol: "AAPL", ts: 5, bid: 149.0},
          %{symbol: "AAPL", ts: 15, bid: 151.0}
        ])

      result =
        Dux.asof_join(trades, quotes, on: :symbol, by: {:ts, :>=})
        |> Dux.sort_by(:price)
        |> Dux.to_rows()

      # ts=10 matches quote at ts=5 (most recent <= 10)
      # ts=20 matches quote at ts=15 (most recent <= 20)
      assert length(result) == 2
      row1 = Enum.find(result, &(&1["price"] == 150.0))
      assert row1["bid"] == 149

      row2 = Enum.find(result, &(&1["price"] == 152.0))
      assert row2["bid"] == 151
    end

    test "ASOF JOIN with multiple equality columns" do
      trades =
        Dux.from_list([
          %{exchange: "NYSE", symbol: "A", ts: 10, price: 100},
          %{exchange: "NYSE", symbol: "B", ts: 20, price: 200},
          %{exchange: "NASDAQ", symbol: "A", ts: 15, price: 150}
        ])

      quotes =
        Dux.from_list([
          %{exchange: "NYSE", symbol: "A", ts: 5, bid: 99},
          %{exchange: "NYSE", symbol: "B", ts: 18, bid: 198},
          %{exchange: "NASDAQ", symbol: "A", ts: 12, bid: 148}
        ])

      result =
        Dux.asof_join(trades, quotes,
          on: [:exchange, :symbol],
          by: {:ts, :>=}
        )
        |> Dux.sort_by(:price)
        |> Dux.to_rows()

      assert length(result) == 3
    end

    test "LEFT ASOF preserves unmatched left rows" do
      trades =
        Dux.from_list([
          %{symbol: "AAPL", ts: 1, price: 100.0},
          %{symbol: "AAPL", ts: 10, price: 150.0}
        ])

      quotes =
        Dux.from_list([
          %{symbol: "AAPL", ts: 5, bid: 149.0}
        ])

      result =
        Dux.asof_join(trades, quotes,
          on: :symbol,
          by: {:ts, :>=},
          how: :left
        )
        |> Dux.sort_by(:ts)
        |> Dux.to_rows()

      # ts=1 has no matching quote (no quote <= 1) → bid is NULL
      assert length(result) == 2
      row1 = hd(result)
      assert row1["price"] == 100.0
      assert row1["bid"] == nil
    end

    test "ASOF JOIN with <= operator" do
      events =
        Dux.from_list([
          %{id: 1, ts: 10},
          %{id: 2, ts: 20}
        ])

      snapshots =
        Dux.from_list([
          %{ts: 15, value: "snap_15"},
          %{ts: 25, value: "snap_25"}
        ])

      result =
        Dux.asof_join(events, snapshots, by: {:ts, :<=})
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      # ts=10 matches next snapshot at ts=15 (nearest ts >= 10)
      # ts=20 matches next snapshot at ts=25
      assert length(result) == 2
      assert hd(result)["value"] == "snap_15"
    end

    test "ASOF JOIN with integer timestamps" do
      left = Dux.from_list(Enum.map(1..5, &%{x: &1 * 10}))
      right = Dux.from_list(Enum.map(1..10, &%{x: &1 * 3, label: "r_#{&1}"}))

      result =
        Dux.asof_join(left, right, by: {:x, :>=})
        |> Dux.sort_by(:x)
        |> Dux.to_rows()

      assert length(result) == 5
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "raises on missing :by option" do
      left = Dux.from_list([%{x: 1}])
      right = Dux.from_list([%{x: 2}])

      assert_raise KeyError, fn ->
        Dux.asof_join(left, right, on: :x)
      end
    end

    test "raises on invalid operator" do
      left = Dux.from_list([%{x: 1}])
      right = Dux.from_list([%{x: 2}])

      assert_raise ArgumentError, ~r/must be one of/, fn ->
        Dux.asof_join(left, right, by: {:x, :==})
      end
    end

    test "inner ASOF with no matches returns empty" do
      left = Dux.from_list([%{symbol: "AAPL", ts: 1}])
      right = Dux.from_list([%{symbol: "AAPL", ts: 100}])

      result =
        Dux.asof_join(left, right, on: :symbol, by: {:ts, :>=})
        |> Dux.to_rows()

      # ts=1 has no quote with ts <= 1 (only ts=100)
      assert result == []
    end

    test "ASOF JOIN with no equality columns" do
      left = Dux.from_list([%{ts: 10, val: "a"}, %{ts: 20, val: "b"}])
      right = Dux.from_list([%{ts: 5, score: 1}, %{ts: 15, score: 2}])

      result =
        Dux.asof_join(left, right, by: {:ts, :>=})
        |> Dux.sort_by(:val)
        |> Dux.to_rows()

      assert length(result) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "exact timestamp matches" do
      left = Dux.from_list([%{ts: 10, val: "a"}])
      right = Dux.from_list([%{ts: 10, score: 100}])

      result =
        Dux.asof_join(left, right, by: {:ts, :>=})
        |> Dux.to_rows()

      assert length(result) == 1
      assert hd(result)["score"] == 100
    end

    test "many-to-one: multiple left rows match same right row" do
      left =
        Dux.from_list([
          %{ts: 10, val: "a"},
          %{ts: 11, val: "b"},
          %{ts: 12, val: "c"}
        ])

      right = Dux.from_list([%{ts: 5, score: 42}])

      result =
        Dux.asof_join(left, right, by: {:ts, :>=})
        |> Dux.to_rows()

      # All 3 left rows match the single right row
      assert length(result) == 3
      assert Enum.all?(result, &(&1["score"] == 42))
    end

    test "large dataset ASOF JOIN" do
      left = Dux.from_query("SELECT x AS ts, x * 1.5 AS val FROM range(1, 1001) t(x)")

      right =
        Dux.from_query("SELECT x * 3 AS ts, x AS label FROM range(1, 501) t(x)")

      result =
        Dux.asof_join(left, right, by: {:ts, :>=})
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.to_rows()

      assert hd(result)["n"] > 0
    end

    test "string column names work" do
      left = Dux.from_list([%{"my_ts" => 10, "val" => "a"}])
      right = Dux.from_list([%{"my_ts" => 5, "score" => 100}])

      result =
        Dux.asof_join(left, right, by: {"my_ts", :>=})
        |> Dux.to_rows()

      assert length(result) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "ASOF JOIN piped into regular join" do
      trades =
        Dux.from_list([
          %{symbol: "AAPL", ts: 10, price: 150},
          %{symbol: "GOOG", ts: 20, price: 2800}
        ])

      quotes =
        Dux.from_list([
          %{symbol: "AAPL", ts: 5, bid: 149},
          %{symbol: "GOOG", ts: 15, bid: 2790}
        ])

      companies =
        Dux.from_list([
          %{symbol: "AAPL", name: "Apple"},
          %{symbol: "GOOG", name: "Google"}
        ])

      result =
        Dux.asof_join(trades, quotes, on: :symbol, by: {:ts, :>=})
        |> Dux.join(companies, on: :symbol)
        |> Dux.sort_by(:price)
        |> Dux.to_rows()

      assert length(result) == 2
      names = Enum.map(result, & &1["name"]) |> Enum.sort()
      assert names == ["Apple", "Google"]
    end

    test "ASOF JOIN followed by aggregation" do
      trades =
        Dux.from_list([
          %{symbol: "A", ts: 10, price: 100},
          %{symbol: "A", ts: 20, price: 200},
          %{symbol: "B", ts: 15, price: 150}
        ])

      quotes =
        Dux.from_list([
          %{symbol: "A", ts: 5, spread: 0.5},
          %{symbol: "A", ts: 15, spread: 0.3},
          %{symbol: "B", ts: 10, spread: 0.8}
        ])

      result =
        Dux.asof_join(trades, quotes, on: :symbol, by: {:ts, :>=})
        |> Dux.group_by(:symbol)
        |> Dux.summarise_with(n: "COUNT(*)", avg_price: "AVG(price)")
        |> Dux.sort_by(:symbol)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.find(result, &(&1["symbol"] == "A"))["n"] == 2
    end

    test "all four operators produce results" do
      left = Dux.from_list([%{ts: 10}])
      right = Dux.from_list([%{ts: 5}, %{ts: 15}])

      for op <- [:>=, :>, :<=, :<] do
        result =
          Dux.asof_join(left, right, by: {:ts, op}, how: :left)
          |> Dux.to_rows()

        assert length(result) == 1, "operator #{op} should produce 1 row"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # E2E with datasets
  # ---------------------------------------------------------------------------

  describe "end-to-end" do
    test "flights ASOF join with time-ordered data" do
      # Create time-ordered flight data and match to time buckets
      flights =
        Dux.from_query("""
          SELECT dep_time, carrier, origin
          FROM (#{flights_sql()})
          WHERE dep_time IS NOT NULL
          ORDER BY dep_time
          LIMIT 100
        """)

      time_buckets =
        Dux.from_list(
          for h <- 0..23 do
            %{dep_time: h * 100, period: if(h < 12, do: "AM", else: "PM")}
          end
        )

      result =
        Dux.asof_join(flights, time_buckets, by: {:dep_time, :>=})
        |> Dux.to_rows()

      assert result != []
      assert Enum.all?(result, &Map.has_key?(&1, "period"))
    end
  end

  defp flights_sql do
    path = Application.app_dir(:dux, "priv/datasets/flights.csv")
    "SELECT * FROM read_csv('#{path}', nullstr='NA')"
  end
end
