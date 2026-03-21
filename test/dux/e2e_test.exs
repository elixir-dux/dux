defmodule Dux.E2ETest do
  use ExUnit.Case, async: false
  require Dux

  @tmp_dir System.tmp_dir!()

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_e2e_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # Full analytics pipelines
  # ---------------------------------------------------------------------------

  describe "analytics pipeline" do
    test "sales analysis: filter → mutate → group → summarise → sort" do
      data =
        for i <- 1..100 do
          %{
            "id" => i,
            "region" => Enum.at(["US", "EU", "APAC"], rem(i, 3)),
            "product" => Enum.at(["Widget", "Gadget"], rem(i, 2)),
            "price" => 10 + rem(i, 50),
            "quantity" => 1 + rem(i, 10)
          }
        end

      result =
        Dux.from_list(data)
        |> Dux.filter(quantity > 3)
        |> Dux.mutate(revenue: price * quantity)
        |> Dux.group_by([:region, :product])
        |> Dux.summarise(
          total_revenue: sum(revenue),
          avg_price: avg(price),
          order_count: count(id)
        )
        |> Dux.sort_by(desc: :total_revenue)
        |> Dux.collect()

      assert result != []
      assert Enum.all?(result, &Map.has_key?(&1, "total_revenue"))
      assert Enum.all?(result, &Map.has_key?(&1, "avg_price"))
      assert Enum.all?(result, &Map.has_key?(&1, "order_count"))

      # Verify sorted descending
      revenues = Enum.map(result, & &1["total_revenue"])
      assert revenues == Enum.sort(revenues, :desc)
    end

    test "top-N per group using window function SQL" do
      data =
        for i <- 1..30 do
          %{
            "dept" => Enum.at(["eng", "sales", "ops"], rem(i, 3)),
            "name" => "person_#{i}",
            "salary" => 50_000 + i * 1000
          }
        end

      # Top 2 earners per department using DuckDB window function
      result =
        Dux.from_list(data)
        |> Dux.mutate_with(
          rn: "ROW_NUMBER() OVER (PARTITION BY \"dept\" ORDER BY \"salary\" DESC)"
        )
        |> Dux.filter_with("rn <= 2")
        |> Dux.sort_by([:dept, :salary])
        |> Dux.collect()

      # 3 departments × 2 top earners = 6 rows
      assert length(result) == 6

      # Each department should have exactly 2
      by_dept = Enum.group_by(result, & &1["dept"])
      assert length(by_dept["eng"]) == 2
      assert length(by_dept["sales"]) == 2
      assert length(by_dept["ops"]) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Compute → branch → rejoin
  # ---------------------------------------------------------------------------

  describe "branching pipelines" do
    test "compute once, branch into two pipelines, rejoin" do
      base =
        Dux.from_list([
          %{"id" => 1, "val" => 10},
          %{"id" => 2, "val" => 20},
          %{"id" => 3, "val" => 30},
          %{"id" => 4, "val" => 40}
        ])
        |> Dux.compute()

      high = base |> Dux.filter(val > 20) |> Dux.select([:id]) |> Dux.rename(id: :high_id)
      low = base |> Dux.filter(val <= 20) |> Dux.select([:id]) |> Dux.rename(id: :low_id)

      high_ids = high |> Dux.sort_by(:high_id) |> Dux.to_columns()
      low_ids = low |> Dux.sort_by(:low_id) |> Dux.to_columns()

      assert high_ids["high_id"] == [3, 4]
      assert low_ids["low_id"] == [1, 2]
    end

    test "computed base survives multiple downstream computations" do
      base = Dux.from_query("SELECT * FROM range(1, 101) t(x)") |> Dux.compute()

      results =
        for threshold <- [10, 25, 50, 75, 90] do
          base |> Dux.filter(x > ^threshold) |> Dux.n_rows()
        end

      assert results == [90, 75, 50, 25, 10]
    end
  end

  # ---------------------------------------------------------------------------
  # IO round-trips with transformations
  # ---------------------------------------------------------------------------

  describe "IO integration" do
    test "CSV → transform → Parquet → read back → verify" do
      csv_path = tmp_path("step1.csv")
      parquet_path = tmp_path("step2.parquet")

      try do
        # Write CSV
        Dux.from_list([
          %{"name" => "Alice", "score" => 85},
          %{"name" => "Bob", "score" => 92},
          %{"name" => "Carol", "score" => 78}
        ])
        |> Dux.to_csv(csv_path)

        # Read CSV → transform → write Parquet
        Dux.from_csv(csv_path)
        |> Dux.filter(score > 80)
        |> Dux.mutate(grade: upper("PASS"))
        |> Dux.to_parquet(parquet_path)

        # Read Parquet → verify
        result =
          Dux.from_parquet(parquet_path)
          |> Dux.sort_by(:name)
          |> Dux.collect()

        assert length(result) == 2
        assert Enum.at(result, 0)["name"] == "Alice"
        assert Enum.at(result, 1)["name"] == "Bob"
        assert Enum.all?(result, &(&1["grade"] == "PASS"))
      after
        File.rm(csv_path)
        File.rm(parquet_path)
      end
    end

    test "multiple Parquet files → glob read → aggregate" do
      dir = tmp_path("multi_parquet")
      File.mkdir_p!(dir)

      try do
        for month <- 1..3 do
          data =
            for day <- 1..10 do
              %{"month" => month, "day" => day, "sales" => month * 100 + day}
            end

          Dux.from_list(data)
          |> Dux.to_parquet(
            Path.join(dir, "month_#{String.pad_leading("#{month}", 2, "0")}.parquet")
          )
        end

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.group_by(:month)
          |> Dux.summarise(total: sum(sales), n: count(sales))
          |> Dux.sort_by(:month)
          |> Dux.collect()

        assert length(result) == 3
        assert Enum.at(result, 0)["n"] == 10
        assert Enum.at(result, 1)["n"] == 10
        assert Enum.at(result, 2)["n"] == 10
      after
        File.rm_rf!(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Graph → Dux verb composition
  # ---------------------------------------------------------------------------

  describe "graph + verb integration" do
    test "PageRank → join with vertex attributes → filter → export" do
      path = tmp_path("pagerank_results.parquet")

      try do
        vertices =
          Dux.from_list([
            %{"id" => 1, "name" => "Alice"},
            %{"id" => 2, "name" => "Bob"},
            %{"id" => 3, "name" => "Carol"}
          ])

        edges =
          Dux.from_list([
            %{"src" => 1, "dst" => 2},
            %{"src" => 2, "dst" => 3},
            %{"src" => 3, "dst" => 1},
            %{"src" => 3, "dst" => 2}
          ])

        graph = Dux.Graph.new(vertices: vertices, edges: edges)

        result =
          graph
          |> Dux.Graph.pagerank(iterations: 10)
          |> Dux.join(vertices, on: :id)
          |> Dux.sort_by(desc: :rank)
          |> Dux.collect()

        assert length(result) == 3
        assert Enum.all?(result, &Map.has_key?(&1, "name"))
        assert Enum.all?(result, &Map.has_key?(&1, "rank"))

        # Write and read back
        graph
        |> Dux.Graph.pagerank(iterations: 5)
        |> Dux.to_parquet(path)

        readback = Dux.from_parquet(path) |> Dux.n_rows()
        assert readback == 3
      after
        File.rm(path)
      end
    end

    test "degree analysis → join → filter high-degree nodes" do
      vertices = Dux.from_list(Enum.map(1..10, &%{"id" => &1, "label" => "node_#{&1}"}))

      # Hub-and-spoke: node 1 connects to everyone
      edges =
        Dux.from_list(for dst <- 2..10, do: %{"src" => 1, "dst" => dst})

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.out_degree()
        |> Dux.join(vertices, on: :id)
        |> Dux.filter(out_degree > 3)
        |> Dux.collect()

      assert length(result) == 1
      assert hd(result)["label"] == "node_1"
      assert hd(result)["out_degree"] == 9
    end
  end

  # ---------------------------------------------------------------------------
  # Query macro integration
  # ---------------------------------------------------------------------------

  describe "macro expressions across verbs" do
    test "filter with complex boolean logic" do
      data =
        for i <- 1..50 do
          %{
            "x" => i,
            "category" => Enum.at(["A", "B", "C"], rem(i, 3)),
            "active" => rem(i, 4) != 0
          }
        end

      min_x = 10
      max_x = 40

      result =
        Dux.from_list(data)
        |> Dux.filter(x >= ^min_x and x <= ^max_x and active == true)
        |> Dux.n_rows()

      assert result > 0
      assert result < 50
    end

    test "mutate with arithmetic and function calls" do
      result =
        Dux.from_list([
          %{"price" => 100, "tax_rate" => 0.08, "name" => "widget"}
        ])
        |> Dux.mutate(
          total: price * (1 + tax_rate),
          upper_name: upper(name),
          rounded: round(price * tax_rate, 2)
        )
        |> Dux.collect()

      row = hd(result)
      assert_in_delta row["total"], 108.0, 0.01
      assert row["upper_name"] == "WIDGET"
      assert_in_delta row["rounded"], 8.0, 0.01
    end

    test "summarise with multiple aggregation functions" do
      data = for i <- 1..100, do: %{"x" => i, "g" => rem(i, 5)}

      result =
        Dux.from_list(data)
        |> Dux.group_by(:g)
        |> Dux.summarise(
          total: sum(x),
          average: avg(x),
          minimum: min(x),
          maximum: max(x),
          n: count(x)
        )
        |> Dux.sort_by(:g)
        |> Dux.collect()

      assert length(result) == 5
      assert Enum.all?(result, &(&1["n"] == 20))
    end
  end

  # ---------------------------------------------------------------------------
  # Joins integration
  # ---------------------------------------------------------------------------

  describe "join integration" do
    test "multi-table join pipeline" do
      orders =
        Dux.from_list([
          %{"order_id" => 1, "customer_id" => 10, "product_id" => 100, "qty" => 5},
          %{"order_id" => 2, "customer_id" => 10, "product_id" => 101, "qty" => 3},
          %{"order_id" => 3, "customer_id" => 11, "product_id" => 100, "qty" => 2}
        ])

      customers =
        Dux.from_list([
          %{"customer_id" => 10, "name" => "Alice"},
          %{"customer_id" => 11, "name" => "Bob"}
        ])

      products =
        Dux.from_list([
          %{"product_id" => 100, "product_name" => "Widget", "unit_price" => 25},
          %{"product_id" => 101, "product_name" => "Gadget", "unit_price" => 50}
        ])

      result =
        orders
        |> Dux.join(customers, on: :customer_id)
        |> Dux.join(products, on: :product_id)
        |> Dux.mutate(total: qty * unit_price)
        |> Dux.group_by(:name)
        |> Dux.summarise(spend: sum(total), orders: count(order_id))
        |> Dux.sort_by(:name)
        |> Dux.collect()

      assert [
               %{"name" => "Alice", "orders" => 2},
               %{"name" => "Bob", "orders" => 1}
             ] = result

      alice = Enum.find(result, &(&1["name"] == "Alice"))
      assert alice["spend"] == 5 * 25 + 3 * 50
    end

    test "left join preserving all left rows" do
      all_users =
        Dux.from_list([
          %{"id" => 1, "name" => "Active"},
          %{"id" => 2, "name" => "Inactive"},
          %{"id" => 3, "name" => "New"}
        ])

      recent_orders =
        Dux.from_list([
          %{"user_id" => 1, "amount" => 100}
        ])

      result =
        all_users
        |> Dux.join(recent_orders, on: [{:id, :user_id}], how: :left)
        |> Dux.sort_by(:id)
        |> Dux.collect()

      assert length(result) == 3
      assert Enum.at(result, 0)["amount"] == 100
      assert Enum.at(result, 1)["amount"] == nil
      assert Enum.at(result, 2)["amount"] == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Stress / wicked
  # ---------------------------------------------------------------------------

  describe "stress" do
    test "1000 rows through full pipeline" do
      data = for i <- 1..1000, do: %{"x" => i, "g" => rem(i, 10)}

      result =
        Dux.from_list(data)
        |> Dux.filter(x > 100)
        |> Dux.mutate(doubled: x * 2)
        |> Dux.group_by(:g)
        |> Dux.summarise(total: sum(doubled), n: count(x))
        |> Dux.sort_by(:g)
        |> Dux.collect()

      assert length(result) == 10
      total_n = Enum.sum(Enum.map(result, & &1["n"]))
      assert total_n == 900
    end

    test "deeply composed pipeline with 15 operations" do
      result =
        Dux.from_query("SELECT x, x % 5 AS g FROM range(1, 101) t(x)")
        |> Dux.filter(x > 10)
        |> Dux.mutate(doubled: x * 2)
        |> Dux.filter(doubled < 180)
        |> Dux.mutate(tripled: x * 3)
        |> Dux.select([:x, :g, :doubled, :tripled])
        |> Dux.sort_by(:x)
        |> Dux.distinct([:g])
        |> Dux.sort_by(:g)
        |> Dux.head(3)
        |> Dux.mutate(final: doubled + tripled)
        |> Dux.discard([:tripled])
        |> Dux.rename(final: :result)
        |> Dux.sort_by(:x)
        |> Dux.collect()

      assert length(result) <= 3
      assert Enum.all?(result, &Map.has_key?(&1, "result"))
      refute Enum.any?(result, &Map.has_key?(&1, "tripled"))
    end
  end
end
