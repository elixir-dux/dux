defmodule Dux.PostgresAttachTest do
  use ExUnit.Case, async: false
  import Testcontainers.ExUnit

  @moduletag :container
  @moduletag timeout: 120_000

  container(:postgres, Testcontainers.PostgresContainer.new(), shared: true)

  defp pg_conn_string(%{postgres: container}) do
    params = Testcontainers.PostgresContainer.connection_parameters(container)

    "host=#{params[:hostname]} port=#{params[:port]} " <>
      "user=#{params[:username]} password=#{params[:password]} dbname=#{params[:database]}"
  end

  setup context do
    conn_string = pg_conn_string(context)
    conn = Dux.Connection.get_conn()

    # Ensure postgres extension is loaded
    Adbc.Connection.query!(conn, "INSTALL postgres; LOAD postgres;")

    # Seed once — tables persist in the shared Postgres container
    seed_if_needed(conn, conn_string)

    on_exit(fn ->
      try do
        Dux.detach(:pg)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, %{conn_string: conn_string}}
  end

  defp seed_if_needed(conn, conn_string) do
    # Check if we already seeded by looking for the customers table
    Adbc.Connection.query!(conn, "ATTACH '#{conn_string}' AS __seed_check (TYPE postgres)")

    already_seeded =
      try do
        Adbc.Connection.query!(conn, "SELECT 1 FROM __seed_check.public.customers LIMIT 1")
        true
      rescue
        _ -> false
      end

    if already_seeded do
      Adbc.Connection.query!(conn, "DETACH __seed_check")
    else
      # Seed the data
      Adbc.Connection.query!(conn, """
      CREATE TABLE __seed_check.public.customers (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        region VARCHAR,
        created_at TIMESTAMP
      )
      """)

      Adbc.Connection.query!(conn, """
      INSERT INTO __seed_check.public.customers VALUES
        (1, 'Alice', 'US', '2026-01-15 10:00:00'),
        (2, 'Bob', 'EU', '2026-02-20 14:30:00'),
        (3, 'Carol', 'US', '2026-03-01 09:00:00'),
        (4, 'Dave', 'APAC', '2026-01-10 08:00:00'),
        (5, 'Eve', 'EU', '2026-03-15 16:00:00')
      """)

      Adbc.Connection.query!(conn, """
      CREATE TABLE __seed_check.public.orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        amount DECIMAL(10,2),
        status VARCHAR
      )
      """)

      Adbc.Connection.query!(conn, """
      INSERT INTO __seed_check.public.orders VALUES
        (1, 1, 100.50, 'active'),
        (2, 1, 200.00, 'completed'),
        (3, 2, 50.75, 'active'),
        (4, 3, 300.00, 'active'),
        (5, 4, 150.25, 'cancelled'),
        (6, 5, 75.00, 'active')
      """)

      Adbc.Connection.query!(conn, "DETACH __seed_check")
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "Postgres attach and query" do
    test "basic query on attached Postgres table", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      result =
        Dux.from_attached(:pg, "public.customers")
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 5
      assert hd(result)["name"] == "Alice"
    end

    test "filter pushdown to Postgres", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      result =
        Dux.from_attached(:pg, "public.customers")
        |> Dux.filter_with("region = 'US'")
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.all?(result, &(&1["region"] == "US"))
    end

    test "timestamp filter pushes down", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      result =
        Dux.from_attached(:pg, "public.customers")
        |> Dux.filter_with("created_at >= '2026-03-01'")
        |> Dux.to_rows()

      assert length(result) == 2
      names = Enum.map(result, & &1["name"]) |> Enum.sort()
      assert names == ["Carol", "Eve"]
    end

    test "aggregation with filter on attached Postgres", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      result =
        Dux.from_attached(:pg, "public.orders")
        |> Dux.filter_with("status = 'active'")
        |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
        |> Dux.to_rows()

      row = hd(result)
      assert row["n"] == 4
      assert_in_delta row["total"], 526.25, 0.01
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-source joins
  # ---------------------------------------------------------------------------

  describe "cross-source joins with Postgres" do
    test "local data joined with Postgres dimension", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      facts =
        Dux.from_list([
          %{customer_id: 1, revenue: 500},
          %{customer_id: 2, revenue: 300},
          %{customer_id: 3, revenue: 700}
        ])

      customers = Dux.from_attached(:pg, "public.customers")

      result =
        facts
        |> Dux.join(customers, on: [{:customer_id, :id}])
        |> Dux.sort_by(:revenue)
        |> Dux.to_rows()

      assert length(result) == 3
      assert Enum.all?(result, &Map.has_key?(&1, "name"))
    end

    test "join two Postgres tables with aggregation", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      result =
        Dux.from_attached(:pg, "public.orders")
        |> Dux.join(Dux.from_attached(:pg, "public.customers"),
          on: [{:customer_id, :id}]
        )
        |> Dux.group_by(:region)
        |> Dux.summarise_with(order_count: "COUNT(*)")
        |> Dux.sort_by(:region)
        |> Dux.to_rows()

      assert length(result) == 3
      us = Enum.find(result, &(&1["region"] == "US"))
      assert us["order_count"] == 3
    end
  end

  # ---------------------------------------------------------------------------
  # Full pipeline
  # ---------------------------------------------------------------------------

  describe "full pipeline on attached Postgres" do
    test "filter → join → group → summarise → sort", %{conn_string: cs} do
      Dux.attach(:pg, cs, type: :postgres, read_only: true)

      result =
        Dux.from_attached(:pg, "public.orders")
        |> Dux.filter_with("status = 'active'")
        |> Dux.join(Dux.from_attached(:pg, "public.customers"),
          on: [{:customer_id, :id}]
        )
        |> Dux.group_by(:region)
        |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
        |> Dux.sort_by(desc: :total)
        |> Dux.to_rows()

      assert result != []
      assert Enum.all?(result, &(&1["n"] > 0))
    end
  end
end
