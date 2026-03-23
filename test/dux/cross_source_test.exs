defmodule Dux.CrossSourceTest do
  use ExUnit.Case, async: false

  # SQLite is the easiest attached DB to test with — no external services needed.
  # DuckDB auto-installs the sqlite extension.

  setup do
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, "INSTALL sqlite; LOAD sqlite;")

    tmp = Path.join(System.tmp_dir!(), "dux_cross_test_#{System.unique_integer([:positive])}.db")

    # Seed the SQLite database with test data
    Adbc.Connection.query!(conn, "ATTACH '#{tmp}' AS __setup_db (TYPE SQLITE)")

    Adbc.Connection.query!(conn, """
    CREATE TABLE __setup_db.customers (id INTEGER, name VARCHAR, region VARCHAR)
    """)

    Adbc.Connection.query!(conn, """
    INSERT INTO __setup_db.customers VALUES
      (1, 'Alice', 'US'), (2, 'Bob', 'EU'), (3, 'Carol', 'US'),
      (4, 'Dave', 'APAC'), (5, 'Eve', 'EU')
    """)

    Adbc.Connection.query!(conn, """
    CREATE TABLE __setup_db.regions (region VARCHAR, full_name VARCHAR)
    """)

    Adbc.Connection.query!(conn, """
    INSERT INTO __setup_db.regions VALUES
      ('US', 'United States'), ('EU', 'Europe'), ('APAC', 'Asia Pacific')
    """)

    Adbc.Connection.query!(conn, "DETACH __setup_db")

    on_exit(fn ->
      try do
        Dux.detach(:testdb)
      catch
        _, _ -> :ok
      end

      File.rm(tmp)
    end)

    %{tmp: tmp}
  end

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "attach and query" do
    test "attach SQLite and query a table", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      result =
        Dux.from_attached(:testdb, "customers")
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 5
      assert hd(result)["name"] == "Alice"
    end

    test "filter pushes down to attached source", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      result =
        Dux.from_attached(:testdb, "customers")
        |> Dux.filter_with("region = 'US'")
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 2
      names = Enum.map(result, & &1["name"])
      assert names == ["Alice", "Carol"]
    end

    test "cross-source join: local data + attached DB", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      orders =
        Dux.from_list([
          %{customer_id: 1, amount: 100},
          %{customer_id: 2, amount: 200},
          %{customer_id: 1, amount: 150}
        ])

      customers = Dux.from_attached(:testdb, "customers")

      result =
        orders
        |> Dux.join(customers, on: [{:customer_id, :id}])
        |> Dux.sort_by(:amount)
        |> Dux.to_rows()

      assert length(result) == 3
      assert Enum.all?(result, &Map.has_key?(&1, "name"))
    end

    test "join two attached tables", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      customers = Dux.from_attached(:testdb, "customers")
      regions = Dux.from_attached(:testdb, "regions")

      result =
        customers
        |> Dux.join(regions, on: :region)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      assert length(result) == 5
      assert Enum.all?(result, &Map.has_key?(&1, "full_name"))
    end

    test "aggregation on attached source", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      result =
        Dux.from_attached(:testdb, "customers")
        |> Dux.group_by(:region)
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.sort_by(:region)
        |> Dux.to_rows()

      assert length(result) == 3
      us = Enum.find(result, &(&1["region"] == "US"))
      assert us["n"] == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Lifecycle
  # ---------------------------------------------------------------------------

  describe "attach/detach/list" do
    test "list_attached includes the attached database", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)
      dbs = Dux.list_attached()
      names = Enum.map(dbs, & &1["name"])
      assert "testdb" in names
    end

    test "detach removes the database", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)
      Dux.detach(:testdb)
      dbs = Dux.list_attached()
      names = Enum.map(dbs, & &1["name"])
      refute "testdb" in names
    end

    test "read_only prevents writes", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite, read_only: true)

      assert_raise Adbc.Error, fn ->
        conn = Dux.Connection.get_conn()
        Adbc.Connection.query!(conn, "INSERT INTO testdb.customers VALUES (99, 'Test', 'XX')")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # from_attached options
  # ---------------------------------------------------------------------------

  describe "from_attached" do
    test "creates a lazy Dux struct" do
      df = Dux.from_attached(:mydb, "public.table")
      assert %Dux{} = df
      assert df.source == {:attached, :mydb, "public.table"}
      assert df.ops == []
    end

    test "version option for time travel" do
      df = Dux.from_attached(:lake, "events", version: 5)
      assert df.source == {:attached, :lake, "events", version: 5}
    end

    test "as_of option for time travel" do
      ts = ~U[2026-01-01 00:00:00Z]
      df = Dux.from_attached(:lake, "events", as_of: ts)
      assert df.source == {:attached, :lake, "events", as_of: ts}
    end
  end

  # ---------------------------------------------------------------------------
  # Secrets
  # ---------------------------------------------------------------------------

  describe "secrets management" do
    test "create and drop a secret with explicit keys" do
      Dux.create_secret(:test_s3_keys,
        type: :s3,
        key_id: "AKIATEST",
        secret: "testsecret",
        region: "us-west-2"
      )

      Dux.drop_secret(:test_s3_keys)
    end

    test "create secret with scope" do
      Dux.create_secret(:scoped_s3,
        type: :s3,
        key_id: "AKIATEST",
        secret: "testsecret",
        scope: "s3://my-bucket/"
      )

      Dux.drop_secret(:scoped_s3)
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "attach with missing type raises" do
      assert_raise KeyError, fn ->
        Dux.attach(:bad, "some_path.db", [])
      end
    end

    test "detach non-existent database raises" do
      assert_raise Adbc.Error, fn ->
        Dux.detach(:nonexistent_db)
      end
    end

    test "drop non-existent secret raises" do
      assert_raise Adbc.Error, fn ->
        Dux.drop_secret(:nonexistent_secret)
      end
    end

    test "query non-existent table in attached DB raises", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      assert_raise ArgumentError, fn ->
        Dux.from_attached(:testdb, "nonexistent_table")
        |> Dux.to_rows()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "attached source composes with full pipeline", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      result =
        Dux.from_attached(:testdb, "customers")
        |> Dux.filter_with("region IN ('US', 'EU')")
        |> Dux.mutate_with(upper_name: "UPPER(name)")
        |> Dux.group_by(:region)
        |> Dux.summarise_with(n: "COUNT(*)", names: "STRING_AGG(name, ', ')")
        |> Dux.sort_by(desc: :n)
        |> Dux.to_rows()

      assert length(result) == 2
    end

    test "re-attach same name after detach", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)
      r1 = Dux.from_attached(:testdb, "customers") |> Dux.to_rows()
      Dux.detach(:testdb)

      Dux.attach(:testdb, tmp, type: :sqlite)
      r2 = Dux.from_attached(:testdb, "customers") |> Dux.to_rows()

      assert length(r1) == length(r2)
    end

    test "cross-source join with ASOF", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      # Attached customers as reference, local time-series data
      events =
        Dux.from_list([
          %{customer_id: 1, ts: 10, event: "click"},
          %{customer_id: 2, ts: 20, event: "purchase"}
        ])

      snapshots =
        Dux.from_list([
          %{ts: 5, status: "active"},
          %{ts: 15, status: "premium"}
        ])

      # ASOF join on local data, then join with attached DB
      result =
        events
        |> Dux.asof_join(snapshots, by: {:ts, :>=})
        |> Dux.to_rows()

      assert length(result) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "three-way join: local + attached + attached", %{tmp: tmp} do
      Dux.attach(:testdb, tmp, type: :sqlite)

      orders =
        Dux.from_list([
          %{customer_id: 1, region: "US", amount: 100},
          %{customer_id: 2, region: "EU", amount: 200}
        ])

      customers = Dux.from_attached(:testdb, "customers")
      regions = Dux.from_attached(:testdb, "regions")

      result =
        orders
        |> Dux.join(customers, on: [{:customer_id, :id}])
        |> Dux.join(regions, on: :region)
        |> Dux.sort_by(:amount)
        |> Dux.to_rows()

      assert length(result) == 2
      assert Enum.all?(result, &Map.has_key?(&1, "full_name"))
      assert Enum.all?(result, &Map.has_key?(&1, "name"))
    end
  end
end
