defmodule Dux.DistributedInsertPeerTest do
  use ExUnit.Case, async: false
  import Testcontainers.ExUnit
  require Dux

  alias Dux.Remote.Worker

  @moduletag :distributed
  @moduletag :container
  @moduletag timeout: 120_000

  @tmp_dir System.tmp_dir!()

  container(:postgres, Testcontainers.PostgresContainer.new(), shared: true)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp pg_conn_string(%{postgres: container}) do
    params = Testcontainers.PostgresContainer.connection_parameters(container)

    "host=#{params[:hostname]} port=#{params[:port]} " <>
      "user=#{params[:username]} password=#{params[:password]} dbname=#{params[:database]}"
  end

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_di_peer_#{System.unique_integer([:positive])}_#{name}")
  end

  defp start_peer(name) do
    unless Node.alive?() do
      raise "distributed tests require a named node — see test_helper.exs"
    end

    pa_args =
      :code.get_path()
      |> Enum.flat_map(fn path -> [~c"-pa", path] end)

    {:ok, peer, node} = :peer.start(%{name: name, args: pa_args})
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:dux])
    {peer, node}
  end

  defp start_worker_on(node) do
    :erpc.call(node, DynamicSupervisor, :start_child, [
      Dux.DynamicSupervisor,
      %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
    ])
  end

  defp pg_query!(conn_string, sql) do
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, "INSTALL postgres; LOAD postgres;")
    alias_name = "__tmp_#{:erlang.unique_integer([:positive])}"
    Adbc.Connection.query!(conn, "ATTACH '#{conn_string}' AS #{alias_name} (TYPE postgres)")
    result = Adbc.Connection.query!(conn, sql |> String.replace("__pg__", alias_name))
    Adbc.Connection.query!(conn, "DETACH #{alias_name}")
    result
  end

  defp pg_count(conn_string, table) do
    conn = Dux.Connection.get_conn()
    alias_name = "__cnt_#{:erlang.unique_integer([:positive])}"
    Adbc.Connection.query!(conn, "ATTACH '#{conn_string}' AS #{alias_name} (TYPE postgres)")

    ref =
      Dux.Backend.query(
        conn,
        "SELECT COUNT(*) AS n FROM #{alias_name}.#{table}"
      )

    rows = Dux.Backend.table_to_rows(conn, ref)
    Adbc.Connection.query!(conn, "DETACH #{alias_name}")
    hd(rows)["n"]
  end

  setup context do
    conn_string = pg_conn_string(context)
    conn = Dux.Connection.get_conn()
    Adbc.Connection.query!(conn, "INSTALL postgres; LOAD postgres;")

    on_exit(fn ->
      try do
        Dux.detach(:dipg)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, %{conn_string: conn_string}}
  end

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "distributed insert_into Postgres" do
    test "create: true creates table and inserts partitioned data", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_create1)
      {peer2, node2} = start_peer(:di_create2)

      try do
        input_dir = tmp_path("di_create_input")
        File.mkdir_p!(input_dir)

        for i <- 1..4 do
          rows = for j <- 1..25, do: %{"id" => (i - 1) * 25 + j, "value" => j * 10}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.insert_into("dipg.public.di_test_create", create: true)

        # Verify data in Postgres
        count = pg_count(cs, "public.di_test_create")
        assert count == 100
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_create")
      end
    end

    test "insert into existing table appends rows", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_append1)
      {peer2, node2} = start_peer(:di_append2)

      try do
        # Create table with initial data
        pg_query!(cs, """
        CREATE TABLE __pg__.public.di_test_append (id INTEGER, value INTEGER)
        """)

        pg_query!(cs, """
        INSERT INTO __pg__.public.di_test_append VALUES (0, 0)
        """)

        input_dir = tmp_path("di_append_input")
        File.mkdir_p!(input_dir)

        for i <- 1..2 do
          rows = for j <- 1..50, do: %{"id" => (i - 1) * 50 + j, "value" => j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.insert_into("dipg.public.di_test_append")

        # 1 initial + 100 new = 101
        count = pg_count(cs, "public.di_test_append")
        assert count == 101
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_append")
      end
    end

    test "distributed insert matches local insert", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_match1)
      {peer2, node2} = start_peer(:di_match2)

      try do
        input_dir = tmp_path("di_match_input")
        File.mkdir_p!(input_dir)

        for i <- 1..4 do
          rows =
            for j <- 1..25 do
              %{
                "id" => (i - 1) * 25 + j,
                "region" => Enum.at(["US", "EU", "APAC"], rem(j, 3)),
                "amount" => j * 10
              }
            end

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        # Local insert
        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.filter_with("amount > 100")
        |> Dux.insert_into("dipg.public.di_test_local", create: true)

        # Distributed insert
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.filter_with("amount > 100")
        |> Dux.insert_into("dipg.public.di_test_dist", create: true)

        local_count = pg_count(cs, "public.di_test_local")
        dist_count = pg_count(cs, "public.di_test_dist")

        assert local_count == dist_count
        assert local_count > 0
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_local")
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_dist")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Scale
  # ---------------------------------------------------------------------------

  describe "distributed insert_into at scale" do
    test "1000 rows across 3 workers into Postgres", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_scale1)
      {peer2, node2} = start_peer(:di_scale2)
      {peer3, node3} = start_peer(:di_scale3)

      try do
        input_dir = tmp_path("di_scale_input")
        File.mkdir_p!(input_dir)

        for i <- 1..10 do
          rows = for j <- 1..100, do: %{"id" => (i - 1) * 100 + j, "val" => j}

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        {:ok, w3} = start_worker_on(node3)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2, w3])
        |> Dux.insert_into("dipg.public.di_test_scale", create: true)

        count = pg_count(cs, "public.di_test_scale")
        assert count == 1000
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        :peer.stop(peer3)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_scale")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "insert into non-existent table without create raises", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_sad1)
      {peer2, node2} = start_peer(:di_sad2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        assert_raise ArgumentError, ~r/all workers failed/, fn ->
          Dux.from_query("SELECT 1 AS x")
          |> Dux.distribute([w1, w2])
          |> Dux.insert_into("dipg.public.this_table_does_not_exist_at_all")
        end
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "insert with mismatched column schema raises", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_sad_schema1)
      {peer2, node2} = start_peer(:di_sad_schema2)

      try do
        # Create a table with specific columns
        pg_query!(cs, """
        CREATE TABLE __pg__.public.di_test_schema (id INTEGER, name VARCHAR)
        """)

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Insert data with wrong column names — should fail
        assert_raise ArgumentError, ~r/all workers failed/, fn ->
          Dux.from_list([%{"wrong_col" => 1, "also_wrong" => "x"}])
          |> Dux.distribute([w1, w2])
          |> Dux.insert_into("dipg.public.di_test_schema")
        end
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_schema")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "inserts data with nulls, quotes, and newlines", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_adv1)
      {peer2, node2} = start_peer(:di_adv2)

      try do
        input_dir = tmp_path("di_adv_input")
        File.mkdir_p!(input_dir)

        Dux.from_list([
          %{"id" => 1, "name" => "it's a test", "value" => 100},
          %{"id" => 2, "name" => nil, "value" => 200},
          %{"id" => 3, "name" => "line1\nline2", "value" => 300},
          %{"id" => 4, "name" => "has \"quotes\"", "value" => 400}
        ])
        |> Dux.to_parquet(Path.join(input_dir, "part_1.parquet"))

        Dux.from_list([
          %{"id" => 5, "name" => "normal", "value" => 500},
          %{"id" => 6, "name" => "", "value" => 600}
        ])
        |> Dux.to_parquet(Path.join(input_dir, "part_2.parquet"))

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2])
        |> Dux.insert_into("dipg.public.di_test_adversarial", create: true)

        count = pg_count(cs, "public.di_test_adversarial")
        assert count == 6

        # Read back and verify special values survived
        result =
          Dux.from_attached(:dipg, "public.di_test_adversarial")
          |> Dux.sort_by(:id)
          |> Dux.to_rows()

        assert Enum.find(result, &(&1["id"] == 1))["name"] == "it's a test"
        assert Enum.find(result, &(&1["id"] == 2))["name"] == nil
        assert Enum.find(result, &(&1["id"] == 3))["name"] == "line1\nline2"
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_adversarial")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked (multi-step)
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "distributed read from Postgres → filter → distributed insert back to Postgres",
         %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_wicked1)
      {peer2, node2} = start_peer(:di_wicked2)

      try do
        # Seed source table
        pg_query!(cs, """
        CREATE TABLE __pg__.public.di_test_source (id INTEGER, category VARCHAR, amount INTEGER)
        """)

        values =
          for i <- 1..200 do
            cat = Enum.at(["A", "B", "C"], rem(i, 3))
            "(#{i}, '#{cat}', #{i * 10})"
          end

        pg_query!(cs, """
        INSERT INTO __pg__.public.di_test_source VALUES #{Enum.join(values, ", ")}
        """)

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        # Distributed read → filter → distributed insert
        Dux.from_attached(:dipg, "public.di_test_source", partition_by: :id)
        |> Dux.distribute([w1, w2])
        |> Dux.filter_with("category = 'A'")
        |> Dux.insert_into("dipg.public.di_test_dest", create: true)

        # Verify: only category A rows, with correct count
        dest_count = pg_count(cs, "public.di_test_dest")

        # Category A: ids where rem(i, 3) == 0, which is 66 or 67 out of 200
        source_a_count =
          Dux.from_attached(:dipg, "public.di_test_source")
          |> Dux.filter_with("category = 'A'")
          |> Dux.summarise_with(n: "COUNT(*)")
          |> Dux.to_rows()
          |> hd()
          |> Map.get("n")

        assert dest_count == source_a_count
        assert dest_count > 0
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_source")
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_dest")
      end
    end

    test "1000-row insert with SUM verification", %{conn_string: cs} do
      Dux.attach(:dipg, cs, type: :postgres, read_only: false)

      {peer1, node1} = start_peer(:di_wk_sum1)
      {peer2, node2} = start_peer(:di_wk_sum2)
      {peer3, node3} = start_peer(:di_wk_sum3)

      try do
        input_dir = tmp_path("di_wk_sum_input")
        File.mkdir_p!(input_dir)

        for i <- 1..10 do
          rows =
            for j <- 1..100 do
              %{"id" => (i - 1) * 100 + j, "value" => (i - 1) * 100 + j}
            end

          Dux.from_list(rows)
          |> Dux.to_parquet(Path.join(input_dir, "part_#{i}.parquet"))
        end

        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        {:ok, w3} = start_worker_on(node3)
        Process.sleep(200)

        Dux.from_parquet(Path.join(input_dir, "*.parquet"))
        |> Dux.distribute([w1, w2, w3])
        |> Dux.insert_into("dipg.public.di_test_sum", create: true)

        # Read back and verify SUM
        result =
          Dux.from_attached(:dipg, "public.di_test_sum")
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(value)")
          |> Dux.to_rows()

        row = hd(result)
        assert row["n"] == 1000
        assert row["total"] == div(1000 * 1001, 2)
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        :peer.stop(peer3)
        pg_query!(cs, "DROP TABLE IF EXISTS __pg__.public.di_test_sum")
      end
    end
  end
end
