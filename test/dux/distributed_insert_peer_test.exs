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
  end
end
