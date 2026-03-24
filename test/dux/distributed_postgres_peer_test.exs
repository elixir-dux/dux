defmodule Dux.DistributedPostgresPeerTest do
  use ExUnit.Case, async: false
  import Testcontainers.ExUnit
  require Dux

  alias Dux.Remote.Worker

  @moduletag :distributed
  @moduletag :container
  @moduletag timeout: 120_000

  container(:postgres, Testcontainers.PostgresContainer.new(), shared: true)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp pg_conn_string(%{postgres: container}) do
    params = Testcontainers.PostgresContainer.connection_parameters(container)

    "host=#{params[:hostname]} port=#{params[:port]} " <>
      "user=#{params[:username]} password=#{params[:password]} dbname=#{params[:database]}"
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

  setup context do
    conn_string = pg_conn_string(context)
    conn = Dux.Connection.get_conn()

    # Load postgres extension on coordinator
    Adbc.Connection.query!(conn, "INSTALL postgres; LOAD postgres;")

    # Seed test data
    seed_distributed_test_data(conn, conn_string)

    on_exit(fn ->
      try do
        Dux.detach(:dpg)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, %{conn_string: conn_string}}
  end

  defp seed_distributed_test_data(conn, conn_string) do
    Adbc.Connection.query!(conn, "ATTACH '#{conn_string}' AS __seed (TYPE postgres)")

    already_seeded =
      try do
        Adbc.Connection.query!(conn, "SELECT 1 FROM __seed.public.dist_orders LIMIT 1")
        true
      rescue
        _ -> false
      end

    unless already_seeded do
      Adbc.Connection.query!(conn, """
      CREATE TABLE __seed.public.dist_orders (
        id INTEGER PRIMARY KEY,
        region VARCHAR,
        amount DECIMAL(10,2)
      )
      """)

      # Insert 100 orders across 3 regions
      values =
        for i <- 1..100 do
          region = Enum.at(["US", "EU", "APAC"], rem(i, 3))
          "(#{i}, '#{region}', #{i * 10}.00)"
        end

      Adbc.Connection.query!(conn, """
      INSERT INTO __seed.public.dist_orders VALUES #{Enum.join(values, ", ")}
      """)
    end

    Adbc.Connection.query!(conn, "DETACH __seed")
  end

  # ---------------------------------------------------------------------------
  # Distributed reads from Postgres
  # ---------------------------------------------------------------------------

  describe "distributed Postgres reads with partition_by" do
    test "hash-partitioned read produces correct total", %{conn_string: cs} do
      Dux.attach(:dpg, cs, type: :postgres, read_only: true)

      {peer1, node1} = start_peer(:dpg_read1)
      {peer2, node2} = start_peer(:dpg_read2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        result =
          Dux.from_attached(:dpg, "public.dist_orders", partition_by: :id)
          |> Dux.distribute([w1, w2])
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
          |> Dux.to_rows()

        row = hd(result)
        # 100 orders, amounts 10..1000, sum = 10 * (1+2+...+100) = 10 * 5050 = 50500
        assert row["n"] == 100
        assert_in_delta row["total"], 50_500.0, 0.01
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "distributed read matches local read", %{conn_string: cs} do
      Dux.attach(:dpg, cs, type: :postgres, read_only: true)

      {peer1, node1} = start_peer(:dpg_match1)
      {peer2, node2} = start_peer(:dpg_match2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        local =
          Dux.from_attached(:dpg, "public.dist_orders")
          |> Dux.filter_with("region = 'US'")
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
          |> Dux.to_rows()

        distributed =
          Dux.from_attached(:dpg, "public.dist_orders", partition_by: :id)
          |> Dux.distribute([w1, w2])
          |> Dux.filter_with("region = 'US'")
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
          |> Dux.to_rows()

        assert hd(local)["n"] == hd(distributed)["n"]
        assert_in_delta hd(local)["total"], hd(distributed)["total"], 0.01
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end

    test "group_by on distributed Postgres read", %{conn_string: cs} do
      Dux.attach(:dpg, cs, type: :postgres, read_only: true)

      {peer1, node1} = start_peer(:dpg_grp1)
      {peer2, node2} = start_peer(:dpg_grp2)
      {peer3, node3} = start_peer(:dpg_grp3)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        {:ok, w3} = start_worker_on(node3)
        Process.sleep(200)

        local =
          Dux.from_attached(:dpg, "public.dist_orders")
          |> Dux.group_by(:region)
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        distributed =
          Dux.from_attached(:dpg, "public.dist_orders", partition_by: :id)
          |> Dux.distribute([w1, w2, w3])
          |> Dux.group_by(:region)
          |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        # Same number of groups
        assert length(local) == length(distributed)

        # Same values per group
        Enum.zip(local, distributed)
        |> Enum.each(fn {l, d} ->
          assert l["region"] == d["region"]
          assert l["n"] == d["n"]
          assert_in_delta l["total"], d["total"], 0.01
        end)
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
        :peer.stop(peer3)
      end
    end

    test "without partition_by reads locally (not distributed)", %{conn_string: cs} do
      Dux.attach(:dpg, cs, type: :postgres, read_only: true)

      # Without partition_by, attached sources read on the coordinator only
      result =
        Dux.from_attached(:dpg, "public.dist_orders")
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.to_rows()

      row = hd(result)
      assert row["n"] == 100
    end
  end
end
