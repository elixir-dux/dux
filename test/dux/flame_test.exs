if Code.ensure_loaded?(FLAME) do
  defmodule Dux.FlameTest do
    use ExUnit.Case, async: false
    require Dux
    alias Dux.Remote.Worker

    @moduletag timeout: 120_000

    setup do
      pool_name = :"flame_test_#{System.unique_integer([:positive])}"

      {:ok, _} =
        DynamicSupervisor.start_child(
          Dux.DynamicSupervisor,
          {FLAME.Pool,
           name: pool_name,
           backend: FLAME.LocalBackend,
           min: 0,
           max: 5,
           idle_shutdown_after: :timer.seconds(30),
           boot_timeout: 30_000}
        )

      %{pool: pool_name}
    end

    describe "spin_up + basic operations" do
      test "spins up workers that are alive and in :pg", %{pool: pool} do
        workers = Dux.Flame.spin_up(2, pool: pool)

        assert length(workers) == 2
        assert Enum.all?(workers, &is_pid/1)
        assert Enum.all?(workers, &Process.alive?/1)

        all = Worker.list()
        assert Enum.all?(workers, &(&1 in all))
      end

      test "workers execute pipelines", %{pool: pool} do
        [worker] = Dux.Flame.spin_up(1, pool: pool)

        pipeline = Dux.from_query("SELECT 42 AS answer")
        {:ok, ipc} = Worker.execute(worker, pipeline)
        conn = Dux.Connection.get_conn()
        ref = Dux.Backend.table_from_ipc(conn, ipc)
        cols = Dux.Backend.table_to_columns(conn, ref)
        assert cols["answer"] == [42]
      end
    end

    describe "distribute with FLAME workers" do
      test "full pipeline: spin up → distribute → compute", %{pool: pool} do
        workers = Dux.Flame.spin_up(2, pool: pool)

        result =
          Dux.from_list([%{x: 1}, %{x: 2}, %{x: 3}])
          |> Dux.distribute(workers)
          |> Dux.summarise_with(total: "SUM(x)")
          |> Dux.to_rows()

        # Replicated source: each worker sums 6, merger re-aggregates: 12
        assert hd(result)["total"] == 12
      end

      test "group_by + summarise across FLAME workers", %{pool: pool} do
        workers = Dux.Flame.spin_up(2, pool: pool)

        result =
          Dux.from_list([
            %{region: "US", amount: 100},
            %{region: "EU", amount: 200},
            %{region: "US", amount: 150}
          ])
          |> Dux.distribute(workers)
          |> Dux.group_by(:region)
          |> Dux.summarise_with(total: "SUM(amount)")
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        assert length(result) == 2
        eu = Enum.find(result, &(&1["region"] == "EU"))
        us = Enum.find(result, &(&1["region"] == "US"))
        assert eu["total"] == 400
        assert us["total"] == 500
      end
    end

    describe "status" do
      test "returns cluster info", %{pool: pool} do
        _workers = Dux.Flame.spin_up(2, pool: pool)
        status = Dux.Flame.status(pool)

        assert status.total_workers >= 2
        assert is_map(status.nodes)
      end
    end
  end
end
