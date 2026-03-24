if Code.ensure_loaded?(FLAME) do
  defmodule Dux.Flame do
    @moduledoc """
    Spin up Dux workers on FLAME runners for distributed queries.

    FLAME handles pool management and machine lifecycle — see the
    [FLAME docs](https://hexdocs.pm/flame) for pool configuration.
    This module provides `spin_up/2` to place `Dux.Remote.Worker`
    processes on FLAME runners, and `status/1` to inspect the cluster.

    ## Livebook

        # 1. Start a FLAME pool (see FLAME docs for backend options)
        Kino.start_child!(
          {FLAME.Pool,
            name: :dux_pool,
            code_sync: [
              start_apps: true,
              sync_beams: [Path.join(System.tmp_dir!(), "livebook_runtime")]
            ],
            min: 0,
            max: 10,
            backend: {FLAME.FlyBackend,
              cpu_kind: "performance", cpus: 4, memory_mb: 8192,
              token: System.fetch_env!("FLY_API_TOKEN"),
              env: Map.take(System.get_env(), ["LIVEBOOK_COOKIE"])
            },
            idle_shutdown_after: :timer.minutes(5)}
        )

        # 2. Spin up workers and distribute
        workers = Dux.Flame.spin_up(5, pool: :dux_pool)

        Dux.from_parquet("s3://bucket/data/**/*.parquet")
        |> Dux.distribute(workers)
        |> Dux.filter(amount > 100)
        |> Dux.group_by(:region)
        |> Dux.summarise(total: sum(amount))
        |> Dux.compute()

    ## Deployed app

        # In your application supervisor children:
        {FLAME.Pool,
          name: :dux_pool,
          backend: {FLAME.FlyBackend, ...},
          max: 10,
          code_sync: [start_apps: [:dux], copy_apps: true],
          idle_shutdown_after: :timer.minutes(5)}

        # Then at runtime:
        workers = Dux.Flame.spin_up(5, pool: :dux_pool)

    Workers read S3 data directly — nothing flows through your machine.
    After idle timeout, FLAME auto-terminates the runners.
    """

    @default_pool Dux.FlamePool

    @doc """
    Spin up `n` Dux workers on FLAME runners.

    Each worker is placed on a separate FLAME runner via `FLAME.place_child/3`.
    Returns a list of worker PIDs suitable for `Dux.distribute/2`.

    ## Options

      * `:pool` — FLAME pool name (default: `Dux.FlamePool`)
    """
    def spin_up(n, opts \\ []) when is_integer(n) and n > 0 do
      pool = Keyword.get(opts, :pool, @default_pool)

      workers =
        for _ <- 1..n do
          {:ok, pid} = FLAME.place_child(pool, {Dux.Remote.Worker, []})
          pid
        end

      # Wait for :pg registration to propagate
      Process.sleep(100)
      workers
    end

    @doc """
    Get status of the FLAME-backed Dux cluster.

    Returns worker count and PIDs, grouped by node.
    """
    alias Dux.Remote.Worker

    def status(pool \\ @default_pool) do
      workers = Worker.list()

      nodes =
        workers
        |> Enum.group_by(&node/1)
        |> Enum.map(fn {node, pids} -> {node, length(pids)} end)
        |> Map.new()

      %{
        pool: pool,
        total_workers: length(workers),
        nodes: nodes
      }
    end
  end
end
