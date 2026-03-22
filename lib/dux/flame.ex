if Code.ensure_loaded?(FLAME) do
  defmodule Dux.Flame do
    @moduledoc """
    Elastic compute via FLAME for distributed Dux queries.

    FLAME boots ephemeral cloud machines with a full copy of your application,
    starts `Dux.Remote.Worker` on each, and auto-terminates when idle. No Docker
    builds, no cluster management.

    ## Quick start

        # Start a FLAME pool (e.g. in Livebook)
        Dux.Flame.start_pool(
          backend: {FLAME.FlyBackend,
            token: System.fetch_env!("FLY_API_TOKEN"),
            cpus: 4,
            memory_mb: 16_384},
          max: 10
        )

        # Spin up workers and distribute
        Dux.from_parquet("s3://bucket/data/**/*.parquet")
        |> Dux.distribute(Dux.Flame.spin_up(5))
        |> Dux.filter(amount > 100)
        |> Dux.group_by(:region)
        |> Dux.summarise(total: sum(amount))
        |> Dux.to_rows()

    Workers read S3 data directly — nothing flows through your machine.
    After 5 minutes idle (configurable), machines auto-terminate.

    ## Pools

    You can run multiple pools for different workloads:

        Dux.Flame.start_pool(name: Dux.CpuPool, backend: cpu_backend, max: 10)
        Dux.Flame.start_pool(name: Dux.GpuPool, backend: gpu_backend, max: 4)

        cpu_workers = Dux.Flame.spin_up(5, pool: Dux.CpuPool)
        gpu_workers = Dux.Flame.spin_up(2, pool: Dux.GpuPool)
    """

    @default_pool Dux.FlamePool

    @doc """
    Start a FLAME pool for Dux workers.

    ## Options

      * `:name` — pool name (default: `Dux.FlamePool`)
      * `:backend` — FLAME backend (required). E.g. `{FLAME.FlyBackend, token: ..., cpus: 4}`
      * `:max` — maximum number of runners (default: 10)
      * `:min` — minimum runners to keep warm (default: 0)
      * `:idle_shutdown_after` — ms before idle runner terminates (default: 5 minutes)
      * `:boot_timeout` — ms to wait for runner boot (default: 30 seconds)

    Returns `{:ok, pid}` of the pool supervisor.
    """
    def start_pool(opts \\ []) do
      backend = Keyword.fetch!(opts, :backend)

      pool_opts =
        [
          name: Keyword.get(opts, :name, @default_pool),
          max: Keyword.get(opts, :max, 10),
          min: Keyword.get(opts, :min, 0),
          max_concurrency: 1,
          idle_shutdown_after: Keyword.get(opts, :idle_shutdown_after, :timer.minutes(5)),
          boot_timeout: Keyword.get(opts, :boot_timeout, 30_000),
          backend: backend
        ] ++
          if(backend != FLAME.LocalBackend,
            do: [code_sync: [start_apps: [:dux], copy_apps: true]],
            else: []
          )

      DynamicSupervisor.start_child(
        Dux.DynamicSupervisor,
        {FLAME.Pool, pool_opts}
      )
    end

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
