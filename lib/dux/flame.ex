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
            max_concurrency: 1,
            backend: {FLAME.FlyBackend,
              cpu_kind: "performance", cpus: 4, memory_mb: 8192,
              token: System.fetch_env!("FLY_API_TOKEN"),
              env: %{"LIVEBOOK_COOKIE" => Atom.to_string(Node.get_cookie())}
            },
            boot_timeout: 120_000,
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
          max_concurrency: 1,
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
      * `:setup` — callback function to run on each worker after startup
      * `:memory_limit` — DuckDB memory limit per worker (e.g. `"2GB"`)
      * `:temp_directory` — spill-to-disk directory (default: system temp)
    """
    def spin_up(n, opts \\ []) when is_integer(n) and n > 0 do
      pool = Keyword.get(opts, :pool, @default_pool)
      setup = Keyword.get(opts, :setup)
      worker_opts = Keyword.take(opts, [:memory_limit, :temp_directory, :path])

      # Place workers sequentially. With max_concurrency: 1, each placed
      # child holds its concurrency slot permanently, so the next
      # place_child boots a new runner. Sequential avoids internal FLAME
      # GenServer timeouts that occur with concurrent placement.
      workers =
        for _ <- 1..n do
          {:ok, pid} = FLAME.place_child(pool, {Dux.Remote.Worker, worker_opts})
          pid
        end

      # Run setup callback on each worker (e.g. create S3 secrets, load extensions)
      if setup do
        workers
        |> Task.async_stream(
          fn worker -> GenServer.call(worker, {:setup, setup}, 30_000) end,
          timeout: 60_000
        )
        |> Enum.each(fn
          {:ok, :ok} -> :ok
          {:ok, {:error, reason}} -> raise "Worker setup failed: #{reason}"
          {:exit, reason} -> raise "Worker setup crashed: #{inspect(reason)}"
        end)
      end

      workers
    end

    @doc """
    Get status of the Dux worker cluster.

    Pass the workers list returned by `spin_up/2`.

        workers = Dux.Flame.spin_up(3, pool: :dux_pool)
        Dux.Flame.status(workers)
        # => %{total_workers: 3, nodes: %{:"flame-abc@..." => 1, ...}, ...}

    Returns worker count grouped by node.
    """
    def status(workers) when is_list(workers) do
      nodes =
        workers
        |> Enum.group_by(&node/1)
        |> Map.new(fn {n, pids} -> {n, length(pids)} end)

      %{
        total_workers: length(workers),
        nodes: nodes,
        worker_pids: workers
      }
    end
  end
end
