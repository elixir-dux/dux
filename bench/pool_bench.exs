# Connection pool throughput benchmark
# Run with: mix run bench/pool_bench.exs
#
# Compares sequential vs concurrent query throughput at different pool sizes.
# Starts its own Dux.Connection (bypasses the app-level one) to control pool_size.

# Stop the app-level connection so we can start our own with different pool sizes
case Process.whereis(Dux.Connection) do
  nil -> :ok
  pid -> Supervisor.terminate_child(Dux.Supervisor, Dux.Connection)
end

IO.puts("Setting up benchmark data...\n")

# --- Test each pool size ---

for pool_size <- [1, 2, 4] do
  # Start connection with this pool size
  {:ok, pid} = Dux.Connection.start_link(pool_size: pool_size)

  # Pre-compute a 100K dataset
  medium_sql = "SELECT x AS id, x % 100 AS grp, x * 1.5 AS value FROM range(100000) t(x)"
  computed = Dux.from_query(medium_sql) |> Dux.compute()

  IO.puts("--- pool_size: #{pool_size} ---\n")

  Benchee.run(
    %{
      "sequential: 20 filter+compute" => fn ->
        for _ <- 1..20 do
          computed |> Dux.filter_with("value > 75000") |> Dux.compute()
        end
      end,
      "concurrent: 20 filter+compute" => fn ->
        1..20
        |> Enum.map(fn _ ->
          Task.async(fn ->
            computed |> Dux.filter_with("value > 75000") |> Dux.compute()
          end)
        end)
        |> Task.await_many(30_000)
      end,
      "concurrent: 20 full pipelines" => fn ->
        1..20
        |> Enum.map(fn _ ->
          Task.async(fn ->
            computed
            |> Dux.filter_with("value > 50000")
            |> Dux.mutate_with(adjusted: "value * 1.1")
            |> Dux.group_by(:grp)
            |> Dux.summarise_with(total: "SUM(adjusted)", n: "COUNT(*)")
            |> Dux.to_rows()
          end)
        end)
        |> Task.await_many(30_000)
      end
    },
    warmup: 2,
    time: 5,
    print: [configuration: false]
  )

  GenServer.stop(pid)
  IO.puts("")
end

IO.puts("Pool benchmark complete.")
