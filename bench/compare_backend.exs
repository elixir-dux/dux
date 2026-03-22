# Benchmark: ADBC backend performance
#
# Run against current branch:
#   mix run bench/compare_backend.exs
#
# To compare against v0.1.1 NIF baseline:
#   1. git stash && git checkout v0.1.1
#   2. DUX_BUILD=true mix run bench/compare_backend.exs
#   3. git checkout - && git stash pop
#   4. mix run bench/compare_backend.exs
#   5. Compare the output

require Dux

# -- Setup data --

small_list = Enum.map(1..100, &%{id: &1, region: Enum.at(["US", "EU", "APAC"], rem(&1, 3)), amount: &1 * 10})
medium_list = Enum.map(1..10_000, &%{id: &1, region: Enum.at(["US", "EU", "APAC"], rem(&1, 3)), amount: &1 * 10})

# Write a temp parquet file for parquet benchmarks
parquet_dir = Path.join(System.tmp_dir!(), "dux_bench_#{System.unique_integer([:positive])}")
File.mkdir_p!(parquet_dir)
parquet_path = Path.join(parquet_dir, "bench.parquet")
Dux.from_list(medium_list) |> Dux.to_parquet(parquet_path)

IO.puts("\n=== Dux Backend Benchmark ===\n")

Benchee.run(
  %{
    "from_list(100) → compute" => fn ->
      Dux.from_list(small_list) |> Dux.compute()
    end,
    "from_list(10K) → compute" => fn ->
      Dux.from_list(medium_list) |> Dux.compute()
    end,
    "from_query(range 10K) → compute" => fn ->
      Dux.from_query("SELECT * FROM range(10000) t(x)") |> Dux.compute()
    end,
    "from_parquet → compute" => fn ->
      Dux.from_parquet(parquet_path) |> Dux.compute()
    end,
    "filter + mutate → compute" => fn ->
      Dux.from_list(medium_list)
      |> Dux.filter_with("amount > 5000")
      |> Dux.mutate_with(doubled: "amount * 2")
      |> Dux.compute()
    end,
    "group_by + summarise → compute" => fn ->
      Dux.from_list(medium_list)
      |> Dux.group_by(:region)
      |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)", avg: "AVG(amount)")
      |> Dux.compute()
    end,
    "full pipeline → to_rows" => fn ->
      Dux.from_list(medium_list)
      |> Dux.filter_with("amount > 5000")
      |> Dux.group_by(:region)
      |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
      |> Dux.sort_by(desc: :total)
      |> Dux.to_rows()
    end,
    "join (small) → compute" => fn ->
      left = Dux.from_list(small_list)
      right = Dux.from_list([%{id: 1, tag: "a"}, %{id: 50, tag: "b"}, %{id: 100, tag: "c"}])
      Dux.join(left, right, on: :id) |> Dux.compute()
    end,
    "IPC round-trip (1K rows)" => fn ->
      conn = Dux.Connection.get_conn()
      ref = Dux.Backend.query(conn, "SELECT * FROM range(1000) t(x)")
      ipc = Dux.Backend.table_to_ipc(conn, ref)
      Dux.Backend.table_from_ipc(conn, ipc)
    end,
    "to_columns (10K rows)" => fn ->
      Dux.from_list(medium_list) |> Dux.to_columns()
    end,
    "to_rows (1K rows)" => fn ->
      Dux.from_list(small_list) |> Dux.to_rows()
    end
  },
  time: 3,
  warmup: 1,
  memory_time: 1,
  print: [configuration: false]
)

# Distributed benchmark (if workers available)
IO.puts("\n=== Distributed Benchmark (2 local workers) ===\n")

{:ok, w1} = Dux.Remote.Worker.start_link()
{:ok, w2} = Dux.Remote.Worker.start_link()

Benchee.run(
  %{
    "local: group_by + summarise" => fn ->
      Dux.from_list(medium_list)
      |> Dux.group_by(:region)
      |> Dux.summarise_with(total: "SUM(amount)")
      |> Dux.to_rows()
    end,
    "distributed(2): group_by + summarise" => fn ->
      Dux.from_list(medium_list)
      |> Dux.distribute([w1, w2])
      |> Dux.group_by(:region)
      |> Dux.summarise_with(total: "SUM(amount)")
      |> Dux.to_rows()
    end
  },
  time: 3,
  warmup: 1,
  print: [configuration: false]
)

GenServer.stop(w1)
GenServer.stop(w2)

# Cleanup
File.rm_rf!(parquet_dir)

IO.puts("\nDone.")
