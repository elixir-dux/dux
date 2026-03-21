# Core Dux benchmarks
# Run with: mix run bench/core_bench.exs

alias Dux.Remote.Worker

# ---------------------------------------------------------------------------
# Setup: generate test data
# ---------------------------------------------------------------------------

IO.puts("Setting up benchmark data...")

# Small dataset (1K rows)
small_data = for i <- 1..1_000, do: %{"id" => i, "group" => rem(i, 10), "value" => i * 1.5}

# Medium dataset (100K via DuckDB)
medium_sql = "SELECT x AS id, x % 100 AS grp, x * 1.5 AS value FROM range(100000) t(x)"

# Large dataset (1M via DuckDB)
large_sql = "SELECT x AS id, x % 1000 AS grp, x * 1.5 AS value FROM range(1000000) t(x)"

# Pre-compute a medium dataset for chained operations
medium_computed = Dux.from_query(medium_sql) |> Dux.compute()

# Write Parquet files for IO benchmarks
tmp_dir = Path.join(System.tmp_dir!(), "dux_bench_#{System.unique_integer([:positive])}")
File.mkdir_p!(tmp_dir)
parquet_path = Path.join(tmp_dir, "medium.parquet")

Dux.from_query(medium_sql) |> Dux.to_parquet(parquet_path)

IO.puts("Benchmark data ready.\n")

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

Benchee.run(
  %{
    # --- Construction ---
    "from_list (1K rows)" => fn ->
      Dux.from_list(small_data) |> Dux.compute()
    end,
    "from_query (100K rows)" => fn ->
      Dux.from_query(medium_sql) |> Dux.compute()
    end,
    "from_query (1M rows)" => fn ->
      Dux.from_query(large_sql) |> Dux.compute()
    end,

    # --- IO ---
    "from_parquet (100K rows)" => fn ->
      Dux.from_parquet(parquet_path) |> Dux.compute()
    end,

    # --- Filter ---
    "filter (100K → ~50K)" => fn ->
      Dux.from_query(medium_sql) |> Dux.filter_with("value > 75000") |> Dux.compute()
    end,

    # --- Mutate ---
    "mutate add column (100K)" => fn ->
      Dux.from_query(medium_sql) |> Dux.mutate_with(doubled: "value * 2") |> Dux.compute()
    end,

    # --- Group + Aggregate ---
    "group_by + summarise (100K → 100 groups)" => fn ->
      Dux.from_query(medium_sql)
      |> Dux.group_by(:grp)
      |> Dux.summarise_with(total: "SUM(value)", n: "COUNT(*)")
      |> Dux.compute()
    end,

    # --- Sort ---
    "sort_by (100K rows)" => fn ->
      Dux.from_query(medium_sql) |> Dux.sort_by(:value) |> Dux.compute()
    end,

    # --- Chained pipeline ---
    "full pipeline (100K): filter → mutate → group → summarise → sort" => fn ->
      Dux.from_query(medium_sql)
      |> Dux.filter_with("value > 50000")
      |> Dux.mutate_with(adjusted: "value * 1.1")
      |> Dux.group_by(:grp)
      |> Dux.summarise_with(total: "SUM(adjusted)", n: "COUNT(*)")
      |> Dux.sort_by(desc: :total)
      |> Dux.compute()
    end,

    # --- Computed base + chained ops ---
    "chained from computed (100K): filter → head" => fn ->
      medium_computed |> Dux.filter_with("value > 75000") |> Dux.head(100) |> Dux.compute()
    end
  },
  warmup: 1,
  time: 5,
  memory_time: 2,
  print: [configuration: false]
)

# ---------------------------------------------------------------------------
# Distributed benchmark (if workers available)
# ---------------------------------------------------------------------------

IO.puts("\n--- Distributed benchmark ---\n")

# Start 2 local workers
{:ok, w1} = Worker.start_link()
{:ok, w2} = Worker.start_link()

Benchee.run(
  %{
    "distributed (2 workers): 100K filter + aggregate" => fn ->
      Dux.from_query(medium_sql)
      |> Dux.filter_with("value > 50000")
      |> Dux.summarise_with(total: "SUM(value)")
      |> Dux.Remote.Coordinator.execute(workers: [w1, w2])
    end,
    "single-node baseline: 100K filter + aggregate" => fn ->
      Dux.from_query(medium_sql)
      |> Dux.filter_with("value > 50000")
      |> Dux.summarise_with(total: "SUM(value)")
      |> Dux.compute()
    end
  },
  warmup: 1,
  time: 5,
  print: [configuration: false]
)

# Cleanup
GenServer.stop(w1)
GenServer.stop(w2)
File.rm_rf!(tmp_dir)

IO.puts("\nBenchmarks complete.")
