# Core Dux benchmarks
# Run with: mix run bench/core_bench.exs

alias Dux.Remote.Worker

# ---------------------------------------------------------------------------
# Setup: generate test data
# ---------------------------------------------------------------------------

IO.puts("Setting up benchmark data...")

# Small dataset (100 rows)
small_data = for i <- 1..100, do: %{"id" => i, "group" => rem(i, 10), "value" => i * 1.5}

# Large from_list dataset (5K rows, atom keys)
large_list_data =
  for i <- 1..5_000,
      do: %{id: i, group: rem(i, 100), value: i * 1.5, label: "item_#{i}"}

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
    "from_list (100 rows)" => fn ->
      Dux.from_list(small_data) |> Dux.compute()
    end,
    "from_list (5K rows)" => fn ->
      Dux.from_list(large_list_data) |> Dux.compute()
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
# Scale benchmarks (1M rows — catches materialization regressions)
# ---------------------------------------------------------------------------

IO.puts("\n--- Scale benchmarks (1M rows) ---\n")

large_computed = Dux.from_query(large_sql) |> Dux.compute()

Benchee.run(
  %{
    "filter (1M → ~500K)" => fn ->
      Dux.from_query(large_sql) |> Dux.filter_with("value > 750000") |> Dux.compute()
    end,
    "filter from computed (1M → ~500K)" => fn ->
      large_computed |> Dux.filter_with("value > 750000") |> Dux.compute()
    end,
    "mutate (1M rows)" => fn ->
      large_computed |> Dux.mutate_with(doubled: "value * 2") |> Dux.compute()
    end,
    "group_by + summarise (1M → 1K groups)" => fn ->
      large_computed
      |> Dux.group_by(:grp)
      |> Dux.summarise_with(total: "SUM(value)", n: "COUNT(*)")
      |> Dux.compute()
    end,
    "to_rows (10K)" => fn ->
      large_computed |> Dux.head(10_000) |> Dux.to_rows()
    end,
    "to_rows (50K)" => fn ->
      large_computed |> Dux.head(50_000) |> Dux.to_rows()
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

# ---------------------------------------------------------------------------
# Streaming vs batch merge benchmark
# ---------------------------------------------------------------------------

IO.puts("\n--- Streaming vs batch merge ---\n")

# SUM pipeline → streaming merge (lattice-compatible)
# The distributed path now auto-selects streaming for SUM/COUNT/MIN/MAX
Benchee.run(
  %{
    "streaming merge (SUM + COUNT, 2 workers)" => fn ->
      Dux.from_query(medium_sql)
      |> Dux.group_by(:grp)
      |> Dux.summarise_with(total: "SUM(value)", n: "COUNT(*)")
      |> Dux.Remote.Coordinator.execute(workers: [w1, w2])
    end,
    "streaming merge (MIN + MAX, 2 workers)" => fn ->
      Dux.from_query(medium_sql)
      |> Dux.summarise_with(lo: "MIN(value)", hi: "MAX(value)")
      |> Dux.Remote.Coordinator.execute(workers: [w1, w2])
    end
  },
  warmup: 1,
  time: 5,
  print: [configuration: false]
)

# ---------------------------------------------------------------------------
# Shuffle join benchmark
# ---------------------------------------------------------------------------

IO.puts("\n--- Shuffle join benchmark ---\n")

# Two large datasets that will trigger shuffle (both too big for broadcast)
left_large = Dux.from_query("SELECT x AS id, x % 50 AS key, x * 1.5 AS val FROM range(100000) t(x)")
right_large = Dux.from_query("SELECT x AS id, x % 50 AS key, x * 2.0 AS score FROM range(100000) t(x)")

Benchee.run(
  %{
    "shuffle join (2 workers, 100K × 100K)" => fn ->
      left_large
      |> Dux.distribute([w1, w2])
      |> Dux.join(right_large, on: :key)
      |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(val)")
      |> Dux.collect()
    end,
    "local join baseline (100K × 100K)" => fn ->
      left_large
      |> Dux.join(right_large, on: :key)
      |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(val)")
      |> Dux.compute()
    end
  },
  warmup: 1,
  time: 5,
  print: [configuration: false]
)

# ---------------------------------------------------------------------------
# Broadcast join + bloom filter benchmark
# ---------------------------------------------------------------------------

IO.puts("\n--- Broadcast join (bloom filter) benchmark ---\n")

# Large fact table joined with small dimension — triggers broadcast with bloom pre-filter
fact_table = Dux.from_query("SELECT x AS id, x % 1000 AS dim_key, x * 1.5 AS amount FROM range(100000) t(x)")
dim_table = Dux.from_list(for i <- 1..20, do: %{dim_key: i, label: "label_#{i}"})

Benchee.run(
  %{
    "broadcast join + bloom filter (2 workers, 100K × 20)" => fn ->
      fact_table
      |> Dux.distribute([w1, w2])
      |> Dux.join(dim_table, on: :dim_key)
      |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
      |> Dux.collect()
    end,
    "local join baseline (100K × 20)" => fn ->
      fact_table
      |> Dux.join(dim_table, on: :dim_key)
      |> Dux.summarise_with(n: "COUNT(*)", total: "SUM(amount)")
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
