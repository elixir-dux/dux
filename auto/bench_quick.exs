# Quick benchmark for autoresearch loop.
#
# Outputs machine-readable METRIC lines for the autoresearch script to parse.
# Bypasses Benchee for speed — does manual warmup + measured iterations.
#
# Usage:
#   mix run auto/bench_quick.exs
#   BENCH_ROWS=1000000 mix run auto/bench_quick.exs
#
# Environment:
#   BENCH_ROWS      — number of rows (default: 1_000_000)
#   BENCH_WARMUP    — warmup iterations (default: 5)
#   BENCH_ITERS     — measured iterations (default: 7)

require Dux

alias Explorer.DataFrame
require Explorer.DataFrame

# ---------------------------------------------------------------------------
# Data generator (same as original akoutmos benchmark)
# ---------------------------------------------------------------------------

defmodule BenchData do
  @regions ~w(North South East West Central)
  @products ~w(Widget Gadget Doohickey Thingamajig Gizmo)

  def gen(num_rows) do
    month = String.pad_leading("#{:rand.uniform(12)}", 2, "0")
    day = String.pad_leading("#{:rand.uniform(28)}", 2, "0")

    1..num_rows
    |> Enum.map(fn _ ->
      %{
        region: Enum.random(@regions),
        product: Enum.random(@products),
        quantity: :rand.uniform(100),
        price: Float.round(:rand.uniform() * 500, 2),
        date: Date.from_iso8601!("2025-#{month}-#{day}")
      }
    end)
  end
end

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

defmodule Bench do
  @doc "Run fun `n` times, return list of durations in microseconds."
  def measure(fun, n) do
    Enum.map(1..n, fn _ ->
      :erlang.garbage_collect()
      {us, _} = :timer.tc(fun)
      us
    end)
  end

  @doc "Take the median of a list of numbers."
  def median(list) do
    sorted = Enum.sort(list)
    len = length(sorted)
    mid = div(len, 2)

    if rem(len, 2) == 0 do
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    else
      Enum.at(sorted, mid)
    end
  end
end

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

num_rows = String.to_integer(System.get_env("BENCH_ROWS") || "1000000")
warmup = String.to_integer(System.get_env("BENCH_WARMUP") || "5")
iters = String.to_integer(System.get_env("BENCH_ITERS") || "7")

IO.puts("# Bench config: rows=#{num_rows} warmup=#{warmup} iters=#{iters}")

# ---------------------------------------------------------------------------
# Generate and load data
# ---------------------------------------------------------------------------

IO.puts("# Generating data...")
data_set = BenchData.gen(num_rows)

IO.puts("# Loading Explorer...")
xl_df = DataFrame.new(data_set)

IO.puts("# Loading Dux...")
xl_dux = data_set |> Dux.from_list() |> Dux.compute()

# ---------------------------------------------------------------------------
# Sanity check — row counts must match
# ---------------------------------------------------------------------------

{exp_rows, _} =
  xl_df
  |> DataFrame.filter(quantity > 50 and price > 100.0)
  |> DataFrame.shape()

dux_result =
  xl_dux
  |> Dux.filter(quantity > 50 and price > 100.0)
  |> Dux.compute()

%Dux{source: {:table, ref}} = dux_result
conn = Dux.Connection.get_conn()
dux_rows = Dux.Backend.table_n_rows(conn, ref)

if exp_rows != dux_rows do
  IO.puts("FATAL: row count mismatch — Explorer=#{exp_rows} Dux=#{dux_rows}")
  System.halt(1)
end

IO.puts("# Sanity check passed (#{exp_rows} filtered rows)")

# ---------------------------------------------------------------------------
# Define benchmark functions
# ---------------------------------------------------------------------------

benchmarks = %{
  "filter" => %{
    explorer: fn -> DataFrame.filter(xl_df, quantity > 50 and price > 100.0) end,
    dux: fn -> xl_dux |> Dux.filter(quantity > 50 and price > 100.0) |> Dux.compute() end
  },
  "mutate" => %{
    explorer: fn -> DataFrame.mutate(xl_df, revenue: quantity * price) end,
    dux: fn -> xl_dux |> Dux.mutate(revenue: quantity * price) |> Dux.compute() end
  },
  "summarise" => %{
    explorer: fn ->
      xl_df |> DataFrame.group_by(:region) |> DataFrame.summarise(total: sum(quantity), avg_price: mean(price))
    end,
    dux: fn ->
      xl_dux |> Dux.group_by(:region) |> Dux.summarise(total: sum(quantity), avg_price: avg(price)) |> Dux.compute()
    end
  }
}

# ---------------------------------------------------------------------------
# Run benchmarks
# ---------------------------------------------------------------------------

results =
  for {name, %{explorer: exp_fn, dux: dux_fn}} <- benchmarks do
    IO.puts("# Benchmarking #{name}...")

    # Warmup
    for _ <- 1..warmup do
      exp_fn.()
      dux_fn.()
    end

    # Measure
    exp_times = Bench.measure(exp_fn, iters)
    dux_times = Bench.measure(dux_fn, iters)

    exp_median = Bench.median(exp_times)
    dux_median = Bench.median(dux_times)

    ratio = if exp_median > 0, do: Float.round(dux_median / exp_median, 2), else: 0.0

    {name, %{explorer_us: exp_median, dux_us: dux_median, ratio: ratio}}
  end

# ---------------------------------------------------------------------------
# Output machine-readable metrics
# ---------------------------------------------------------------------------

IO.puts("")

for {name, %{explorer_us: exp, dux_us: dux, ratio: ratio}} <- results do
  IO.puts("METRIC #{name}_explorer_us=#{exp}")
  IO.puts("METRIC #{name}_dux_us=#{dux}")
  IO.puts("METRIC #{name}_ratio=#{ratio}")
end

# Combined: geometric mean of the 3 ratios (single optimization target)
ratios = Enum.map(results, fn {_, %{ratio: r}} -> r end)
geo_mean = :math.pow(Enum.reduce(ratios, 1.0, &(&1 * &2)), 1.0 / length(ratios)) |> Float.round(2)

IO.puts("METRIC combined_ratio=#{geo_mean}")

# Check for stale views — views not cleaned up by GC are a memory leak.
:erlang.garbage_collect()
Process.sleep(200)
:erlang.garbage_collect()
Process.sleep(100)

stale =
  case Adbc.Connection.query(Dux.Connection.get_conn(),
         "SELECT count(*) AS n FROM information_schema.tables WHERE table_type = 'VIEW' AND table_name LIKE '__dux_v_%'") do
    {:ok, result} ->
      case Adbc.Result.to_map(result) do
        %{"n" => [n]} -> n
        _ -> -1
      end
    _ -> -1
  end

IO.puts("METRIC stale_views=#{stale}")

IO.puts("")
IO.puts("# Summary: filter=#{Enum.find_value(results, fn {n, m} -> if n == "filter", do: m.ratio end)}x  mutate=#{Enum.find_value(results, fn {n, m} -> if n == "mutate", do: m.ratio end)}x  summarise=#{Enum.find_value(results, fn {n, m} -> if n == "summarise", do: m.ratio end)}x  combined=#{geo_mean}x  stale_views=#{stale}")
