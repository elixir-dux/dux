# Benchmark: Dux vs Explorer (eager mode)
#
# Adapted from https://github.com/akoutmos/dux_v_explorer
# Runs filter, mutate, and summarise on 10M rows.
#
# Usage:
#   mix run bench/dux_v_explorer.exs
#
# To run a smaller dataset for a quick smoke test:
#   BENCH_ROWS=100000 mix run bench/dux_v_explorer.exs

require Dux

alias Explorer.DataFrame
require Explorer.DataFrame

# ---------------------------------------------------------------------------
# Data generator (same as original benchmark)
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
# Generate data
# ---------------------------------------------------------------------------

num_rows =
  case System.get_env("BENCH_ROWS") do
    nil -> 10_000_000
    n -> String.to_integer(n)
  end

IO.puts("Generating #{num_rows} rows of test data...")
{gen_time, data_set} = :timer.tc(fn -> BenchData.gen(num_rows) end)
IO.puts("  Data generated in #{Float.round(gen_time / 1_000_000, 2)}s")

IO.puts("\nLoading into Explorer...")
{exp_time, xl_df} = :timer.tc(fn -> DataFrame.new(data_set) end)
IO.puts("  Explorer loaded in #{Float.round(exp_time / 1_000_000, 2)}s")

IO.puts("\nLoading into Dux (from_list + compute)...")
{dux_time, xl_dux} = :timer.tc(fn -> data_set |> Dux.from_list() |> Dux.compute() end)
IO.puts("  Dux loaded in #{Float.round(dux_time / 1_000_000, 2)}s")

# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------

IO.puts("\n--- Sanity checks ---")

{exp_filter_rows, _} =
  xl_df
  |> DataFrame.filter(quantity > 50 and price > 100.0)
  |> DataFrame.shape()

dux_filter_result =
  xl_dux
  |> Dux.filter(quantity > 50 and price > 100.0)
  |> Dux.compute()

%Dux{source: {:table, table_ref}} = dux_filter_result
conn = Dux.Connection.get_conn()
dux_filter_rows = Dux.Backend.table_n_rows(conn, table_ref)

IO.puts("Filter result rows — Explorer: #{exp_filter_rows}, Dux: #{dux_filter_rows}")

if exp_filter_rows != dux_filter_rows do
  IO.puts("WARNING: Row counts differ!")
end

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

inputs = %{"#{num_rows} rows" => {xl_df, xl_dux}}
bench_opts = [warmup: 2, time: 5, memory_time: 2]

IO.puts("\n========================================")
IO.puts("  FILTER BENCHMARK")
IO.puts("========================================\n")

Benchee.run(
  %{
    "Explorer filter" => fn {df, _} ->
      DataFrame.filter(df, quantity > 50 and price > 100.0)
    end,
    "Dux filter" => fn {_, dux} ->
      dux
      |> Dux.filter(quantity > 50 and price > 100.0)
      |> Dux.compute()
    end
  },
  inputs: inputs,
  warmup: bench_opts[:warmup],
  time: bench_opts[:time],
  memory_time: bench_opts[:memory_time]
)

IO.puts("\n========================================")
IO.puts("  MUTATE BENCHMARK")
IO.puts("========================================\n")

Benchee.run(
  %{
    "Explorer mutate" => fn {df, _} ->
      DataFrame.mutate(df, revenue: quantity * price)
    end,
    "Dux mutate" => fn {_, dux} ->
      dux
      |> Dux.mutate(revenue: quantity * price)
      |> Dux.compute()
    end
  },
  inputs: inputs,
  warmup: bench_opts[:warmup],
  time: bench_opts[:time],
  memory_time: bench_opts[:memory_time]
)

IO.puts("\n========================================")
IO.puts("  SUMMARISE BENCHMARK")
IO.puts("========================================\n")

Benchee.run(
  %{
    "Explorer summarise" => fn {df, _} ->
      df
      |> DataFrame.group_by(:region)
      |> DataFrame.summarise(total: sum(quantity), avg_price: mean(price))
    end,
    "Dux summarise" => fn {_, dux} ->
      dux
      |> Dux.group_by(:region)
      |> Dux.summarise(total: sum(quantity), avg_price: avg(price))
      |> Dux.compute()
    end
  },
  inputs: inputs,
  warmup: bench_opts[:warmup],
  time: bench_opts[:time],
  memory_time: bench_opts[:memory_time]
)
