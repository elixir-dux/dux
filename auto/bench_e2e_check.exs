# E2E benchmark: measures compute + read data (to_rows / to_columns).
#
# This complements bench_quick.exs which only measures compute().
# The key metric is e2e_ratio (Dux time / Explorer time). Target: ≤ 1.0.
#
# Usage:
#   mix run auto/bench_e2e_check.exs
#   BENCH_ROWS=500000 mix run auto/bench_e2e_check.exs

require Dux
alias Explorer.DataFrame
require Explorer.DataFrame

num_rows = String.to_integer(System.get_env("BENCH_ROWS") || "1000000")
warmup = 3
iters = 5

IO.puts("# E2E bench: #{num_rows} rows, warmup=#{warmup}, iters=#{iters}")

data = Enum.map(1..num_rows, fn _ ->
  %{region: Enum.random(~w(North South East West Central)),
    product: Enum.random(~w(Widget Gadget Doohickey Thingamajig Gizmo)),
    quantity: :rand.uniform(100),
    price: Float.round(:rand.uniform() * 500, 2),
    date: Date.from_iso8601!("2025-01-15")}
end)

xl_df = DataFrame.new(data)
xl_dux = Dux.from_list(data) |> Dux.compute()

measure = fn fun ->
  for _ <- 1..warmup, do: fun.()
  times = for _ <- 1..iters do
    :erlang.garbage_collect()
    {us, _} = :timer.tc(fun)
    us
  end
  Enum.sort(times) |> Enum.at(div(length(times), 2))
end

benchmarks = [
  {"filter_rows",
    fn -> DataFrame.filter(xl_df, quantity > 50 and price > 100.0) |> DataFrame.to_rows() end,
    fn -> xl_dux |> Dux.filter(quantity > 50 and price > 100.0) |> Dux.to_rows() end},
  {"mutate_rows",
    fn -> DataFrame.mutate(xl_df, revenue: quantity * price) |> DataFrame.to_rows() end,
    fn -> xl_dux |> Dux.mutate(revenue: quantity * price) |> Dux.to_rows() end},
  {"summarise_rows",
    fn -> xl_df |> DataFrame.group_by(:region) |> DataFrame.summarise(total: sum(quantity), avg_price: mean(price)) |> DataFrame.to_rows() end,
    fn -> xl_dux |> Dux.group_by(:region) |> Dux.summarise(total: sum(quantity), avg_price: avg(price)) |> Dux.to_rows() end},
  {"filter_cols",
    fn -> DataFrame.filter(xl_df, quantity > 50 and price > 100.0) |> DataFrame.to_columns() end,
    fn -> xl_dux |> Dux.filter(quantity > 50 and price > 100.0) |> Dux.to_columns() end},
]

results =
  for {name, exp_fn, dux_fn} <- benchmarks do
    exp = measure.(exp_fn)
    dux = measure.(dux_fn)
    ratio = Float.round(dux / exp, 2)
    IO.puts("METRIC #{name}_explorer_us=#{exp}")
    IO.puts("METRIC #{name}_dux_us=#{dux}")
    IO.puts("METRIC #{name}_ratio=#{ratio}")
    {name, ratio}
  end

# E2E combined: geometric mean of the _rows ratios (exclude _cols)
rows_ratios = for {name, ratio} <- results, String.ends_with?(name, "_rows"), do: ratio
e2e_geo = :math.pow(Enum.reduce(rows_ratios, 1.0, &(&1 * &2)), 1.0 / length(rows_ratios)) |> Float.round(2)
IO.puts("METRIC e2e_combined_ratio=#{e2e_geo}")

IO.puts("")
IO.puts("# Summary: " <> Enum.map_join(results, "  ", fn {n, r} -> "#{n}=#{r}x" end) <> "  e2e_combined=#{e2e_geo}x")

_ = xl_dux
