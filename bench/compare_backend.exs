defmodule Bench do
  def small_list do
    Enum.map(1..100, &%{id: &1, region: Enum.at(["US", "EU", "APAC"], rem(&1, 3)), amount: &1 * 10})
  end

  def medium_list do
    Enum.map(1..10_000, &%{id: &1, region: Enum.at(["US", "EU", "APAC"], rem(&1, 3)), amount: &1 * 10})
  end

  def setup_parquet do
    dir = Path.join(System.tmp_dir!(), "dux_bench_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "bench.parquet")
    Dux.from_list(medium_list()) |> Dux.to_parquet(path)
    {dir, path}
  end

  def from_list_100, do: Dux.from_list(small_list()) |> Dux.compute()
  def from_list_10k, do: Dux.from_list(medium_list()) |> Dux.compute()
  def from_query_10k, do: Dux.from_query("SELECT * FROM range(10000) t(x)") |> Dux.compute()

  def from_parquet(path), do: Dux.from_parquet(path) |> Dux.compute()

  def filter_mutate do
    Dux.from_list(medium_list())
    |> Dux.filter_with("amount > 5000")
    |> Dux.mutate_with(doubled: "amount * 2")
    |> Dux.compute()
  end

  def group_summarise do
    Dux.from_list(medium_list())
    |> Dux.group_by(:region)
    |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)", avg: "AVG(amount)")
    |> Dux.compute()
  end

  def full_pipeline do
    Dux.from_list(medium_list())
    |> Dux.filter_with("amount > 5000")
    |> Dux.group_by(:region)
    |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
    |> Dux.sort_by(desc: :total)
    |> Dux.to_rows()
  end

  def join_small do
    left = Dux.from_list(small_list())
    right = Dux.from_list([%{id: 1, tag: "a"}, %{id: 50, tag: "b"}, %{id: 100, tag: "c"}])
    Dux.join(left, right, on: :id) |> Dux.compute()
  end

  def to_columns_10k, do: Dux.from_list(medium_list()) |> Dux.to_columns()
  def to_rows_1k, do: Dux.from_list(small_list()) |> Dux.to_rows()
end

{dir, parquet_path} = Bench.setup_parquet()

IO.puts("\n=== Dux Backend Benchmark ===\n")

Benchee.run(
  %{
    "from_list(100) → compute" => &Bench.from_list_100/0,
    "from_list(10K) → compute" => &Bench.from_list_10k/0,
    "from_query(range 10K) → compute" => &Bench.from_query_10k/0,
    "from_parquet → compute" => fn -> Bench.from_parquet(parquet_path) end,
    "filter + mutate → compute" => &Bench.filter_mutate/0,
    "group_by + summarise → compute" => &Bench.group_summarise/0,
    "full pipeline → to_rows" => &Bench.full_pipeline/0,
    "join (small) → compute" => &Bench.join_small/0,
    "to_columns (10K rows)" => &Bench.to_columns_10k/0,
    "to_rows (1K rows)" => &Bench.to_rows_1k/0
  },
  time: 3,
  warmup: 1,
  memory_time: 1,
  print: [configuration: false]
)

IO.puts("\n=== Distributed Benchmark (2 local workers) ===\n")

{:ok, w1} = Dux.Remote.Worker.start_link()
{:ok, w2} = Dux.Remote.Worker.start_link()

defmodule DistBench do
  def local(medium) do
    Dux.from_list(medium)
    |> Dux.group_by(:region)
    |> Dux.summarise_with(total: "SUM(amount)")
    |> Dux.to_rows()
  end

  def distributed(medium, workers) do
    Dux.from_list(medium)
    |> Dux.distribute(workers)
    |> Dux.group_by(:region)
    |> Dux.summarise_with(total: "SUM(amount)")
    |> Dux.to_rows()
  end
end

medium = Bench.medium_list()

Benchee.run(
  %{
    "local: group_by + summarise" => fn -> DistBench.local(medium) end,
    "distributed(2): group_by + summarise" => fn -> DistBench.distributed(medium, [w1, w2]) end
  },
  time: 3,
  warmup: 1,
  print: [configuration: false]
)

GenServer.stop(w1)
GenServer.stop(w2)
File.rm_rf!(dir)

IO.puts("\nDone.")
