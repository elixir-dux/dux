# Graph betweenness centrality benchmark
# Run with: mix run bench/graph_bench.exs

require Dux

IO.puts("Setting up graph benchmark data...")

# Small graph: 500 vertices, ~2K edges
small_edges =
  for _ <- 1..3000 do
    %{src: :rand.uniform(500), dst: :rand.uniform(500)}
  end
  |> Enum.uniq_by(fn %{src: s, dst: d} -> {min(s, d), max(s, d)} end)
  |> Enum.reject(fn %{src: s, dst: d} -> s == d end)

small_graph =
  Dux.Graph.new(
    vertices: Dux.from_list(for i <- 1..500, do: %{id: i}),
    edges: Dux.from_list(small_edges)
  )

# Medium graph: 2K vertices, ~10K edges
medium_edges =
  for _ <- 1..15_000 do
    %{src: :rand.uniform(2000), dst: :rand.uniform(2000)}
  end
  |> Enum.uniq_by(fn %{src: s, dst: d} -> {min(s, d), max(s, d)} end)
  |> Enum.reject(fn %{src: s, dst: d} -> s == d end)

medium_graph =
  Dux.Graph.new(
    vertices: Dux.from_list(for i <- 1..2000, do: %{id: i}),
    edges: Dux.from_list(medium_edges)
  )

IO.puts(
  "Small graph: 500 vertices, #{length(small_edges)} edges. Medium: 2K vertices, #{length(medium_edges)} edges.\n"
)

Benchee.run(
  %{
    "betweenness (500v, sample=25)" => fn ->
      Dux.Graph.betweenness_centrality(small_graph, sample: 25, seed: 42)
      |> Dux.compute()
    end,
    "betweenness (500v, sample=50)" => fn ->
      Dux.Graph.betweenness_centrality(small_graph, sample: 50, seed: 42)
      |> Dux.compute()
    end,
    "betweenness (2Kv, sample=50)" => fn ->
      Dux.Graph.betweenness_centrality(medium_graph, sample: 50, seed: 42)
      |> Dux.compute()
    end,
    "betweenness (2Kv, sample=100)" => fn ->
      Dux.Graph.betweenness_centrality(medium_graph, sample: 100, seed: 42)
      |> Dux.compute()
    end
  },
  warmup: 1,
  time: 10,
  print: [configuration: false]
)

IO.puts("\nGraph benchmarks complete.")
