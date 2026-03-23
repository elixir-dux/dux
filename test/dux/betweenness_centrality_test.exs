defmodule Dux.BetweennessCentralityTest do
  use ExUnit.Case, async: false

  alias Dux.Remote.Worker

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "local betweenness centrality" do
    test "line graph: middle vertex has highest BC" do
      # 1 -- 2 -- 3 -- 4 -- 5
      # Vertex 3 should have highest BC (all paths between sides pass through it)
      vertices = Dux.from_list(Enum.map(1..5, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 2, dst: 3},
          %{src: 3, dst: 2},
          %{src: 3, dst: 4},
          %{src: 4, dst: 3},
          %{src: 4, dst: 5},
          %{src: 5, dst: 4}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 5, seed: 42)
        |> Dux.sort_by(desc: :bc)
        |> Dux.to_rows()

      # Vertex 3 should be first (highest BC)
      assert hd(result)["id"] == 3

      # Vertex 3 BC > vertex 2 BC > vertex 1 BC
      bc_map = Map.new(result, fn r -> {r["id"], r["bc"]} end)
      assert bc_map[3] > bc_map[2]
      assert bc_map[2] > bc_map[1]

      # Endpoints should have zero BC (no shortest paths pass through them)
      assert bc_map[1] == 0.0
      assert bc_map[5] == 0.0
    end

    test "star graph: center has highest BC" do
      # Center: 1, spokes: 2,3,4,5
      vertices = Dux.from_list(Enum.map(1..5, &%{id: &1}))

      edges =
        Dux.from_list(
          for spoke <- 2..5, dir <- [:fwd, :bwd] do
            case dir do
              :fwd -> %{src: 1, dst: spoke}
              :bwd -> %{src: spoke, dst: 1}
            end
          end
        )

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 5, seed: 42)
        |> Dux.sort_by(desc: :bc)
        |> Dux.to_rows()

      # Center vertex 1 has highest BC
      assert hd(result)["id"] == 1

      # All spokes have zero BC
      bc_map = Map.new(result, fn r -> {r["id"], r["bc"]} end)

      for spoke <- 2..5 do
        assert bc_map[spoke] == 0.0
      end
    end

    test "complete graph: all vertices have equal BC" do
      # K4: all connected to all — BC is equal for all vertices
      vertices = Dux.from_list(Enum.map(1..4, &%{id: &1}))

      edges =
        Dux.from_list(
          for i <- 1..4, j <- 1..4, i != j do
            %{src: i, dst: j}
          end
        )

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 4, seed: 42)
        |> Dux.to_rows()

      bcs = Enum.map(result, & &1["bc"])
      # All should be equal (within tolerance)
      assert_in_delta Enum.min(bcs), Enum.max(bcs), 0.01
    end

    test "triangle: all vertices have equal BC" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 2, dst: 3},
          %{src: 3, dst: 2},
          %{src: 1, dst: 3},
          %{src: 3, dst: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 3, seed: 42)
        |> Dux.to_rows()

      bcs = Enum.map(result, & &1["bc"])
      assert_in_delta Enum.min(bcs), Enum.max(bcs), 0.01
    end

    test "seed produces reproducible results" do
      vertices = Dux.from_list(Enum.map(1..10, &%{id: &1}))

      edges =
        Dux.from_list(
          for i <- 1..9 do
            [%{src: i, dst: i + 1}, %{src: i + 1, dst: i}]
          end
          |> List.flatten()
        )

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      r1 =
        Dux.Graph.betweenness_centrality(graph, sample: 5, seed: 123)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      r2 =
        Dux.Graph.betweenness_centrality(graph, sample: 5, seed: 123)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      for {row1, row2} <- Enum.zip(r1, r2) do
        assert_in_delta row1["bc"], row2["bc"], 1.0e-10
      end
    end

    test "normalize: false returns raw scores" do
      vertices = Dux.from_list(Enum.map(1..5, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 2, dst: 3},
          %{src: 3, dst: 2},
          %{src: 3, dst: 4},
          %{src: 4, dst: 3},
          %{src: 4, dst: 5},
          %{src: 5, dst: 4}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      normalized =
        Dux.Graph.betweenness_centrality(graph, sample: 5, seed: 42, normalize: true)
        |> Dux.to_rows()

      raw =
        Dux.Graph.betweenness_centrality(graph, sample: 5, seed: 42, normalize: false)
        |> Dux.to_rows()

      # Normalized values should be smaller than raw
      max_norm = normalized |> Enum.map(& &1["bc"]) |> Enum.max()
      max_raw = raw |> Enum.map(& &1["bc"]) |> Enum.max()
      assert max_raw >= max_norm
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "single vertex graph" do
      vertices = Dux.from_list([%{id: 1}])
      edges = Dux.from_list([%{src: 1, dst: 1}])
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 1, seed: 42)
        |> Dux.to_rows()

      assert length(result) == 1
      assert hd(result)["bc"] == 0.0
    end

    test "two vertices" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}])

      edges =
        Dux.from_list([%{src: 1, dst: 2}, %{src: 2, dst: 1}])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 2, seed: 42)
        |> Dux.to_rows()

      # With only 2 vertices, no vertex can be "between" others
      bcs = Enum.map(result, & &1["bc"])
      assert Enum.all?(bcs, &(&1 == 0.0))
    end

    test "disconnected graph" do
      # Two disconnected components: {1,2} and {3,4}
      vertices = Dux.from_list(Enum.map(1..4, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 3, dst: 4},
          %{src: 4, dst: 3}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 4, seed: 42)
        |> Dux.to_rows()

      # All should have 0 BC (no bridges possible in disconnected pairs)
      bcs = Enum.map(result, & &1["bc"])
      assert Enum.all?(bcs, &(&1 == 0.0))
    end
  end

  # ---------------------------------------------------------------------------
  # Karate club dataset
  # ---------------------------------------------------------------------------

  describe "karate club" do
    test "known high-BC vertices in Zachary's karate club" do
      graph = Dux.Datasets.karate_club()

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 34, seed: 42)
        |> Dux.sort_by(desc: :bc)
        |> Dux.to_rows()

      top_5_ids = Enum.take(result, 5) |> Enum.map(& &1["id"]) |> MapSet.new()

      # Vertices 1 and 34 are the two faction leaders — they should be in top 5
      assert MapSet.member?(top_5_ids, 1)
      assert MapSet.member?(top_5_ids, 34)
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed
  # ---------------------------------------------------------------------------

  describe "distributed betweenness centrality" do
    test "distributed matches local for small graph" do
      {:ok, w1} = Worker.start_link()
      {:ok, w2} = Worker.start_link()

      on_exit(fn ->
        Enum.each([w1, w2], fn w ->
          try do
            GenServer.stop(w)
          catch
            :exit, _ -> :ok
          end
        end)
      end)

      vertices = Dux.from_list(Enum.map(1..6, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 2, dst: 3},
          %{src: 3, dst: 2},
          %{src: 3, dst: 4},
          %{src: 4, dst: 3},
          %{src: 4, dst: 5},
          %{src: 5, dst: 4},
          %{src: 5, dst: 6},
          %{src: 6, dst: 5}
        ])

      local_graph = Dux.Graph.new(vertices: vertices, edges: edges)

      dist_graph =
        Dux.Graph.new(vertices: vertices, edges: edges)
        |> Dux.Graph.distribute([w1, w2])

      local_result =
        Dux.Graph.betweenness_centrality(local_graph, sample: 6, seed: 42)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      dist_result =
        Dux.Graph.betweenness_centrality(dist_graph, sample: 6, seed: 42)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      for {local, dist} <- Enum.zip(local_result, dist_result) do
        assert local["id"] == dist["id"]
        assert_in_delta local["bc"], dist["bc"], 0.01
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "sample larger than vertex count doesn't crash" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 2, dst: 3},
          %{src: 3, dst: 2}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        Dux.Graph.betweenness_centrality(graph, sample: 1000, seed: 42)
        |> Dux.to_rows()

      assert length(result) == 3
    end
  end
end
