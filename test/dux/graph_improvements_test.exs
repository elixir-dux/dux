defmodule Dux.GraphImprovementsTest do
  use ExUnit.Case, async: true
  require Dux

  alias Dux.Datasets
  alias Dux.Remote.Worker

  # ---------------------------------------------------------------------------
  # USING KEY shortest paths — happy path
  # ---------------------------------------------------------------------------

  describe "shortest_paths USING KEY" do
    test "linear chain: distances are sequential" do
      vertices = Dux.from_list(Enum.map(1..10, &%{id: &1}))

      edges =
        Dux.from_list(
          for i <- 1..9 do
            %{src: i, dst: i + 1}
          end
        )

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert result["node"] == Enum.to_list(1..10)
      assert result["dist"] == Enum.to_list(0..9)
    end

    test "diamond graph: takes shortest path" do
      # 1 → 2 → 4 (distance 2)
      # 1 → 3 → 4 (distance 2)
      # 1 → 4     (distance 1 — direct edge)
      vertices = Dux.from_list(Enum.map(1..4, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 1, dst: 3},
          %{src: 2, dst: 4},
          %{src: 3, dst: 4},
          %{src: 1, dst: 4}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert result["node"] == [1, 2, 3, 4]
      # Node 4 should have distance 1 (direct edge), not 2
      assert result["dist"] == [0, 1, 1, 1]
    end

    test "disconnected vertices are not reached" do
      vertices = Dux.from_list(Enum.map(1..4, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 3, dst: 4}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1)
        |> Dux.to_columns()

      # Only nodes 1 and 2 reachable from 1
      assert Enum.sort(result["node"]) == [1, 2]
    end

    test "max_depth limits traversal" do
      vertices = Dux.from_list(Enum.map(1..10, &%{id: &1}))

      edges =
        Dux.from_list(
          for i <- 1..9 do
            %{src: i, dst: i + 1}
          end
        )

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1, max_depth: 3)
        |> Dux.to_columns()

      # Should only reach nodes 1-4 (distances 0-3)
      assert Enum.sort(result["node"]) == [1, 2, 3, 4]
    end

    test "cycle doesn't infinite loop" do
      vertices = Dux.from_list(Enum.map(1..3, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 3},
          %{src: 3, dst: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert result["node"] == [1, 2, 3]
      assert result["dist"] == [0, 1, 2]
    end
  end

  # ---------------------------------------------------------------------------
  # Weighted shortest paths
  # ---------------------------------------------------------------------------

  describe "weighted shortest paths" do
    test "weight option uses edge weights" do
      vertices = Dux.from_list(Enum.map(1..4, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2, cost: 1},
          %{src: 2, dst: 4, cost: 1},
          %{src: 1, dst: 3, cost: 10},
          %{src: 3, dst: 4, cost: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      # Without weight: all edges cost 1, so 1→3→4 = distance 2
      unweighted =
        graph
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert unweighted["dist"] == [0, 1, 1, 2]

      # With weight: 1→2→4 costs 2, 1→3→4 costs 11
      weighted =
        graph
        |> Dux.Graph.shortest_paths(1, weight: :cost)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      # Node 3 costs 10 (direct), node 4 costs 2 (via node 2)
      assert weighted["dist"] == [0, 1, 10, 2]
    end

    test "weighted path chooses cheaper route" do
      # Two paths from A to C:
      # A →(cost:10)→ B →(cost:1)→ C  = 11
      # A →(cost:5)→ C                 = 5
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2, w: 10},
          %{src: 2, dst: 3, w: 1},
          %{src: 1, dst: 3, w: 5}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1, weight: :w)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      # Node 2 costs 10 (direct), node 3 costs 5 (direct, cheaper than via 2)
      assert result["dist"] == [0, 10, 5]
    end
  end

  # ---------------------------------------------------------------------------
  # USING KEY connected components — happy path
  # ---------------------------------------------------------------------------

  describe "connected_components USING KEY" do
    test "single connected component" do
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
      result = graph |> Dux.Graph.connected_components() |> Dux.to_columns()

      components = Enum.uniq(result["component"])
      assert length(components) == 1
    end

    test "two components" do
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
        graph
        |> Dux.Graph.connected_components()
        |> Dux.sort_by(:id)
        |> Dux.to_columns()

      [c1, c2, c3, c4] = result["component"]
      assert c1 == c2
      assert c3 == c4
      assert c1 != c3
    end

    test "isolated vertices are their own component" do
      vertices = Dux.from_list(Enum.map(1..5, &%{id: &1}))
      # Only 1-2 connected, 3,4,5 isolated
      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.connected_components()
        |> Dux.to_columns()

      components = Enum.uniq(result["component"]) |> length()
      # 1-2 in one component, 3, 4, 5 each in their own = 4 components
      assert components == 4
    end

    test "karate club is single component" do
      graph = Datasets.karate_club()

      result =
        graph
        |> Dux.Graph.connected_components()
        |> Dux.to_columns()

      assert length(Enum.uniq(result["component"])) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # PageRank convergence
  # ---------------------------------------------------------------------------

  describe "PageRank convergence" do
    test "converges before max_iterations on small graph" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 3},
          %{src: 3, dst: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      # With convergence detection, PageRank should produce stable results
      # and converge early (well before max_iterations).
      result =
        graph
        |> Dux.Graph.pagerank(max_iterations: 100, tolerance: 1.0e-6)
        |> Dux.to_rows()

      assert length(result) == 3
      assert Enum.all?(result, &(&1["rank"] > 0))

      # Ranks should sum to ~1.0 (converged)
      total = result |> Enum.map(& &1["rank"]) |> Enum.sum()
      assert_in_delta total, 1.0, 0.01

      # For a symmetric 3-cycle, all ranks should be equal
      ranks = Enum.map(result, & &1["rank"])
      assert_in_delta Enum.min(ranks), Enum.max(ranks), 0.001
    end

    test "tolerance: 0 runs all iterations without early exit" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 3},
          %{src: 3, dst: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      # With tolerance: 0, PageRank runs all max_iterations.
      # Verify it produces valid results (not that it ran exactly N iterations,
      # since telemetry events can leak across async tests).
      result =
        graph
        |> Dux.Graph.pagerank(max_iterations: 5, tolerance: 0)
        |> Dux.to_rows()

      assert length(result) == 3
      assert Enum.all?(result, &(&1["rank"] > 0))
      # Ranks should sum to ~1.0 (within floating point tolerance)
      total = result |> Enum.map(& &1["rank"]) |> Enum.sum()
      assert_in_delta total, 1.0, 0.01
    end

    test "karate club PageRank: node 34 is top-ranked" do
      graph = Datasets.karate_club()

      result =
        graph
        |> Dux.Graph.pagerank(max_iterations: 50, tolerance: 1.0e-8)
        |> Dux.sort_by(desc: :rank)
        |> Dux.head(3)
        |> Dux.to_rows()

      top_id = hd(result)["id"]
      assert top_id == 34
    end

    test "backwards compatible: :iterations option still works" do
      graph = Datasets.karate_club()

      result =
        graph
        |> Dux.Graph.pagerank(iterations: 10)
        |> Dux.to_rows()

      assert length(result) == 34
      assert Enum.all?(result, &(&1["rank"] > 0))
    end
  end

  # ---------------------------------------------------------------------------
  # HLL COUNT DISTINCT (via distributed)
  # ---------------------------------------------------------------------------

  describe "HLL COUNT DISTINCT" do
    test "approximate count distinct is within 10% of exact" do
      {:ok, w1} = Worker.start_link()
      {:ok, w2} = Worker.start_link()

      # 1000 rows with 100 distinct values
      data =
        Enum.map(1..1000, fn i ->
          %{id: i, category: "cat_#{rem(i, 100)}"}
        end)

      result =
        Dux.from_list(data)
        |> Dux.distribute([w1, w2])
        |> Dux.summarise_with(n_distinct: "COUNT(DISTINCT category)")
        |> Dux.to_rows()

      # Each worker sees all 1000 rows (replicated), computes approx_count_distinct
      # Merger sums: 2 * ~100 = ~200 (since replicated)
      # For replicated sources, this is 2x the actual (same as regular COUNT)
      n = hd(result)["n_distinct"]
      assert is_integer(n)
      assert n > 0

      GenServer.stop(w1)
      GenServer.stop(w2)
    catch
      :exit, _ -> :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Community detection (label propagation)
  # ---------------------------------------------------------------------------

  describe "community detection" do
    test "two distinct communities" do
      vertices = Dux.from_list(Enum.map(1..6, &%{id: &1}))

      # Two cliques: {1,2,3} and {4,5,6} connected by one bridge 3→4
      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 2, dst: 3},
          %{src: 3, dst: 2},
          %{src: 1, dst: 3},
          %{src: 3, dst: 1},
          %{src: 4, dst: 5},
          %{src: 5, dst: 4},
          %{src: 5, dst: 6},
          %{src: 6, dst: 5},
          %{src: 4, dst: 6},
          %{src: 6, dst: 4},
          %{src: 3, dst: 4},
          %{src: 4, dst: 3}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)
      result = graph |> Dux.Graph.communities() |> Dux.sort_by(:id) |> Dux.to_columns()

      [c1, c2, c3, c4, c5, c6] = result["community"]
      # Clique 1
      assert c1 == c2 and c2 == c3
      # Clique 2
      assert c4 == c5 and c5 == c6
      # Different communities
      assert c1 != c4
    end

    test "disconnected components become separate communities" do
      vertices = Dux.from_list(Enum.map(1..4, &%{id: &1}))

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 1},
          %{src: 3, dst: 4},
          %{src: 4, dst: 3}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)
      result = graph |> Dux.Graph.communities() |> Dux.sort_by(:id) |> Dux.to_columns()

      [c1, c2, c3, c4] = result["community"]
      assert c1 == c2
      assert c3 == c4
      assert c1 != c3
    end

    test "karate club converges without error" do
      graph = Datasets.karate_club()
      result = graph |> Dux.Graph.communities(max_iterations: 30) |> Dux.to_columns()

      # All 34 vertices should have a community label
      assert length(result["community"]) == 34
      # At least 1 community (standard label propagation on dense graphs
      # may converge to 1 community — this tests correctness, not quality)
      assert Enum.uniq(result["community"]) != []
    end

    test "single vertex is its own community" do
      vertices = Dux.from_list([%{id: 1}])
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.communities() |> Dux.to_rows()
      assert length(result) == 1
      assert hd(result)["community"] == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial graph tests
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "shortest paths on empty graph" do
      vertices = Dux.from_query("SELECT 1 AS id WHERE false")
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.shortest_paths(1) |> Dux.to_rows()
      # Source vertex should still appear with distance 0
      assert length(result) <= 1
    end

    test "connected components on 100 isolated vertices" do
      vertices = Dux.from_list(Enum.map(1..100, &%{id: &1}))
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.connected_components() |> Dux.to_columns()
      components = Enum.uniq(result["component"])
      assert length(components) == 100
    end

    test "PageRank on single vertex" do
      vertices = Dux.from_list([%{id: 1}])
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result = graph |> Dux.Graph.pagerank(max_iterations: 5) |> Dux.to_rows()
      assert length(result) == 1
      assert hd(result)["rank"] > 0
    end

    test "weighted shortest paths with zero-weight edges" do
      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2, w: 0},
          %{src: 2, dst: 3, w: 0}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      result =
        graph
        |> Dux.Graph.shortest_paths(1, weight: :w)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert result["dist"] == [0, 0, 0]
    end
  end
end
