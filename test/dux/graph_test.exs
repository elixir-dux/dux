defmodule Dux.GraphTest do
  use ExUnit.Case, async: false

  # ---------------------------------------------------------------------------
  # Test fixtures
  # ---------------------------------------------------------------------------

  # Triangle: 1→2, 2→3, 3→1
  defp triangle_graph do
    vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])

    edges =
      Dux.from_list([
        %{"src" => 1, "dst" => 2},
        %{"src" => 2, "dst" => 3},
        %{"src" => 3, "dst" => 1}
      ])

    Dux.Graph.new(vertices: vertices, edges: edges)
  end

  # Star: center=1, spokes to 2,3,4,5
  defp star_graph do
    vertices = Dux.from_list(Enum.map(1..5, &%{"id" => &1}))

    edges =
      Dux.from_list([
        %{"src" => 1, "dst" => 2},
        %{"src" => 1, "dst" => 3},
        %{"src" => 1, "dst" => 4},
        %{"src" => 1, "dst" => 5}
      ])

    Dux.Graph.new(vertices: vertices, edges: edges)
  end

  # Two disconnected components: {1,2,3} and {4,5}
  defp two_components_graph do
    vertices = Dux.from_list(Enum.map(1..5, &%{"id" => &1}))

    edges =
      Dux.from_list([
        %{"src" => 1, "dst" => 2},
        %{"src" => 2, "dst" => 3},
        %{"src" => 2, "dst" => 1},
        %{"src" => 3, "dst" => 2},
        %{"src" => 4, "dst" => 5},
        %{"src" => 5, "dst" => 4}
      ])

    Dux.Graph.new(vertices: vertices, edges: edges)
  end

  # Linear chain: 1→2→3→4→5
  defp chain_graph do
    vertices = Dux.from_list(Enum.map(1..5, &%{"id" => &1}))

    edges =
      Dux.from_list([
        %{"src" => 1, "dst" => 2},
        %{"src" => 2, "dst" => 3},
        %{"src" => 3, "dst" => 4},
        %{"src" => 4, "dst" => 5}
      ])

    Dux.Graph.new(vertices: vertices, edges: edges)
  end

  # ---------------------------------------------------------------------------
  # Happy path: construction
  # ---------------------------------------------------------------------------

  describe "new/1" do
    test "creates graph with default column names" do
      graph = triangle_graph()
      assert graph.vertex_id == "id"
      assert graph.edge_src == "src"
      assert graph.edge_dst == "dst"
    end

    test "creates graph with custom column names" do
      vertices = Dux.from_list([%{"node" => 1}])
      edges = Dux.from_list([%{"from" => 1, "to" => 1}])

      graph =
        Dux.Graph.new(
          vertices: vertices,
          edges: edges,
          vertex_id: :node,
          edge_src: :from,
          edge_dst: :to
        )

      assert graph.vertex_id == "node"
      assert graph.edge_src == "from"
      assert graph.edge_dst == "to"
    end
  end

  describe "from_edgelist/2" do
    test "infers vertices from edges" do
      edges =
        Dux.from_list([
          %{"src" => 1, "dst" => 2},
          %{"src" => 2, "dst" => 3}
        ])

      graph = Dux.Graph.from_edgelist(edges)
      vertex_count = Dux.Graph.vertex_count(graph)
      assert vertex_count == 3
    end
  end

  describe "vertex_count/1 and edge_count/1" do
    test "returns correct counts" do
      graph = triangle_graph()
      assert Dux.Graph.vertex_count(graph) == 3
      assert Dux.Graph.edge_count(graph) == 3
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path: degree functions
  # ---------------------------------------------------------------------------

  describe "out_degree/1" do
    test "triangle graph" do
      result =
        triangle_graph()
        |> Dux.Graph.out_degree()
        |> Dux.sort_by(:id)
        |> Dux.to_columns()

      assert result["id"] == [1, 2, 3]
      assert result["out_degree"] == [1, 1, 1]
    end

    test "star graph — center has high out-degree" do
      result =
        star_graph()
        |> Dux.Graph.out_degree()
        |> Dux.sort_by(:id)
        |> Dux.to_columns()

      assert result["id"] == [1]
      assert result["out_degree"] == [4]
    end
  end

  describe "in_degree/1" do
    test "star graph — leaves have in-degree 1" do
      result =
        star_graph()
        |> Dux.Graph.in_degree()
        |> Dux.sort_by(:id)
        |> Dux.to_columns()

      assert result["id"] == [2, 3, 4, 5]
      assert result["in_degree"] == [1, 1, 1, 1]
    end
  end

  describe "degree/1" do
    test "triangle graph — all vertices have degree 2" do
      result =
        triangle_graph()
        |> Dux.Graph.degree()
        |> Dux.sort_by(:id)
        |> Dux.to_columns()

      assert result["id"] == [1, 2, 3]
      assert result["degree"] == [2, 2, 2]
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path: PageRank
  # ---------------------------------------------------------------------------

  describe "pagerank/2" do
    test "triangle graph — all vertices roughly equal rank" do
      result =
        triangle_graph()
        |> Dux.Graph.pagerank(iterations: 10)
        |> Dux.sort_by(:id)
        |> Dux.to_rows()

      ranks = Enum.map(result, & &1["rank"])

      # In a symmetric cycle, all ranks should converge to 1/3
      for rank <- ranks do
        assert_in_delta rank, 1 / 3, 0.01
      end
    end

    test "star graph — center has highest rank" do
      graph = star_graph()

      # Add return edges so rank flows back
      edges =
        Dux.from_list([
          %{"src" => 1, "dst" => 2},
          %{"src" => 1, "dst" => 3},
          %{"src" => 1, "dst" => 4},
          %{"src" => 1, "dst" => 5},
          %{"src" => 2, "dst" => 1},
          %{"src" => 3, "dst" => 1},
          %{"src" => 4, "dst" => 1},
          %{"src" => 5, "dst" => 1}
        ])

      graph = %{graph | edges: edges}

      result =
        graph
        |> Dux.Graph.pagerank(iterations: 10)
        |> Dux.sort_by(desc: :rank)
        |> Dux.to_rows()

      # Center vertex should have highest rank
      assert hd(result)["id"] == 1
    end

    test "pagerank values sum to approximately 1" do
      result =
        triangle_graph()
        |> Dux.Graph.pagerank(iterations: 5)
        |> Dux.to_rows()

      total = Enum.sum(Enum.map(result, & &1["rank"]))
      assert_in_delta total, 1.0, 0.01
    end

    test "custom damping factor" do
      result =
        triangle_graph()
        |> Dux.Graph.pagerank(damping: 0.5, iterations: 20)
        |> Dux.to_rows()

      assert length(result) == 3
      assert Enum.all?(result, fn row -> row["rank"] > 0 end)
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path: Shortest paths
  # ---------------------------------------------------------------------------

  describe "shortest_paths/2" do
    test "chain graph — distances are hop counts" do
      result =
        chain_graph()
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert result["node"] == [1, 2, 3, 4, 5]
      assert result["dist"] == [0, 1, 2, 3, 4]
    end

    test "triangle graph — max distance is 2" do
      result =
        triangle_graph()
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      assert result["node"] == [1, 2, 3]
      assert result["dist"] == [0, 1, 2]
    end

    test "disconnected vertices not reached" do
      result =
        two_components_graph()
        |> Dux.Graph.shortest_paths(1)
        |> Dux.sort_by(:node)
        |> Dux.to_columns()

      # Only vertices 1,2,3 are reachable from 1
      assert 4 not in result["node"]
      assert 5 not in result["node"]
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path: Connected components
  # ---------------------------------------------------------------------------

  describe "connected_components/1" do
    test "two disconnected components" do
      result =
        two_components_graph()
        |> Dux.Graph.connected_components()
        |> Dux.sort_by(:id)
        |> Dux.to_columns()

      assert result["id"] == [1, 2, 3, 4, 5]
      # Vertices 1,2,3 should share one component, 4,5 another
      [c1, c2, c3, c4, c5] = result["component"]
      assert c1 == c2 and c2 == c3
      assert c4 == c5
      assert c1 != c4
    end

    test "fully connected graph — one component" do
      result =
        triangle_graph()
        |> Dux.Graph.connected_components()
        |> Dux.to_columns()

      components = result["component"] |> Enum.uniq()
      assert length(components) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path: Triangle counting
  # ---------------------------------------------------------------------------

  describe "triangle_count/1" do
    test "single triangle (bidirectional edges)" do
      vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])

      edges =
        Dux.from_list([
          %{"src" => 1, "dst" => 2},
          %{"src" => 2, "dst" => 1},
          %{"src" => 2, "dst" => 3},
          %{"src" => 3, "dst" => 2},
          %{"src" => 1, "dst" => 3},
          %{"src" => 3, "dst" => 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)
      assert Dux.Graph.triangle_count(graph) == 1
    end

    test "no triangles in a chain" do
      assert Dux.Graph.triangle_count(chain_graph()) == 0
    end

    test "star graph has no triangles" do
      assert Dux.Graph.triangle_count(star_graph()) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "pagerank on graph with no edges" do
      vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}])
      edges = Dux.from_query("SELECT 1 AS src, 2 AS dst WHERE false")
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      # Should not crash — vertices with no incoming edges get base rank
      result = Dux.Graph.pagerank(graph, iterations: 5) |> Dux.to_rows()
      assert length(result) == 2
    end

    test "shortest_paths from isolated vertex" do
      result =
        two_components_graph()
        |> Dux.Graph.shortest_paths(4)
        |> Dux.to_columns()

      # Should only reach 4 and 5
      assert length(result["node"]) == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "self-loops don't crash algorithms" do
      vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}])
      edges = Dux.from_list([%{"src" => 1, "dst" => 1}, %{"src" => 1, "dst" => 2}])
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      # Should work without infinite loops
      assert Dux.Graph.out_degree(graph) |> Dux.n_rows() >= 1
      result = Dux.Graph.pagerank(graph, iterations: 10) |> Dux.to_rows()
      assert length(result) == 2
    end

    test "duplicate edges handled gracefully" do
      vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}])

      edges =
        Dux.from_list([
          %{"src" => 1, "dst" => 2},
          %{"src" => 1, "dst" => 2},
          %{"src" => 1, "dst" => 2}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)
      out_deg = Dux.Graph.out_degree(graph) |> Dux.to_rows()
      # Duplicate edges count as separate edges
      assert hd(out_deg)["out_degree"] == 3
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "medium graph — 100 vertices, ~200 edges" do
      vertices = Dux.from_query("SELECT x AS id FROM range(100) t(x)")

      edges =
        Dux.from_query("""
          SELECT x AS src, (x + 1) % 100 AS dst FROM range(100) t(x)
          UNION ALL
          SELECT x AS src, (x + 7) % 100 AS dst FROM range(100) t(x)
        """)

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      # PageRank should converge
      result = Dux.Graph.pagerank(graph, iterations: 10) |> Dux.to_rows()
      assert length(result) == 100

      total_rank = Enum.sum(Enum.map(result, & &1["rank"]))
      assert_in_delta total_rank, 1.0, 0.05

      # Shortest paths from vertex 0
      paths = Dux.Graph.shortest_paths(graph, 0) |> Dux.n_rows()
      assert paths == 100
    end

    test "graph algorithms compose with Dux verbs" do
      graph = triangle_graph()

      # PageRank → filter → sort → collect
      result =
        graph
        |> Dux.Graph.pagerank(iterations: 10)
        |> Dux.filter_with("rank > 0.1")
        |> Dux.sort_by(desc: :rank)
        |> Dux.to_rows()

      assert length(result) == 3
    end
  end
end
