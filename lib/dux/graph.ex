defmodule Dux.Graph do
  import Dux.SQL.Helpers, only: [qi: 1]
  alias Dux.Remote.{Coordinator, Worker}

  @moduledoc """
  Graph analytics on Dux dataframes.

  A graph is two Dux structs — vertices and edges. Every graph algorithm
  reduces to joins, aggregations, and iterations on those tables. DuckDB
  provides native acceleration via recursive CTEs for path algorithms.

  ## Creating a graph

      vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])
      edges = Dux.from_list([%{"src" => 1, "dst" => 2}, %{"src" => 2, "dst" => 3}])
      graph = Dux.Graph.new(vertices: vertices, edges: edges)

  ## Algorithms

  All algorithms are compositions of Dux verbs — they return `%Dux{}` structs
  that you can pipe into further operations.

      graph
      |> Dux.Graph.pagerank()
      |> Dux.sort_by(desc: :rank)
      |> Dux.head(10)
      |> Dux.to_rows()
  """

  defstruct [:vertices, :edges, :vertex_id, :edge_src, :edge_dst, :workers]

  @type t :: %__MODULE__{
          vertices: Dux.t(),
          edges: Dux.t(),
          vertex_id: String.t(),
          edge_src: String.t(),
          edge_dst: String.t()
        }

  @doc """
  Create a new graph from vertex and edge dataframes.

  ## Options

    * `:vertex_id` - column name for vertex IDs (default: `:id`)
    * `:edge_src` - column name for edge source (default: `:src`)
    * `:edge_dst` - column name for edge destination (default: `:dst`)

  ## Examples

      vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])
      edges = Dux.from_list([%{"src" => 1, "dst" => 2}, %{"src" => 2, "dst" => 3}])
      graph = Dux.Graph.new(vertices: vertices, edges: edges)
  """
  def new(opts) do
    %__MODULE__{
      vertices: Keyword.fetch!(opts, :vertices),
      edges: Keyword.fetch!(opts, :edges),
      vertex_id: to_string(Keyword.get(opts, :vertex_id, :id)),
      edge_src: to_string(Keyword.get(opts, :edge_src, :src)),
      edge_dst: to_string(Keyword.get(opts, :edge_dst, :dst))
    }
  end

  @doc """
  Create a graph from an edge list, inferring vertices from unique source and destination nodes.

  This is a convenience constructor when you only have edges. Vertices are
  derived by taking the union of all distinct `src` and `dst` values.

  ## Options

    * `:edge_src` - column name for edge source (default: `:src`)
    * `:edge_dst` - column name for edge destination (default: `:dst`)
    * `:vertex_id` - column name for the inferred vertex ID (default: `:id`)

  ## Examples

      iex> edges = Dux.from_list([%{"src" => 1, "dst" => 2}, %{"src" => 2, "dst" => 3}])
      iex> graph = Dux.Graph.from_edgelist(edges)
      iex> Dux.Graph.vertex_count(graph)
      3
  """
  def from_edgelist(%Dux{} = edges, opts \\ []) do
    src_col = to_string(Keyword.get(opts, :edge_src, :src))
    dst_col = to_string(Keyword.get(opts, :edge_dst, :dst))
    id_col = to_string(Keyword.get(opts, :vertex_id, :id))

    conn = Dux.Connection.get_conn()
    {edges_sql, _} = Dux.QueryBuilder.build(edges, conn)

    vertices_sql = """
      SELECT DISTINCT #{qi(id_col)} FROM (
        SELECT #{qi(src_col)} AS #{qi(id_col)} FROM (#{edges_sql}) __e1
        UNION
        SELECT #{qi(dst_col)} AS #{qi(id_col)} FROM (#{edges_sql}) __e2
      ) __combined
    """

    %__MODULE__{
      vertices: Dux.from_query(vertices_sql),
      edges: edges,
      vertex_id: id_col,
      edge_src: src_col,
      edge_dst: dst_col
    }
  end

  @doc """
  Mark a graph for distributed execution across the given workers.

  All subsequent algorithms will automatically use the distributed path.

      graph = Dux.Graph.new(vertices: v, edges: e)
              |> Dux.Graph.distribute(workers)

      graph |> Dux.Graph.pagerank()  # automatically distributed
  """
  def distribute(%__MODULE__{} = graph, workers) when is_list(workers) do
    %{graph | workers: workers}
  end

  # ---------------------------------------------------------------------------
  # Degree functions
  # ---------------------------------------------------------------------------

  @doc """
  Compute the out-degree of each vertex.

  Returns a `%Dux{}` with columns `[vertex_id, out_degree]`.

  ## Examples

      iex> edges = Dux.from_list([%{"src" => 1, "dst" => 2}, %{"src" => 1, "dst" => 3}, %{"src" => 2, "dst" => 3}])
      iex> graph = Dux.Graph.new(vertices: Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]), edges: edges)
      iex> Dux.Graph.out_degree(graph) |> Dux.sort_by(:id) |> Dux.to_columns()
      %{"id" => [1, 2], "out_degree" => [2, 1]}
  """
  def out_degree(%__MODULE__{} = graph) do
    graph.edges
    |> Dux.group_by(String.to_atom(graph.edge_src))
    |> Dux.summarise_with(out_degree: "COUNT(*)")
    |> Dux.rename([{String.to_atom(graph.edge_src), String.to_atom(graph.vertex_id)}])
  end

  @doc """
  Compute the in-degree of each vertex.

  Returns a `%Dux{}` with columns `[vertex_id, in_degree]`.

  ## Examples

      iex> edges = Dux.from_list([%{"src" => 1, "dst" => 2}, %{"src" => 1, "dst" => 3}, %{"src" => 2, "dst" => 3}])
      iex> graph = Dux.Graph.new(vertices: Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]), edges: edges)
      iex> Dux.Graph.in_degree(graph) |> Dux.sort_by(:id) |> Dux.to_columns()
      %{"id" => [2, 3], "in_degree" => [1, 2]}
  """
  def in_degree(%__MODULE__{} = graph) do
    graph.edges
    |> Dux.group_by(String.to_atom(graph.edge_dst))
    |> Dux.summarise_with(in_degree: "COUNT(*)")
    |> Dux.rename([{String.to_atom(graph.edge_dst), String.to_atom(graph.vertex_id)}])
  end

  @doc """
  Compute the total degree (in-degree + out-degree) of each vertex.

  Returns a `%Dux{}` with columns `[vertex_id, degree]`. Each edge contributes
  1 to both the source vertex's degree and the destination vertex's degree.

  ## Examples

      iex> edges = Dux.from_list([%{"src" => 1, "dst" => 2}, %{"src" => 2, "dst" => 3}])
      iex> graph = Dux.Graph.new(vertices: Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]), edges: edges)
      iex> Dux.Graph.degree(graph) |> Dux.sort_by(:id) |> Dux.to_columns()
      %{"degree" => [1, 2, 1], "id" => [1, 2, 3]}
  """
  def degree(%__MODULE__{} = graph) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    conn = Dux.Connection.get_conn()
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, conn)

    sql = """
      SELECT #{qi(vid)}, CAST(COUNT(*) AS BIGINT) AS degree FROM (
        SELECT #{qi(src)} AS #{qi(vid)} FROM (#{edges_sql}) __e1
        UNION ALL
        SELECT #{qi(dst)} AS #{qi(vid)} FROM (#{edges_sql}) __e2
      ) __all_edges
      GROUP BY #{qi(vid)}
    """

    Dux.from_query(sql)
  end

  # ---------------------------------------------------------------------------
  # PageRank
  # ---------------------------------------------------------------------------

  @doc """
  Compute PageRank scores for all vertices.

  Returns a `%Dux{}` with columns `[vertex_id, rank]`.

  ## Options

    * `:damping` - damping factor (default: `0.85`)
    * `:iterations` - number of iterations (default: `20`)
    * `:workers` - list of worker PIDs for distributed execution (default: `nil` for local)

  When `workers` is provided, uses the broadcast-iterate pattern: each iteration
  broadcasts current ranks to all workers, workers compute local contributions,
  coordinator merges.

  ## Examples

      iex> edges = Dux.from_list([
      ...>   %{"src" => 1, "dst" => 2},
      ...>   %{"src" => 2, "dst" => 3},
      ...>   %{"src" => 3, "dst" => 1},
      ...>   %{"src" => 3, "dst" => 2}
      ...> ])
      iex> vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])
      iex> graph = Dux.Graph.new(vertices: vertices, edges: edges)
      iex> result = Dux.Graph.pagerank(graph) |> Dux.sort_by(:id) |> Dux.to_rows()
      iex> length(result) == 3
      true
      iex> Enum.all?(result, fn row -> row["rank"] > 0 end)
      true
  """
  def pagerank(%__MODULE__{} = graph, opts \\ []) do
    damping = Keyword.get(opts, :damping, 0.85)
    iterations = Keyword.get(opts, :max_iterations, Keyword.get(opts, :iterations, 100))
    tolerance = Keyword.get(opts, :tolerance, 1.0e-6)
    workers = graph.workers

    meta = %{
      algorithm: :pagerank,
      n_vertices: vertex_count(graph),
      n_edges: edge_count(graph),
      distributed: workers != nil
    }

    :telemetry.span([:dux, :graph, :algorithm], meta, fn ->
      result =
        if workers do
          pagerank_distributed(graph, damping, iterations, workers)
        else
          pagerank_local(graph, damping, iterations, tolerance)
        end

      {result, meta}
    end)
  end

  defp pagerank_local(graph, damping, iterations, tolerance) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    n =
      graph.vertices
      |> Dux.summarise_with(n: "COUNT(*)")
      |> Dux.to_rows()
      |> hd()
      |> Map.get("n")

    out_deg = out_degree(graph) |> Dux.compute()
    initial_rank = 1.0 / n

    ranks =
      graph.vertices
      |> Dux.select([String.to_atom(vid)])
      |> Dux.mutate_with(rank: "#{initial_rank}")
      |> Dux.compute()

    base_rank = (1.0 - damping) / n

    {final, _kept_out_deg, _kept_prev} =
      Enum.reduce_while(1..iterations, {ranks, out_deg, [ranks]}, fn i, {ranks, out_deg, prev} ->
        conn = Dux.Connection.get_conn()
        Process.put(:dux_compute_ref, {ranks.source, out_deg.source})

        {:table, ranks_ref} = ranks.source
        ranks_table = ranks_ref.name

        {:table, outdeg_ref} = out_deg.source
        outdeg_table = outdeg_ref.name

        {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, conn)
        {verts_sql, _} = Dux.QueryBuilder.build(graph.vertices, conn)

        sql = """
          WITH
            edges AS (#{edges_sql}),
            verts AS (#{verts_sql}),
            contributions AS (
              SELECT
                e.#{qi(dst)} AS #{qi(vid)},
                SUM(r."rank" / od."out_degree") AS incoming
              FROM edges e
              JOIN "#{ranks_table}" r ON e.#{qi(src)} = r.#{qi(vid)}
              JOIN "#{outdeg_table}" od ON e.#{qi(src)} = od.#{qi(vid)}
              GROUP BY e.#{qi(dst)}
            )
          SELECT
            v.#{qi(vid)},
            COALESCE(#{base_rank} + #{damping} * c.incoming, #{base_rank}) AS "rank"
          FROM verts v
          LEFT JOIN contributions c ON v.#{qi(vid)} = c.#{qi(vid)}
        """

        new_ranks = Dux.from_query(sql) |> Dux.compute()
        Process.delete(:dux_compute_ref)

        # Convergence check: L1 norm of rank change
        converged =
          if tolerance > 0 and i > 1 do
            delta_sql = """
              SELECT SUM(ABS(n."rank" - o."rank")) AS delta
              FROM "#{new_ranks.source |> elem(1) |> Map.get(:name)}" n
              JOIN "#{ranks_table}" o ON n.#{qi(vid)} = o.#{qi(vid)}
            """

            delta_result = Dux.from_query(delta_sql) |> Dux.to_rows()
            delta = hd(delta_result)["delta"] || 0
            delta < tolerance
          else
            false
          end

        :telemetry.execute([:dux, :graph, :iteration, :stop], %{iteration: i}, %{
          algorithm: :pagerank,
          iteration: i,
          max_iterations: iterations,
          converged: converged
        })

        if converged do
          {:halt, {new_ranks, out_deg, [new_ranks | prev]}}
        else
          {:cont, {new_ranks, out_deg, [new_ranks | prev]}}
        end
      end)

    final
  end

  defp pagerank_distributed(graph, damping, iterations, workers) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    n =
      graph.vertices
      |> Dux.summarise_with(n: "COUNT(*)")
      |> Dux.to_rows()
      |> hd()
      |> Map.get("n")

    initial_rank = 1.0 / n
    base_rank = (1.0 - damping) / n

    # Initialize ranks
    ranks =
      graph.vertices
      |> Dux.select([String.to_atom(vid)])
      |> Dux.mutate_with(rank: "#{initial_rank}")
      |> Dux.compute()

    # Compute out-degree and serialize to IPC ONCE.
    # IPC binaries are plain BEAM binaries — immune to the ResourceArc GC race
    # that affects DuxTableRef. This avoids the flaky GC issue where out_deg's
    # temp table gets collected between iterations under test GC pressure.
    out_deg = out_degree(graph) |> Dux.compute()
    {:table, outdeg_ref} = out_deg.source
    conn = Dux.Connection.get_conn()
    outdeg_ipc = Dux.Backend.table_to_ipc(conn, outdeg_ref)

    result =
      Enum.reduce(1..iterations, ranks, fn _i, ranks ->
        # Serialize current ranks each iteration (ranks changes)
        {:table, ranks_ref} = ranks.source
        conn = Dux.Connection.get_conn()
        ranks_ipc = Dux.Backend.table_to_ipc(conn, ranks_ref)

        stage = :erlang.unique_integer([:positive])

        broadcast_to_workers(workers, [
          {"__pr_ranks_#{stage}", ranks_ipc},
          {"__pr_outdeg_#{stage}", outdeg_ipc}
        ])

        # Each worker computes contributions from its copy of the edges
        contribution_pipeline =
          graph.edges
          |> Dux.join(
            Dux.from_query(~s(SELECT * FROM "__pr_ranks_#{stage}")),
            on: [{String.to_atom(src), String.to_atom(vid)}]
          )
          |> Dux.join(
            Dux.from_query(~s(SELECT * FROM "__pr_outdeg_#{stage}")),
            on: [{String.to_atom(src), String.to_atom(vid)}]
          )
          |> Dux.mutate_with(contribution: "\"rank\" / \"out_degree\"")
          |> Dux.group_by(String.to_atom(dst))
          |> Dux.summarise_with(incoming: "SUM(contribution)")

        # Fan out, merge contributions, then rename on coordinator
        contributions =
          Coordinator.execute(contribution_pipeline, workers: workers)
          |> Dux.rename([{String.to_atom(dst), String.to_atom(vid)}])
          |> Dux.compute()

        # Compute new ranks on coordinator: base_rank + damping * incoming
        # Left join with vertices to ensure all vertices get a rank
        new_ranks =
          graph.vertices
          |> Dux.select([String.to_atom(vid)])
          |> Dux.join(contributions, on: String.to_atom(vid), how: :left)
          |> Dux.mutate_with(rank: "COALESCE(#{base_rank} + #{damping} * incoming, #{base_rank})")
          |> Dux.select([String.to_atom(vid), :rank])
          |> Dux.compute()

        # Cleanup broadcast tables
        Enum.each(workers, fn w ->
          try do
            Worker.drop_table(w, "__pr_ranks_#{stage}")
            Worker.drop_table(w, "__pr_outdeg_#{stage}")
          catch
            _, _ -> :ok
          end
        end)

        new_ranks
      end)

    result
  end

  # ---------------------------------------------------------------------------
  # Shortest paths (via recursive CTE)
  # ---------------------------------------------------------------------------

  @doc """
  Find the shortest path distance from a source vertex to all reachable vertices.

  Uses DuckDB's `USING KEY` recursive CTEs for efficient graph traversal with
  automatic deduplication — only the shortest distance per node is kept. Returns
  a `%Dux{}` with columns `[node, dist]`.

  ## Options

    * `:max_depth` — maximum traversal depth (default: `1000`)
    * `:weight` — edge column to use as weight (default: `nil` for unweighted BFS).
      When set, computes weighted shortest paths (Bellman-Ford via SQL).

  ## Examples

      iex> edges = Dux.from_list([
      ...>   %{"src" => 1, "dst" => 2},
      ...>   %{"src" => 2, "dst" => 3},
      ...>   %{"src" => 1, "dst" => 3}
      ...> ])
      iex> vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])
      iex> graph = Dux.Graph.new(vertices: vertices, edges: edges)
      iex> Dux.Graph.shortest_paths(graph, 1) |> Dux.sort_by(:node) |> Dux.to_columns()
      %{"dist" => [0, 1, 1], "node" => [1, 2, 3]}
  """
  def shortest_paths(%__MODULE__{} = graph, from_vertex, opts \\ []) do
    workers = graph.workers
    max_depth = Keyword.get(opts, :max_depth, 1000)
    weight = Keyword.get(opts, :weight)

    meta = %{
      algorithm: :shortest_paths,
      n_vertices: vertex_count(graph),
      n_edges: edge_count(graph),
      distributed: workers != nil
    }

    :telemetry.span([:dux, :graph, :algorithm], meta, fn ->
      result =
        if workers do
          shortest_paths_distributed(graph, from_vertex, max_depth, weight, workers)
        else
          shortest_paths_local(graph, from_vertex, max_depth, weight)
        end

      {result, meta}
    end)
  end

  defp shortest_paths_local(graph, from_vertex, max_depth, weight) do
    src = graph.edge_src
    dst = graph.edge_dst

    conn = Dux.Connection.get_conn()
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, conn)

    # Cost expression: +1 for unweighted, +weight column for weighted
    cost_expr =
      if weight do
        "p.dist + e.#{qi(to_string(weight))}"
      else
        "p.dist + 1"
      end

    sql =
      if weight do
        # Weighted: use UNION + GROUP BY MIN (Bellman-Ford).
        # USING KEY has a limitation with weighted graphs where multiple paths
        # of different weights reach the same node in the same iteration.
        """
        WITH RECURSIVE
          edges_cte AS (#{edges_sql}),
          paths AS (
            SELECT #{from_vertex} AS node, 0 AS dist
            UNION
            SELECT e.#{qi(dst)} AS node, #{cost_expr}
            FROM paths p
            JOIN edges_cte e ON p.node = e.#{qi(src)}
            WHERE #{cost_expr} <= #{max_depth}
          )
        SELECT node, MIN(dist) AS dist FROM paths GROUP BY node
        """
      else
        # Unweighted: USING KEY eliminates duplicate rows automatically.
        # 1000x intermediate row reduction vs plain UNION.
        """
        WITH RECURSIVE
          edges_cte AS (#{edges_sql}),
          paths(node, dist) USING KEY (node) AS (
            SELECT #{from_vertex} AS node, 0 AS dist
            UNION
            (SELECT e.#{qi(dst)} AS node, #{cost_expr} AS dist
             FROM paths p
             JOIN edges_cte e ON p.node = e.#{qi(src)}
             LEFT JOIN recurring.paths AS rec ON rec.node = e.#{qi(dst)}
             WHERE #{cost_expr} < COALESCE(rec.dist, #{max_depth} + 1))
          )
        SELECT * FROM paths
        """
      end

    Dux.from_query(sql)
  end

  # Distributed: broadcast edges to one worker, run recursive CTE there.
  # BFS needs the full edge set visible, so we offload to a worker's DuckDB.
  defp shortest_paths_distributed(graph, from_vertex, max_depth, weight, workers) do
    src = graph.edge_src
    dst = graph.edge_dst
    worker = hd(workers)

    edges_computed = Dux.compute(graph.edges)
    {:table, edges_ref} = edges_computed.source
    conn = Dux.Connection.get_conn()
    edges_ipc = Dux.Backend.table_to_ipc(conn, edges_ref)

    Worker.register_table(worker, "__bfs_edges", edges_ipc)

    cost_expr =
      if weight do
        "p.dist + e.#{qi(to_string(weight))}"
      else
        "p.dist + 1"
      end

    sql =
      if weight do
        """
        WITH RECURSIVE paths AS (
            SELECT #{from_vertex} AS node, 0 AS dist
            UNION
            SELECT e.#{qi(dst)} AS node, #{cost_expr}
            FROM paths p
            JOIN "__bfs_edges" e ON p.node = e.#{qi(src)}
            WHERE #{cost_expr} <= #{max_depth}
          )
        SELECT node, MIN(dist) AS dist FROM paths GROUP BY node
        """
      else
        """
        WITH RECURSIVE paths(node, dist) USING KEY (node) AS (
            SELECT #{from_vertex} AS node, 0 AS dist
            UNION
            (SELECT e.#{qi(dst)} AS node, #{cost_expr} AS dist
             FROM paths p
             JOIN "__bfs_edges" e ON p.node = e.#{qi(src)}
             LEFT JOIN recurring.paths AS rec ON rec.node = e.#{qi(dst)}
             WHERE #{cost_expr} < COALESCE(rec.dist, #{max_depth} + 1))
          )
        SELECT * FROM paths
        """
      end

    {:ok, result_ipc} = Worker.execute(worker, Dux.from_query(sql))
    result = Dux.Backend.table_from_ipc(conn, result_ipc)
    names = Dux.Backend.table_names(conn, result)
    dtypes = Dux.Backend.table_dtypes(conn, result) |> Map.new()

    try do
      Worker.drop_table(worker, "__bfs_edges")
    catch
      _, _ -> :ok
    end

    %Dux{source: {:table, result}, names: names, dtypes: dtypes}
  end

  # ---------------------------------------------------------------------------
  # Connected components
  # ---------------------------------------------------------------------------

  @doc """
  Find connected components using iterative label propagation.

  Each vertex is assigned a component ID (the minimum vertex ID in its component).
  Returns a `%Dux{}` with columns `[vertex_id, component]`.

  ## Options

    * `:max_iterations` - maximum propagation iterations (default: `100`)
    * `:workers` - list of worker PIDs for distributed execution (default: `nil` for local).
      When provided, uses the broadcast-iterate pattern: broadcasts labels and edges
      to workers each iteration.

  ## Examples

      iex> edges = Dux.from_list([
      ...>   %{"src" => 1, "dst" => 2},
      ...>   %{"src" => 2, "dst" => 1},
      ...>   %{"src" => 3, "dst" => 4},
      ...>   %{"src" => 4, "dst" => 3}
      ...> ])
      iex> vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}, %{"id" => 4}])
      iex> graph = Dux.Graph.new(vertices: vertices, edges: edges)
      iex> result = Dux.Graph.connected_components(graph) |> Dux.sort_by(:id) |> Dux.to_columns()
      iex> result["component"]
      [1, 1, 3, 3]
  """
  def connected_components(%__MODULE__{} = graph, opts \\ []) do
    max_iterations = Keyword.get(opts, :max_iterations, 100)
    workers = graph.workers

    meta = %{
      algorithm: :connected_components,
      n_vertices: vertex_count(graph),
      n_edges: edge_count(graph),
      distributed: workers != nil
    }

    :telemetry.span([:dux, :graph, :algorithm], meta, fn ->
      result =
        if workers do
          cc_distributed(graph, max_iterations, workers)
        else
          cc_local(graph, max_iterations)
        end

      {result, meta}
    end)
  end

  defp cc_local(graph, _max_iterations) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst
    conn = Dux.Connection.get_conn()

    {verts_sql, _} = Dux.QueryBuilder.build(graph.vertices, conn)
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, conn)

    # USING KEY eliminates the Elixir iteration loop — DuckDB handles
    # convergence internally. Each vertex keeps the minimum component ID
    # seen through any neighbor.
    sql = """
    WITH RECURSIVE
      bidir_edges AS (
        SELECT #{qi(src)} AS a, #{qi(dst)} AS b FROM (#{edges_sql}) __e
        UNION
        SELECT #{qi(dst)} AS a, #{qi(src)} AS b FROM (#{edges_sql}) __e2
      ),
      cc(#{qi(vid)}, component) USING KEY (#{qi(vid)}) AS (
        SELECT #{qi(vid)}, #{qi(vid)} AS component FROM (#{verts_sql}) __v
        UNION
        (SELECT e.b AS #{qi(vid)}, cc.component
         FROM cc
         JOIN bidir_edges e ON cc.#{qi(vid)} = e.a
         LEFT JOIN recurring.cc AS rec ON rec.#{qi(vid)} = e.b
         WHERE cc.component < COALESCE(rec.component, cc.component + 1))
      )
    SELECT * FROM cc
    """

    Dux.from_query(sql)
  end

  defp cc_distributed(graph, max_iterations, workers) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    labels =
      graph.vertices
      |> Dux.select([String.to_atom(vid)])
      |> Dux.mutate_with(component: ~s(#{qi(vid)}))
      |> Dux.compute()

    # Broadcast edges once (they don't change between iterations)
    edges_computed = Dux.compute(graph.edges)
    {:table, edges_ref} = edges_computed.source
    conn = Dux.Connection.get_conn()
    edges_ipc = Dux.Backend.table_to_ipc(conn, edges_ref)
    broadcast_to_workers(workers, [{"__cc_edges", edges_ipc}])

    result =
      Enum.reduce_while(1..max_iterations, labels, fn _i, labels ->
        {:table, labels_ref} = labels.source
        conn = Dux.Connection.get_conn()
        labels_ipc = Dux.Backend.table_to_ipc(conn, labels_ref)

        stage = :erlang.unique_integer([:positive])
        broadcast_to_workers(workers, [{"__cc_labels_#{stage}", labels_ipc}])

        # Forward propagation: for each edge src→dst, propagate src's label to dst
        # Use the same join-against-broadcast pattern as PageRank
        fwd_pipeline =
          Dux.from_query(~s(SELECT * FROM "__cc_edges"))
          |> Dux.join(
            Dux.from_query(~s(SELECT * FROM "__cc_labels_#{stage}")),
            on: [{String.to_atom(src), String.to_atom(vid)}]
          )
          |> Dux.select([String.to_atom(dst), :component])

        # Reverse propagation: for each edge src→dst, propagate dst's label to src
        rev_pipeline =
          Dux.from_query(~s(SELECT * FROM "__cc_edges"))
          |> Dux.join(
            Dux.from_query(~s(SELECT * FROM "__cc_labels_#{stage}")),
            on: [{String.to_atom(dst), String.to_atom(vid)}]
          )
          |> Dux.select([String.to_atom(src), :component])

        # Fan out to workers, collect results
        fwd_results = fan_out_pipeline(workers, fwd_pipeline)
        rev_results = fan_out_pipeline(workers, rev_pipeline)

        # Collect all IPC results
        all_ipc =
          (fwd_results ++ rev_results)
          |> Enum.flat_map(fn
            {:ok, ipc} -> [ipc]
            _ -> []
          end)

        # Load results + current labels into coordinator DuckDB, take MIN
        conn = Dux.Connection.get_conn()

        input_refs =
          Enum.map(all_ipc, fn ipc ->
            ref = Dux.Backend.table_from_ipc(conn, ipc)
            name = ref.name
            {name, ref}
          end)

        Process.put(:dux_cc_refs, {labels, input_refs, edges_computed})

        {:table, cur_labels_ref} = labels.source
        cur_table = cur_labels_ref.name

        # UNION ALL: current labels + forward neighbor labels (renamed) + reverse neighbor labels (renamed)
        fwd_unions =
          input_refs
          |> Enum.take(length(fwd_results))
          |> Enum.map_join(" UNION ALL ", fn {name, _} ->
            ~s(SELECT #{qi(dst)} AS #{qi(vid)}, component FROM "#{name}")
          end)

        rev_unions =
          input_refs
          |> Enum.drop(length(fwd_results))
          |> Enum.map_join(" UNION ALL ", fn {name, _} ->
            ~s(SELECT #{qi(src)} AS #{qi(vid)}, component FROM "#{name}")
          end)

        parts = [~s(SELECT #{qi(vid)}, component FROM "#{cur_table}")]
        parts = if fwd_unions != "", do: parts ++ [fwd_unions], else: parts
        parts = if rev_unions != "", do: parts ++ [rev_unions], else: parts

        union_sql = Enum.join(parts, " UNION ALL ")

        merge_sql = """
          SELECT #{qi(vid)}, MIN(component) AS component
          FROM (#{union_sql}) __all
          GROUP BY #{qi(vid)}
        """

        merge_ref = Dux.Backend.query(conn, merge_sql)
        merge_names = Dux.Backend.table_names(conn, merge_ref)
        merge_dtypes = Dux.Backend.table_dtypes(conn, merge_ref) |> Map.new()
        new_labels = %Dux{source: {:table, merge_ref}, names: merge_names, dtypes: merge_dtypes}

        Process.delete(:dux_cc_refs)

        # Cleanup this iteration's labels (keep edges for next iteration)
        Enum.each(workers, fn w ->
          try do
            Worker.drop_table(w, "__cc_labels_#{stage}")
          catch
            _, _ -> :ok
          end
        end)

        if converged?(labels, new_labels, vid) do
          {:halt, new_labels}
        else
          {:cont, new_labels}
        end
      end)

    # Final cleanup: remove broadcast edges
    Enum.each(workers, fn w ->
      try do
        Worker.drop_table(w, "__cc_edges")
      catch
        _, _ -> :ok
      end
    end)

    result
  end

  defp converged?(old_labels, new_labels, vid, label_col \\ "component") do
    old_cols = Dux.to_columns(old_labels)
    new_cols = Dux.to_columns(new_labels)

    old_sorted = Enum.zip(old_cols[vid], old_cols[label_col]) |> Enum.sort()
    new_sorted = Enum.zip(new_cols[vid], new_cols[label_col]) |> Enum.sort()

    old_sorted == new_sorted
  end

  # ---------------------------------------------------------------------------
  # Community detection (label propagation)
  # ---------------------------------------------------------------------------

  @doc """
  Detect communities using label propagation.

  Each vertex starts with its own label. In each iteration, vertices adopt
  the most frequent label among their neighbors (tie-broken by smallest label).
  Converges when labels stabilize.

  Returns a `%Dux{}` with columns `[vertex_id, community]`.

  ## Options

    * `:max_iterations` — maximum iterations (default: `20`)

  ## Examples

      iex> edges = Dux.from_list([
      ...>   %{"src" => 1, "dst" => 2}, %{"src" => 2, "dst" => 1},
      ...>   %{"src" => 2, "dst" => 3}, %{"src" => 3, "dst" => 2},
      ...>   %{"src" => 4, "dst" => 5}, %{"src" => 5, "dst" => 4}
      ...> ])
      iex> vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}, %{"id" => 4}, %{"id" => 5}])
      iex> graph = Dux.Graph.new(vertices: vertices, edges: edges)
      iex> result = Dux.Graph.communities(graph) |> Dux.sort_by(:id) |> Dux.to_columns()
      iex> [c1, c2, c3, c4, c5] = result["community"]
      iex> c1 == c2 and c2 == c3
      true
      iex> c4 == c5
      true
      iex> c1 != c4
      true
  """
  @doc group: :algorithms
  def communities(%__MODULE__{} = graph, opts \\ []) do
    max_iterations = Keyword.get(opts, :max_iterations, 20)

    meta = %{
      algorithm: :communities,
      n_vertices: vertex_count(graph),
      n_edges: edge_count(graph),
      distributed: graph.workers != nil
    }

    :telemetry.span([:dux, :graph, :algorithm], meta, fn ->
      result = communities_local(graph, max_iterations)
      {result, meta}
    end)
  end

  defp communities_local(graph, max_iterations) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst
    conn = Dux.Connection.get_conn()

    labels =
      graph.vertices
      |> Dux.select([String.to_atom(vid)])
      |> Dux.mutate_with(community: ~s(#{qi(vid)}))
      |> Dux.compute()

    {final, _} =
      Enum.reduce_while(1..max_iterations, {labels, nil}, fn _i, {labels, _} ->
        Process.put(:dux_compute_ref, labels.source)
        {:table, labels_ref} = labels.source
        labels_table = labels_ref.name
        {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, conn)

        sql = """
          WITH
            edges AS (#{edges_sql}),
            bidir_edges AS (
              SELECT #{qi(src)} AS a, #{qi(dst)} AS b FROM edges
              UNION
              SELECT #{qi(dst)} AS a, #{qi(src)} AS b FROM edges
            ),
            -- Include own label + neighbor labels for frequency voting
            all_votes AS (
              SELECT be.b AS #{qi(vid)}, l.community
              FROM bidir_edges be
              JOIN "#{labels_table}" l ON be.a = l.#{qi(vid)}
              UNION ALL
              SELECT #{qi(vid)}, community FROM "#{labels_table}"
            ),
            neighbor_labels AS (
              SELECT #{qi(vid)}, community, COUNT(*) AS freq
              FROM all_votes
              GROUP BY #{qi(vid)}, community
            ),
            best AS (
              SELECT #{qi(vid)}, community FROM neighbor_labels
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY #{qi(vid)} ORDER BY freq DESC, community ASC
              ) = 1
            )
          SELECT l.#{qi(vid)}, COALESCE(b.community, l.community) AS community
          FROM "#{labels_table}" l
          LEFT JOIN best b ON l.#{qi(vid)} = b.#{qi(vid)}
        """

        new_labels = Dux.from_query(sql) |> Dux.compute()
        Process.delete(:dux_compute_ref)

        if converged?(labels, new_labels, vid, "community") do
          {:halt, {new_labels, nil}}
        else
          {:cont, {new_labels, nil}}
        end
      end)

    final
  end

  # ---------------------------------------------------------------------------
  # Triangle counting
  # ---------------------------------------------------------------------------

  @doc """
  Count the number of triangles in the graph.

  A triangle is a set of three vertices where each pair is connected by an edge.
  Edges must be bidirectional for triangle detection (i.e., if vertex A connects
  to B, there must also be an edge from B to A).
  Returns an integer count.

  When the graph has workers set via `distribute/2`, the computation runs on a
  remote worker node. The full edge set is broadcast to one worker and the
  triple self-join executes there, keeping the coordinator free.

  ## Examples

      iex> edges = Dux.from_list([
      ...>   %{"src" => 1, "dst" => 2}, %{"src" => 2, "dst" => 1},
      ...>   %{"src" => 2, "dst" => 3}, %{"src" => 3, "dst" => 2},
      ...>   %{"src" => 1, "dst" => 3}, %{"src" => 3, "dst" => 1}
      ...> ])
      iex> vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])
      iex> graph = Dux.Graph.new(vertices: vertices, edges: edges)
      iex> Dux.Graph.triangle_count(graph)
      1
  """
  def triangle_count(%__MODULE__{} = graph) do
    workers = graph.workers

    meta = %{
      algorithm: :triangle_count,
      n_vertices: vertex_count(graph),
      n_edges: edge_count(graph),
      distributed: workers != nil
    }

    :telemetry.span([:dux, :graph, :algorithm], meta, fn ->
      result =
        if workers do
          triangle_count_distributed(graph, workers)
        else
          triangle_count_local(graph)
        end

      {result, meta}
    end)
  end

  defp triangle_count_local(graph) do
    src = graph.edge_src
    dst = graph.edge_dst

    conn = Dux.Connection.get_conn()
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, conn)

    sql = """
    WITH edges_cte AS (#{edges_sql})
    SELECT COUNT(*) AS cnt FROM (
      SELECT DISTINCT
        LEAST(e1.#{qi(src)}, e1.#{qi(dst)}, e2.#{qi(dst)}) AS a,
        LEAST(
          GREATEST(e1.#{qi(src)}, e1.#{qi(dst)}),
          GREATEST(e1.#{qi(src)}, e2.#{qi(dst)}),
          GREATEST(e1.#{qi(dst)}, e2.#{qi(dst)})
        ) AS b,
        GREATEST(e1.#{qi(src)}, e1.#{qi(dst)}, e2.#{qi(dst)}) AS c
      FROM edges_cte e1
      JOIN edges_cte e2 ON e1.#{qi(dst)} = e2.#{qi(src)}
      JOIN edges_cte e3 ON e2.#{qi(dst)} = e3.#{qi(src)} AND e3.#{qi(dst)} = e1.#{qi(src)}
      WHERE e1.#{qi(src)} < e1.#{qi(dst)}
        AND e1.#{qi(dst)} < e2.#{qi(dst)}
    ) triangles
    """

    result = Dux.from_query(sql) |> Dux.to_rows()
    hd(result)["cnt"]
  end

  # Distributed: broadcast all edges to one worker, count there.
  # Triangles can span partitions, so the full edge set must be visible.
  defp triangle_count_distributed(graph, workers) do
    src = graph.edge_src
    dst = graph.edge_dst
    worker = hd(workers)

    edges_computed = Dux.compute(graph.edges)
    {:table, edges_ref} = edges_computed.source
    conn = Dux.Connection.get_conn()
    edges_ipc = Dux.Backend.table_to_ipc(conn, edges_ref)

    Worker.register_table(worker, "__tri_edges", edges_ipc)

    sql = """
    SELECT COUNT(*) AS cnt FROM (
      SELECT DISTINCT
        LEAST(e1.#{qi(src)}, e1.#{qi(dst)}, e2.#{qi(dst)}) AS a,
        LEAST(
          GREATEST(e1.#{qi(src)}, e1.#{qi(dst)}),
          GREATEST(e1.#{qi(src)}, e2.#{qi(dst)}),
          GREATEST(e1.#{qi(dst)}, e2.#{qi(dst)})
        ) AS b,
        GREATEST(e1.#{qi(src)}, e1.#{qi(dst)}, e2.#{qi(dst)}) AS c
      FROM "__tri_edges" e1
      JOIN "__tri_edges" e2 ON e1.#{qi(dst)} = e2.#{qi(src)}
      JOIN "__tri_edges" e3 ON e2.#{qi(dst)} = e3.#{qi(src)} AND e3.#{qi(dst)} = e1.#{qi(src)}
      WHERE e1.#{qi(src)} < e1.#{qi(dst)}
        AND e1.#{qi(dst)} < e2.#{qi(dst)}
    ) triangles
    """

    {:ok, result_ipc} = Worker.execute(worker, Dux.from_query(sql))
    result = Dux.Backend.table_from_ipc(conn, result_ipc)
    cols = Dux.Backend.table_to_columns(conn, result)

    try do
      Worker.drop_table(worker, "__tri_edges")
    catch
      _, _ -> :ok
    end

    hd(cols["cnt"])
  end

  # ---------------------------------------------------------------------------
  # Counts
  # ---------------------------------------------------------------------------

  @doc """
  Return the number of vertices in the graph. Triggers computation.

  ## Examples

      iex> graph = Dux.Graph.new(vertices: Dux.from_list([%{"id" => 1}, %{"id" => 2}]), edges: Dux.from_list([%{"src" => 1, "dst" => 2}]))
      iex> Dux.Graph.vertex_count(graph)
      2
  """
  def vertex_count(%__MODULE__{} = graph) do
    Dux.n_rows(graph.vertices)
  end

  @doc """
  Return the number of edges in the graph. Triggers computation.

  ## Examples

      iex> graph = Dux.Graph.new(vertices: Dux.from_list([%{"id" => 1}, %{"id" => 2}]), edges: Dux.from_list([%{"src" => 1, "dst" => 2}]))
      iex> Dux.Graph.edge_count(graph)
      1
  """
  def edge_count(%__MODULE__{} = graph) do
    Dux.n_rows(graph.edges)
  end

  defp broadcast_to_workers(workers, tables) do
    tasks = Enum.map(workers, &Task.async(fn -> register_tables(&1, tables) end))
    Task.await_many(tasks, 30_000)
  end

  defp fan_out_pipeline(workers, pipeline) do
    workers
    |> Enum.map(&Task.async(fn -> Worker.execute(&1, pipeline) end))
    |> Task.await_many(30_000)
  end

  defp register_tables(worker, tables) do
    Enum.each(tables, fn {name, ipc} ->
      case Worker.register_table(worker, name, ipc) do
        {:ok, _} -> :ok
        {:error, reason} -> raise "Failed to register table #{name}: #{reason}"
      end
    end)
  end
end
