defmodule Dux.Graph do
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
      |> Dux.collect()
  """

  defstruct [:vertices, :edges, :vertex_id, :edge_src, :edge_dst]

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
  Create a graph from an edge list. Vertices are inferred from unique
  source and destination nodes.
  """
  def from_edgelist(%Dux{} = edges, opts \\ []) do
    src_col = to_string(Keyword.get(opts, :edge_src, :src))
    dst_col = to_string(Keyword.get(opts, :edge_dst, :dst))
    id_col = to_string(Keyword.get(opts, :vertex_id, :id))

    db = Dux.Connection.get_db()
    {edges_sql, _} = Dux.QueryBuilder.build(edges, db)

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
  Compute the total degree (in + out) of each vertex.

  Returns a `%Dux{}` with columns `[vertex_id, degree]`.
  """
  def degree(%__MODULE__{} = graph) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    db = Dux.Connection.get_db()
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, db)

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

  ## Examples

      iex> edges = Dux.from_list([
      ...>   %{"src" => 1, "dst" => 2},
      ...>   %{"src" => 2, "dst" => 3},
      ...>   %{"src" => 3, "dst" => 1},
      ...>   %{"src" => 3, "dst" => 2}
      ...> ])
      iex> vertices = Dux.from_list([%{"id" => 1}, %{"id" => 2}, %{"id" => 3}])
      iex> graph = Dux.Graph.new(vertices: vertices, edges: edges)
      iex> result = Dux.Graph.pagerank(graph) |> Dux.sort_by(:id) |> Dux.collect()
      iex> length(result) == 3
      true
      iex> Enum.all?(result, fn row -> row["rank"] > 0 end)
      true
  """
  def pagerank(%__MODULE__{} = graph, opts \\ []) do
    damping = Keyword.get(opts, :damping, 0.85)
    iterations = Keyword.get(opts, :iterations, 20)
    workers = Keyword.get(opts, :workers)

    if workers do
      pagerank_distributed(graph, damping, iterations, workers)
    else
      pagerank_local(graph, damping, iterations)
    end
  end

  defp pagerank_local(graph, damping, iterations) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    n =
      graph.vertices
      |> Dux.summarise_with(n: "COUNT(*)")
      |> Dux.collect()
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

    {final, kept_out_deg, kept_prev} =
      Enum.reduce(1..iterations, {ranks, out_deg, [ranks]}, fn _i, {ranks, out_deg, prev} ->
        db = Dux.Connection.get_db()
        Process.put(:dux_compute_ref, {ranks.source, out_deg.source})

        {:table, ranks_ref} = ranks.source
        ranks_table = Dux.Native.table_ensure(db, ranks_ref)

        {:table, outdeg_ref} = out_deg.source
        outdeg_table = Dux.Native.table_ensure(db, outdeg_ref)

        {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, db)
        {verts_sql, _} = Dux.QueryBuilder.build(graph.vertices, db)

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
        {new_ranks, out_deg, [new_ranks | prev]}
      end)

    :erlang.garbage_collect()
    _ = {kept_out_deg, kept_prev}
    final
  end

  defp pagerank_distributed(graph, damping, iterations, workers) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst

    n =
      graph.vertices
      |> Dux.summarise_with(n: "COUNT(*)")
      |> Dux.collect()
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

    # Compute out-degree
    out_deg = out_degree(graph) |> Dux.compute()

    # Each iteration: broadcast ranks + out_deg to workers,
    # workers compute contributions from their edges,
    # coordinator merges and updates ranks
    Enum.reduce(1..iterations, ranks, fn _i, ranks ->
      # Broadcast current ranks and out_degree to all workers
      {:table, ranks_ref} = ranks.source
      ranks_ipc = Dux.Native.table_to_ipc(ranks_ref)

      {:table, outdeg_ref} = out_deg.source
      outdeg_ipc = Dux.Native.table_to_ipc(outdeg_ref)

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
  end

  # ---------------------------------------------------------------------------
  # Shortest paths (via recursive CTE)
  # ---------------------------------------------------------------------------

  @doc """
  Find the shortest path distance from a source vertex to all reachable vertices.

  Uses DuckDB's recursive CTEs for efficient graph traversal.
  Returns a `%Dux{}` with columns `[node, dist]`.

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
    workers = Keyword.get(opts, :workers)
    max_depth = Keyword.get(opts, :max_depth, 1000)

    if workers do
      shortest_paths_distributed(graph, from_vertex, max_depth, workers)
    else
      shortest_paths_local(graph, from_vertex, max_depth)
    end
  end

  defp shortest_paths_local(graph, from_vertex, max_depth) do
    src = graph.edge_src
    dst = graph.edge_dst

    db = Dux.Connection.get_db()
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, db)

    sql = """
    WITH RECURSIVE
      edges_cte AS (#{edges_sql}),
      paths AS (
        SELECT #{from_vertex} AS node, 0 AS dist
        UNION
        SELECT e.#{qi(dst)} AS node, p.dist + 1
        FROM paths p
        JOIN edges_cte e ON p.node = e.#{qi(src)}
        WHERE p.dist < #{max_depth}
      )
    SELECT node, MIN(dist) AS dist FROM paths GROUP BY node
    """

    Dux.from_query(sql)
  end

  # Distributed: broadcast edges to one worker, run recursive CTE there.
  # BFS needs the full edge set visible, so we offload to a worker's DuckDB.
  defp shortest_paths_distributed(graph, from_vertex, max_depth, workers) do
    src = graph.edge_src
    dst = graph.edge_dst
    worker = hd(workers)

    edges_computed = Dux.compute(graph.edges)
    {:table, edges_ref} = edges_computed.source
    edges_ipc = Dux.Native.table_to_ipc(edges_ref)

    Worker.register_table(worker, "__bfs_edges", edges_ipc)

    sql = """
    WITH RECURSIVE paths AS (
        SELECT #{from_vertex} AS node, 0 AS dist
        UNION
        SELECT e.#{qi(dst)} AS node, p.dist + 1
        FROM paths p
        JOIN "__bfs_edges" e ON p.node = e.#{qi(src)}
        WHERE p.dist < #{max_depth}
      )
    SELECT node, MIN(dist) AS dist FROM paths GROUP BY node
    """

    {:ok, result_ipc} = Worker.execute(worker, Dux.from_query(sql))
    result = Dux.Native.table_from_ipc(result_ipc)
    names = Dux.Native.table_names(result)
    dtypes = result |> Dux.Native.table_dtypes() |> Map.new()

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
    workers = Keyword.get(opts, :workers)

    if workers do
      cc_distributed(graph, max_iterations, workers)
    else
      cc_local(graph, max_iterations)
    end
  end

  defp cc_local(graph, max_iterations) do
    vid = graph.vertex_id
    src = graph.edge_src
    dst = graph.edge_dst
    db = Dux.Connection.get_db()

    labels =
      graph.vertices
      |> Dux.select([String.to_atom(vid)])
      |> Dux.mutate_with(component: ~s(#{qi(vid)}))
      |> Dux.compute()

    {final, _history} =
      Enum.reduce_while(1..max_iterations, {labels, [labels]}, fn _i, {labels, history} ->
        Process.put(:dux_compute_ref, labels.source)
        {:table, labels_ref} = labels.source
        labels_table = Dux.Native.table_ensure(db, labels_ref)
        {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, db)

        sql = """
          WITH
            edges AS (#{edges_sql}),
            bidir_edges AS (
              SELECT #{qi(src)} AS a, #{qi(dst)} AS b FROM edges
              UNION
              SELECT #{qi(dst)} AS a, #{qi(src)} AS b FROM edges
            ),
            neighbor_labels AS (
              SELECT be.b AS #{qi(vid)}, l.component
              FROM bidir_edges be
              JOIN "#{labels_table}" l ON be.a = l.#{qi(vid)}
            ),
            all_labels AS (
              SELECT #{qi(vid)}, component FROM "#{labels_table}"
              UNION ALL
              SELECT #{qi(vid)}, component FROM neighbor_labels
            )
          SELECT #{qi(vid)}, MIN(component) AS component
          FROM all_labels
          GROUP BY #{qi(vid)}
        """

        new_labels = Dux.from_query(sql) |> Dux.compute()
        Process.delete(:dux_compute_ref)

        if converged?(labels, new_labels, vid) do
          {:halt, {new_labels, [new_labels | history]}}
        else
          {:cont, {new_labels, [new_labels | history]}}
        end
      end)

    final
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
    edges_ipc = Dux.Native.table_to_ipc(edges_ref)
    broadcast_to_workers(workers, [{"__cc_edges", edges_ipc}])

    result =
      Enum.reduce_while(1..max_iterations, labels, fn _i, labels ->
        {:table, labels_ref} = labels.source
        labels_ipc = Dux.Native.table_to_ipc(labels_ref)

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
        db = Dux.Connection.get_db()

        input_refs =
          Enum.map(all_ipc, fn ipc ->
            ref = Dux.Native.table_from_ipc(ipc)
            name = Dux.Native.table_ensure(db, ref)
            {name, ref}
          end)

        Process.put(:dux_cc_refs, {labels, input_refs, edges_computed})

        {:table, cur_labels_ref} = labels.source
        cur_table = Dux.Native.table_ensure(db, cur_labels_ref)

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

        new_labels =
          case Dux.Native.df_query(db, merge_sql) do
            {:error, reason} ->
              raise "CC merge failed: #{reason}"

            ref ->
              names = Dux.Native.table_names(ref)
              dtypes = ref |> Dux.Native.table_dtypes() |> Map.new()
              %Dux{source: {:table, ref}, names: names, dtypes: dtypes}
          end

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

  defp converged?(old_labels, new_labels, vid) do
    old_cols = Dux.to_columns(old_labels)
    new_cols = Dux.to_columns(new_labels)

    old_sorted = Enum.zip(old_cols[vid], old_cols["component"]) |> Enum.sort()
    new_sorted = Enum.zip(new_cols[vid], new_cols["component"]) |> Enum.sort()

    old_sorted == new_sorted
  end

  # ---------------------------------------------------------------------------
  # Triangle counting
  # ---------------------------------------------------------------------------

  @doc """
  Count the number of triangles in the graph.

  A triangle is a set of three vertices where each pair is connected by an edge.
  Edges must be bidirectional for triangle detection.
  Returns an integer count.

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
  def triangle_count(%__MODULE__{} = graph, opts \\ []) do
    workers = Keyword.get(opts, :workers)

    if workers do
      triangle_count_distributed(graph, workers)
    else
      triangle_count_local(graph)
    end
  end

  defp triangle_count_local(graph) do
    src = graph.edge_src
    dst = graph.edge_dst

    db = Dux.Connection.get_db()
    {edges_sql, _} = Dux.QueryBuilder.build(graph.edges, db)

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

    result = Dux.from_query(sql) |> Dux.collect()
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
    edges_ipc = Dux.Native.table_to_ipc(edges_ref)

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
    result = Dux.Native.table_from_ipc(result_ipc)
    cols = Dux.Native.table_to_columns(result)

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
  Return the number of vertices.
  """
  def vertex_count(%__MODULE__{} = graph) do
    Dux.n_rows(graph.vertices)
  end

  @doc """
  Return the number of edges.
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
    Enum.each(tables, fn {name, ipc} -> Worker.register_table(worker, name, ipc) end)
  end

  # Escape double quotes in SQL identifiers to prevent injection
  defp qi(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end
end
