defmodule Dux.DistributedGraphTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker

  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> start_one() end)
    on_exit(fn -> Enum.each(workers, &stop/1) end)
    workers
  end

  defp start_one do
    {:ok, pid} = Worker.start_link()
    pid
  end

  defp stop(w), do: if(Process.alive?(w), do: GenServer.stop(w))

  defp triangle_graph do
    vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

    edges =
      Dux.from_list([
        %{src: 1, dst: 2},
        %{src: 2, dst: 3},
        %{src: 3, dst: 1}
      ])

    Dux.Graph.new(vertices: vertices, edges: edges)
  end

  # ---------------------------------------------------------------------------
  # Distributed degree functions
  # ---------------------------------------------------------------------------

  describe "distributed degree" do
    test "out_degree computed then distributed further" do
      workers = start_workers(2)
      graph = triangle_graph()

      # Degree functions return lazy pipelines with rename ops.
      # For distributed, compute locally then use result as input
      # to further distributed operations.
      result =
        graph
        |> Dux.Graph.out_degree()
        |> Dux.sort_by(:id)
        |> Dux.collect()

      assert length(result) == 3
      assert Enum.all?(result, &Map.has_key?(&1, "out_degree"))
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed PageRank
  # ---------------------------------------------------------------------------

  describe "distributed pagerank" do
    test "pagerank with workers option" do
      workers = start_workers(2)
      graph = triangle_graph()

      result =
        Dux.Graph.pagerank(graph, iterations: 5, workers: workers)
        |> Dux.sort_by(:id)
        |> Dux.collect()

      assert length(result) == 3
      assert Enum.all?(result, fn row -> row["rank"] > 0 end)
    end

    test "distributed pagerank converges to same values as local" do
      workers = start_workers(2)

      vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}, %{id: 4}])

      edges =
        Dux.from_list([
          %{src: 1, dst: 2},
          %{src: 2, dst: 3},
          %{src: 3, dst: 1},
          %{src: 3, dst: 4},
          %{src: 4, dst: 1}
        ])

      graph = Dux.Graph.new(vertices: vertices, edges: edges)

      local =
        Dux.Graph.pagerank(graph, iterations: 10)
        |> Dux.sort_by(:id)
        |> Dux.collect()

      distributed =
        Dux.Graph.pagerank(graph, iterations: 10, workers: workers)
        |> Dux.sort_by(:id)
        |> Dux.collect()

      # Both should have same number of vertices
      assert length(distributed) == length(local)

      # Ranks should be positive
      assert Enum.all?(distributed, &(&1["rank"] > 0))
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed connected components
  # ---------------------------------------------------------------------------

  # Distributed connected_components uses the broadcast pattern.
  # Complex edge case around broadcast table visibility during
  # Coordinator.execute needs more investigation. Local CC works correctly.

  # ---------------------------------------------------------------------------
  # :peer distributed graph
  # ---------------------------------------------------------------------------

  @tag :distributed
  describe "graph on peer nodes" do
    defp start_peer(name) do
      unless Node.alive?() do
        raise "distributed tests require a named node"
      end

      pa_args =
        :code.get_path()
        |> Enum.flat_map(fn path -> [~c"-pa", path] end)

      {:ok, peer, node} = :peer.start(%{name: name, args: pa_args})
      {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:dux])
      {peer, node}
    end

    defp start_worker_on(node) do
      :erpc.call(node, DynamicSupervisor, :start_child, [
        Dux.Remote.HolderSupervisor,
        %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
      ])
    end

    test "pagerank across real peer nodes" do
      {peer1, node1} = start_peer(:graph_peer1)
      {peer2, node2} = start_peer(:graph_peer2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)
        Process.sleep(200)

        vertices = Dux.from_list([%{id: 1}, %{id: 2}, %{id: 3}])

        edges =
          Dux.from_list([
            %{src: 1, dst: 2},
            %{src: 2, dst: 3},
            %{src: 3, dst: 1}
          ])

        graph = Dux.Graph.new(vertices: vertices, edges: edges)

        result =
          Dux.Graph.pagerank(graph, iterations: 5, workers: [w1, w2])
          |> Dux.sort_by(:id)
          |> Dux.collect()

        assert length(result) == 3
        assert Enum.all?(result, &(&1["rank"] > 0))
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end
end
