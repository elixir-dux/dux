defmodule Dux.BroadcastTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.{Broadcast, Worker}

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> start_one_worker() end)
    on_exit(fn -> Enum.each(workers, &stop_worker/1) end)
    workers
  end

  defp start_one_worker do
    {:ok, pid} = Worker.start_link()
    pid
  end

  defp stop_worker(w) do
    if Process.alive?(w), do: GenServer.stop(w)
  end

  # ---------------------------------------------------------------------------
  # Happy path: local workers
  # ---------------------------------------------------------------------------

  describe "broadcast join with local workers" do
    test "basic star-schema join" do
      workers = start_workers(2)

      fact =
        Dux.from_list([
          %{"region_id" => 1, "amount" => 100},
          %{"region_id" => 2, "amount" => 200},
          %{"region_id" => 1, "amount" => 150}
        ])

      dim =
        Dux.from_list([
          %{"region_id" => 1, "name" => "US"},
          %{"region_id" => 2, "name" => "EU"}
        ])

      result =
        Broadcast.execute(fact, dim, on: :region_id, workers: workers)
        |> Dux.sort_by(:region_id)
        |> Dux.to_rows()

      # 2 workers × 3 fact rows = 6 joined rows
      assert length(result) == 6
      assert Enum.all?(result, &Map.has_key?(&1, "name"))
      # Verify sorted
      ids = Enum.map(result, & &1["region_id"])
      assert ids == Enum.sort(ids)
    end

    test "broadcast join with aggregation" do
      workers = start_workers(2)

      orders =
        Dux.from_list([
          %{"product_id" => 1, "qty" => 5},
          %{"product_id" => 2, "qty" => 3},
          %{"product_id" => 1, "qty" => 2}
        ])

      products =
        Dux.from_list([
          %{"product_id" => 1, "name" => "Widget"},
          %{"product_id" => 2, "name" => "Gadget"}
        ])

      # Collect immediately — chaining more verbs after distributed execute
      # can hit GC race on the merged result's temp table
      result = Broadcast.execute(orders, products, on: :product_id, workers: workers)
      rows = Dux.to_rows(result)

      # 2 workers replicate the data, so rows are doubled
      assert length(rows) == 6
      assert Enum.all?(rows, &Map.has_key?(&1, "name"))

      gadget_qty =
        rows |> Enum.filter(&(&1["name"] == "Gadget")) |> Enum.map(& &1["qty"]) |> Enum.sum()

      widget_qty =
        rows |> Enum.filter(&(&1["name"] == "Widget")) |> Enum.map(& &1["qty"]) |> Enum.sum()

      assert gadget_qty == 3 * 2
      assert widget_qty == 7 * 2
    end

    test "left broadcast join preserves unmatched left rows" do
      workers = start_workers(1)

      fact =
        Dux.from_list([
          %{"id" => 1, "val" => 10},
          %{"id" => 2, "val" => 20},
          %{"id" => 3, "val" => 30}
        ])

      dim =
        Dux.from_list([
          %{"id" => 1, "label" => "one"}
        ])

      joined = Broadcast.execute(fact, dim, on: :id, how: :left, workers: workers)
      result = Dux.to_rows(joined)

      assert length(result) == 3
      matched = Enum.filter(result, &(&1["label"] != nil))
      assert length(matched) == 1
      assert hd(matched)["label"] == "one"
    end
  end

  # ---------------------------------------------------------------------------
  # should_broadcast?
  # ---------------------------------------------------------------------------

  describe "should_broadcast?/2" do
    test "small table is broadcastable" do
      small = Dux.from_list([%{"x" => 1}, %{"x" => 2}])
      assert Broadcast.should_broadcast?(small)
    end

    test "respects custom threshold" do
      small = Dux.from_list([%{"x" => 1}])
      # With a 1 byte threshold, nothing is small enough
      refute Broadcast.should_broadcast?(small, 1)
    end
  end

  # ---------------------------------------------------------------------------
  # Cleanup
  # ---------------------------------------------------------------------------

  describe "cleanup" do
    test "broadcast tables are cleaned up after join" do
      workers = start_workers(1)
      worker = hd(workers)

      fact = Dux.from_list([%{"id" => 1, "val" => 10}])
      dim = Dux.from_list([%{"id" => 1, "name" => "test"}])

      Broadcast.execute(fact, dim,
        on: :id,
        workers: workers,
        broadcast_name: "__test_cleanup"
      )

      # The broadcast table should be cleaned up
      pipeline = Dux.from_query(~s(SELECT * FROM "__test_cleanup"))
      assert {:error, _} = Worker.execute(worker, pipeline)
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "no workers raises" do
      assert_raise ArgumentError, ~r/no workers/, fn ->
        Broadcast.execute(
          Dux.from_query("SELECT 1"),
          Dux.from_query("SELECT 1"),
          on: :x,
          workers: []
        )
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "large dimension table broadcast" do
      workers = start_workers(2)

      fact = Dux.from_query("SELECT x AS id, x * 10 AS amount FROM range(100) t(x)")

      dim =
        Dux.from_query(
          "SELECT x AS id, 'item_' || CAST(x AS VARCHAR) AS name FROM range(100) t(x)"
        )

      result =
        Broadcast.execute(fact, dim, on: :id, workers: workers)
        |> Dux.n_rows()

      # 2 workers × 100 matched rows = 200
      assert result == 200
    end

    test "broadcast join with no matching rows" do
      workers = start_workers(1)

      fact = Dux.from_list([%{"id" => 1, "val" => 10}])
      dim = Dux.from_list([%{"id" => 999, "name" => "nothing"}])

      result =
        Broadcast.execute(fact, dim, on: :id, workers: workers)
        |> Dux.n_rows()

      assert result == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Distributed integration (peer nodes)
  # ---------------------------------------------------------------------------

  describe "broadcast join with peer workers" do
    @describetag :distributed
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
        Dux.DynamicSupervisor,
        %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
      ])
    end

    test "broadcast join across real peer nodes" do
      {peer1, node1} = start_peer(:bcast_peer1)
      {peer2, node2} = start_peer(:bcast_peer2)

      try do
        {:ok, w1} = start_worker_on(node1)
        {:ok, w2} = start_worker_on(node2)

        Process.sleep(200)

        fact =
          Dux.from_query("SELECT x AS product_id, x * 100 AS revenue FROM range(1, 51) t(x)")

        dim =
          Dux.from_list([
            %{"product_id" => 1, "category" => "Electronics"},
            %{"product_id" => 2, "category" => "Books"},
            %{"product_id" => 3, "category" => "Electronics"}
          ])

        result =
          Broadcast.execute(fact, dim, on: :product_id, workers: [w1, w2])
          |> Dux.group_by(:category)
          |> Dux.summarise_with(total: "SUM(revenue)")
          |> Dux.sort_by(:category)
          |> Dux.to_rows()

        # Only products 1, 2, 3 match the dim table
        # 2 workers replicate, so totals are 2x
        assert length(result) == 2

        books = Enum.find(result, &(&1["category"] == "Books"))
        electronics = Enum.find(result, &(&1["category"] == "Electronics"))

        assert books["total"] == 200 * 2
        assert electronics["total"] == (100 + 300) * 2
      after
        :peer.stop(peer1)
        :peer.stop(peer2)
      end
    end
  end
end
