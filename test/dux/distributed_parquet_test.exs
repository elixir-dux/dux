defmodule Dux.DistributedParquetTest do
  use ExUnit.Case, async: false
  require Dux

  alias Dux.Remote.Worker

  @tmp_dir System.tmp_dir!()

  defp start_workers(n) do
    workers = Enum.map(1..n, fn _ -> start_one() end)
    on_exit(fn -> Enum.each(workers, &stop/1) end)
    workers
  end

  defp start_one do
    {:ok, pid} = Worker.start_link()
    pid
  end

  defp stop(w) do
    GenServer.stop(w)
  catch
    :exit, _ -> :ok
  end

  defp tmp_path(name) do
    Path.join(@tmp_dir, "dux_parq_test_#{System.unique_integer([:positive])}_#{name}")
  end

  # ---------------------------------------------------------------------------
  # from_parquet + distribute: local workers
  # ---------------------------------------------------------------------------

  describe "from_parquet + distribute (local workers)" do
    test "partitioned parquet reads — no duplication" do
      dir = tmp_path("no_dup")
      File.mkdir_p!(dir)

      try do
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{part: i, value: (i - 1) * 25 + j}
          Dux.from_list(rows) |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        workers = start_workers(2)

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute(workers)
          |> Dux.to_columns()

        # Each worker reads 2 files → 50 rows each → 100 total, no duplication
        all_values = Enum.sort(result["value"])
        assert all_values == Enum.to_list(1..100)
      after
        File.rm_rf!(dir)
      end
    end

    test "single parquet file replicates across workers" do
      dir = tmp_path("single")
      File.mkdir_p!(dir)

      try do
        Dux.from_list(Enum.map(1..10, &%{x: &1}))
        |> Dux.to_parquet(Path.join(dir, "data.parquet"))

        workers = start_workers(2)

        result =
          Dux.from_parquet(Path.join(dir, "data.parquet"))
          |> Dux.distribute(workers)
          |> Dux.summarise_with(total: "SUM(x)", n: "COUNT(*)")
          |> Dux.to_rows()

        row = hd(result)
        # Single file replicates: each worker sums 1..10=55, merger re-aggregates
        assert row["total"] == 55 * 2
        assert row["n"] == 10 * 2
      after
        File.rm_rf!(dir)
      end
    end

    test "partitioned parquet + filter + group_by + summarise" do
      dir = tmp_path("pipeline")
      File.mkdir_p!(dir)

      try do
        for i <- 1..6 do
          rows =
            for j <- 1..20 do
              %{
                region: Enum.at(["US", "EU", "APAC"], rem(j, 3)),
                amount: j * 10 + i
              }
            end

          Dux.from_list(rows) |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        workers = start_workers(3)

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute(workers)
          |> Dux.filter_with("amount > 50")
          |> Dux.group_by(:region)
          |> Dux.summarise_with(total: "SUM(amount)", n: "COUNT(*)")
          |> Dux.sort_by(:region)
          |> Dux.to_rows()

        assert length(result) == 3
        assert Enum.all?(result, &(&1["total"] > 0))
        assert Enum.all?(result, &(&1["n"] > 0))
      after
        File.rm_rf!(dir)
      end
    end

    test "partitioned parquet + join with local dim table" do
      dir = tmp_path("join")
      File.mkdir_p!(dir)

      try do
        for i <- 1..4 do
          rows =
            for j <- 1..10 do
              %{carrier: Enum.at(["AA", "UA", "DL"], rem(j, 3)), flights: j + i}
            end

          Dux.from_list(rows) |> Dux.to_parquet(Path.join(dir, "flights_#{i}.parquet"))
        end

        workers = start_workers(2)

        dim =
          Dux.from_list([
            %{carrier: "AA", name: "American Airlines"},
            %{carrier: "UA", name: "United Airlines"},
            %{carrier: "DL", name: "Delta Air Lines"}
          ])
          |> Dux.compute()

        result =
          Dux.from_parquet(Path.join(dir, "*.parquet"))
          |> Dux.distribute(workers)
          |> Dux.join(dim, on: :carrier)
          |> Dux.group_by(:name)
          |> Dux.summarise_with(total_flights: "SUM(flights)")
          |> Dux.sort_by(:name)
          |> Dux.to_rows()

        assert length(result) == 3
        names = Enum.map(result, & &1["name"])
        assert "American Airlines" in names
        assert "United Airlines" in names
        assert "Delta Air Lines" in names
      after
        File.rm_rf!(dir)
      end
    end

    test "distributed matches local for partitioned parquet" do
      dir = tmp_path("match")
      File.mkdir_p!(dir)

      try do
        for i <- 1..4 do
          rows = for j <- 1..25, do: %{grp: rem(j, 5), val: (i - 1) * 25 + j}
          Dux.from_list(rows) |> Dux.to_parquet(Path.join(dir, "part_#{i}.parquet"))
        end

        workers = start_workers(2)
        glob = Path.join(dir, "*.parquet")

        local =
          Dux.from_parquet(glob)
          |> Dux.group_by(:grp)
          |> Dux.summarise_with(total: "SUM(val)", n: "COUNT(*)")
          |> Dux.sort_by(:grp)
          |> Dux.to_rows()

        distributed =
          Dux.from_parquet(glob)
          |> Dux.distribute(workers)
          |> Dux.group_by(:grp)
          |> Dux.summarise_with(total: "SUM(val)", n: "COUNT(*)")
          |> Dux.sort_by(:grp)
          |> Dux.to_rows()

        # Partitioned parquet distributes files, doesn't replicate
        # So results should match exactly
        assert local == distributed
      after
        File.rm_rf!(dir)
      end
    end
  end
end
