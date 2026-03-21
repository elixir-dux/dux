defmodule Dux.DistributeApiTest do
  use ExUnit.Case, async: true
  require Dux

  # ---------------------------------------------------------------------------
  # Dux.distribute/2 propagation tests (no workers needed — just struct checks)
  # ---------------------------------------------------------------------------

  describe "Dux.distribute/2" do
    test "sets workers on the struct" do
      df = Dux.from_list([%{x: 1}]) |> Dux.distribute([:fake_pid1, :fake_pid2])
      assert df.workers == [:fake_pid1, :fake_pid2]
    end

    test "workers propagate through filter" do
      df =
        Dux.from_list([%{x: 1}])
        |> Dux.distribute([:w1, :w2])
        |> Dux.filter(x > 0)

      assert df.workers == [:w1, :w2]
    end

    test "workers propagate through mutate" do
      df =
        Dux.from_list([%{x: 1}])
        |> Dux.distribute([:w1])
        |> Dux.mutate(y: x * 2)

      assert df.workers == [:w1]
    end

    test "workers propagate through group_by + summarise" do
      df =
        Dux.from_list([%{x: 1, g: "a"}])
        |> Dux.distribute([:w1])
        |> Dux.group_by(:g)
        |> Dux.summarise(total: sum(x))

      assert df.workers == [:w1]
    end

    test "workers propagate through select" do
      df =
        Dux.from_list([%{x: 1, y: 2}])
        |> Dux.distribute([:w1])
        |> Dux.select([:x])

      assert df.workers == [:w1]
    end

    test "workers propagate through sort_by" do
      df =
        Dux.from_list([%{x: 1}])
        |> Dux.distribute([:w1])
        |> Dux.sort_by(:x)

      assert df.workers == [:w1]
    end

    test "workers propagate through join" do
      right = Dux.from_list([%{x: 1, z: "a"}])

      df =
        Dux.from_list([%{x: 1, y: 2}])
        |> Dux.distribute([:w1])
        |> Dux.join(right, on: :x)

      assert df.workers == [:w1]
    end

    test "workers propagate through head" do
      df =
        Dux.from_list([%{x: 1}])
        |> Dux.distribute([:w1])
        |> Dux.head(5)

      assert df.workers == [:w1]
    end

    test "workers propagate through rename" do
      df =
        Dux.from_list([%{x: 1}])
        |> Dux.distribute([:w1])
        |> Dux.rename(x: :y)

      assert df.workers == [:w1]
    end

    test "workers propagate through a full pipeline" do
      df =
        Dux.from_list([%{x: 1, g: "a"}])
        |> Dux.distribute([:w1, :w2])
        |> Dux.filter(x > 0)
        |> Dux.mutate(y: x * 2)
        |> Dux.group_by(:g)
        |> Dux.summarise(total: sum(y))
        |> Dux.sort_by(:g)

      assert df.workers == [:w1, :w2]
    end
  end

  # ---------------------------------------------------------------------------
  # Dux.local/1
  # ---------------------------------------------------------------------------

  describe "Dux.local/1" do
    test "removes workers from the struct" do
      df =
        Dux.from_list([%{x: 1}])
        |> Dux.distribute([:w1])
        |> Dux.local()

      assert df.workers == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Dux.collect/1 — new semantics (local Dux, strips workers)
  # ---------------------------------------------------------------------------

  describe "Dux.collect/1" do
    test "on local pipeline returns computed %Dux{}" do
      df = Dux.from_list([%{x: 1}]) |> Dux.collect()
      assert %Dux{} = df
      assert df.workers == nil
      assert match?({:table, _}, df.source)
      assert df.ops == []
    end

    test "strips workers from result" do
      # Can't actually distribute without real workers, but we can verify
      # that collect on a local pipeline returns workers == nil
      df = Dux.from_list([%{x: 1}]) |> Dux.collect()
      assert df.workers == nil
    end
  end

  # ---------------------------------------------------------------------------
  # to_rows and to_columns work on local data
  # ---------------------------------------------------------------------------

  describe "to_rows/1" do
    test "returns list of maps" do
      result = Dux.from_list([%{x: 1}, %{x: 2}]) |> Dux.to_rows()
      assert result == [%{"x" => 1}, %{"x" => 2}]
    end

    test "atom_keys option" do
      result = Dux.from_list([%{x: 1}]) |> Dux.to_rows(atom_keys: true)
      assert result == [%{x: 1}]
    end
  end

  # ---------------------------------------------------------------------------
  # Graph distribute
  # ---------------------------------------------------------------------------

  describe "Dux.Graph.distribute/2" do
    test "sets workers on graph struct" do
      v = Dux.from_list([%{id: 1}])
      e = Dux.from_list([%{src: 1, dst: 1}])
      graph = Dux.Graph.new(vertices: v, edges: e) |> Dux.Graph.distribute([:w1])
      assert graph.workers == [:w1]
    end
  end
end
