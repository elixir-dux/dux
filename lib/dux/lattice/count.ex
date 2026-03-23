defmodule Dux.Lattice.Count do
  @moduledoc false
  @behaviour Dux.Lattice

  # COUNT is additive across partitions: the total count is the sum of
  # per-partition counts.

  @impl true
  def bottom, do: 0

  @impl true
  def merge(a, b), do: a + b

  @impl true
  def finalize(acc), do: acc
end
