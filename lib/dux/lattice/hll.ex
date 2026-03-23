defmodule Dux.Lattice.Hll do
  @moduledoc false
  @behaviour Dux.Lattice

  # HyperLogLog for approximate COUNT(DISTINCT).
  # DuckDB's approx_count_distinct() returns a scalar estimate, not a
  # mergeable sketch. So the distributed path sums per-worker estimates
  # (an approximation of the approximation). The lattice here models
  # that summation — same as Count.
  #
  # When DuckDB exposes raw HLL sketches (via datasketches extension),
  # this lattice can be upgraded to perform proper sketch union.

  @impl true
  def bottom, do: 0

  @impl true
  def merge(a, b), do: a + b

  @impl true
  def finalize(acc), do: acc
end
