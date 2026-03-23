defmodule Dux.Lattice.Avg do
  @moduledoc false
  @behaviour Dux.Lattice

  # AVG is decomposed as {sum, count}. Workers emit SUM and COUNT;
  # finalize divides to get the mean.

  @impl true
  def bottom, do: {0, 0}

  @impl true
  def merge({s1, c1}, {s2, c2}), do: {s1 + s2, c1 + c2}

  @impl true
  def finalize({_sum, 0}), do: nil
  def finalize({sum, count}), do: sum / count
end
