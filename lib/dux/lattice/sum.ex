defmodule Dux.Lattice.Sum do
  @moduledoc false
  @behaviour Dux.Lattice

  @impl true
  def bottom, do: 0

  @impl true
  def merge(a, b), do: a + b

  @impl true
  def finalize(acc), do: acc
end
