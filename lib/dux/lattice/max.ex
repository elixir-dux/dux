defmodule Dux.Lattice.Max do
  @moduledoc false
  @behaviour Dux.Lattice

  @impl true
  def bottom, do: :neg_infinity

  @impl true
  def merge(:neg_infinity, b), do: b
  def merge(a, :neg_infinity), do: a
  def merge(a, b), do: max(a, b)

  @impl true
  def finalize(:neg_infinity), do: nil
  def finalize(acc), do: acc
end
