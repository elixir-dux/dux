defmodule Dux.Lattice.Min do
  @moduledoc false
  @behaviour Dux.Lattice

  @impl true
  def bottom, do: :infinity

  @impl true
  def merge(:infinity, b), do: b
  def merge(a, :infinity), do: a
  def merge(a, b), do: min(a, b)

  @impl true
  def finalize(:infinity), do: nil
  def finalize(acc), do: acc
end
