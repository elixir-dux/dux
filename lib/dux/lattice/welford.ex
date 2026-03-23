defmodule Dux.Lattice.Welford do
  @moduledoc false
  @behaviour Dux.Lattice

  # Welford's online algorithm for numerically stable variance/stddev.
  # State: {n, mean, m2} where m2 = Σ(x - mean)².
  #
  # Pairwise merge (Chan et al. 1979):
  #   n   = n_a + n_b
  #   δ   = mean_b - mean_a
  #   mean = mean_a + δ * n_b / n
  #   m2  = m2_a + m2_b + δ² * n_a * n_b / n
  #
  # This is exact — no catastrophic cancellation regardless of value
  # magnitude, unlike the (sum_x2 - sum_x²/n) formula.

  @impl true
  def bottom, do: {0, 0.0, 0.0}

  @impl true
  def merge({0, _, _}, b), do: b
  def merge(a, {0, _, _}), do: a

  def merge({n_a, mean_a, m2_a}, {n_b, mean_b, m2_b}) do
    n = n_a + n_b
    delta = mean_b - mean_a
    mean = mean_a + delta * n_b / n
    m2 = m2_a + m2_b + delta * delta * n_a * n_b / n
    {n, mean, m2}
  end

  @impl true
  def finalize({0, _, _}), do: nil
  def finalize({1, _, _}), do: nil
  def finalize({n, _mean, m2}), do: m2 / (n - 1)

  @doc "Finalize as population variance (divides by n instead of n-1)."
  def finalize_pop({0, _, _}), do: nil
  def finalize_pop({n, _mean, m2}), do: m2 / n

  @doc "Finalize as sample standard deviation."
  def finalize_stddev({n, mean, m2}) do
    case finalize({n, mean, m2}) do
      nil -> nil
      var -> :math.sqrt(var)
    end
  end

  @doc "Finalize as population standard deviation."
  def finalize_stddev_pop({n, mean, m2}) do
    case finalize_pop({n, mean, m2}) do
      nil -> nil
      var -> :math.sqrt(var)
    end
  end

  @doc """
  Ingest a single value into a Welford accumulator.

  Useful for building accumulators from raw data.
  """
  def ingest({n, mean, m2}, x) do
    n = n + 1
    delta = x - mean
    mean = mean + delta / n
    delta2 = x - mean
    m2 = m2 + delta * delta2
    {n, mean, m2}
  end
end
