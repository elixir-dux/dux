defmodule Dux.Lattice do
  @moduledoc false

  # A lattice defines a merge-compatible aggregate: an identity element (bottom),
  # a commutative + associative merge, and a finalization step.
  #
  # Used by the distributed merger to fold partial results from workers
  # incrementally rather than batching all IPC results for a single merge query.
  #
  # Each built-in lattice corresponds to a SQL aggregate function. The
  # PipelineSplitter classifies pipeline aggregates and emits a lattice_map
  # so the StreamingMerger knows how to fold each column.

  @doc "Identity element for the merge operation."
  @callback bottom() :: term()

  @doc "Merge two partial results. Must be commutative and associative."
  @callback merge(term(), term()) :: term()

  @doc "Convert accumulated state to the final result value."
  @callback finalize(term()) :: term()

  # Ordered rules — first match wins. APPROX_COUNT_DISTINCT must precede
  # COUNT_DISTINCT, and COUNT(DISTINCT must precede COUNT(.
  @classify_rules [
    {"SUM(", Dux.Lattice.Sum},
    {"APPROX_COUNT_DISTINCT(", Dux.Lattice.Hll},
    {"COUNT(DISTINCT", nil},
    {"COUNT_DISTINCT(", nil},
    {"COUNT(", Dux.Lattice.Count},
    {"MIN(", Dux.Lattice.Min},
    {"MAX(", Dux.Lattice.Max},
    {"AVG(", Dux.Lattice.Avg}
  ]

  @stddev_keywords ~w(STDDEV VARIANCE VAR_SAMP VAR_POP STDDEV_SAMP STDDEV_POP)

  @doc """
  Classify a SQL aggregate expression and return its lattice module, or nil.

  The expression is the uppercased SQL string from the summarise op.
  """
  def classify(upper_expr) when is_binary(upper_expr) do
    find_lattice(upper_expr, @classify_rules)
  end

  def classify(_), do: nil

  defp find_lattice(expr, [{pattern, lattice} | rest]) do
    if String.contains?(expr, pattern), do: lattice, else: find_lattice(expr, rest)
  end

  defp find_lattice(expr, []) do
    if Enum.any?(@stddev_keywords, &String.contains?(expr, &1)),
      do: Dux.Lattice.Welford,
      else: nil
  end
end
