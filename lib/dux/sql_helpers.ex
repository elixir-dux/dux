defmodule Dux.SQL.Helpers do
  @moduledoc false

  # Shared SQL helper functions used across QueryBuilder, Merger,
  # Worker, Broadcast, Shuffle, and Graph modules.

  @doc """
  Quote a SQL identifier by escaping internal double quotes.

  Prevents SQL injection via column/table names containing `"`.

      iex> Dux.SQL.Helpers.qi("normal_col")
      ~s("normal_col")

      iex> Dux.SQL.Helpers.qi(~s(col"injection))
      ~s("col""injection")
  """
  def qi(name) when is_atom(name), do: qi(to_string(name))

  def qi(name) when is_binary(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end

  @doc """
  Escape a SQL string value (single-quote doubling).
  """
  def escape_sql_string(s), do: String.replace(s, "'", "''")

  @doc """
  Keep NIF resource references alive across a block of code.

  Stores refs in the process dictionary (opaque to the BEAM optimizer)
  to prevent GC of DuxTableRef ResourceArcs between table_ensure and
  SQL execution.

  Returns the result of the function.
  """
  def with_refs(key, refs, fun) do
    Process.put(key, refs)

    try do
      fun.()
    after
      Process.delete(key)
    end
  end
end
