defmodule Dux.Remote do
  alias Dux.Remote.{Holder, LocalGC}

  @moduledoc """
  Distributed resource tracking for Dux.

  When a computed `%Dux{source: {:table, ref}}` is sent to another BEAM node,
  the underlying DuckDB temp table lives on the origin node. Without tracking,
  the Erlang GC on the origin would drop the NIF resource (and the temp table)
  while the remote node still needs it.

  `place/1` traverses a data structure, finds remote table references, and sets
  up a Holder process on the origin node to keep the resource alive. A GC sentinel
  NIF resource on the local node fires a cleanup notification when the local
  reference is garbage collected.

  ## How it works

  1. `place/1` detects `%Dux{source: {:table, %TableRef{node: origin}}}` where `origin != node()`
  2. Spawns a `Holder` on the origin node via `:erpc`
  3. Creates a GC sentinel NIF resource on the local node
  4. When the local `%Dux{}` is GC'd, the sentinel fires a message to LocalGC
  5. LocalGC forwards to the Holder on the origin node
  6. Holder releases the ResourceArc → DuxTableRef::Drop → DROP TABLE

  Lazy `%Dux{}` structs (no `{:table, ref}`) are pure data and don't need tracking.
  """

  @doc """
  Traverse a data structure and set up remote GC tracking for any
  remote table references found in `%Dux{}` structs.

  Returns the (possibly updated) term. Only modifies `%Dux{}` structs
  that have remote `{:table, ref}` sources.
  """
  def place(term) do
    {term, _holders} = do_place(term, [])
    term
  end

  # Computed Dux with a remote table ref — needs tracking
  defp do_place(%Dux{source: {:table, %Dux.TableRef{node: origin} = ref}, remote: nil} = dux, acc)
       when origin != node() do
    case setup_tracking(ref) do
      {:ok, sentinel, holder_pid} ->
        {%{dux | remote: {sentinel, holder_pid, ref}}, [holder_pid | acc]}

      :error ->
        # Can't reach origin node — leave as-is
        {dux, acc}
    end
  end

  # Already placed or local — skip
  defp do_place(%Dux{} = dux, acc), do: {dux, acc}

  # Recurse into lists
  defp do_place(list, acc) when is_list(list) do
    Enum.map_reduce(list, acc, &do_place/2)
  end

  # Recurse into tuples
  defp do_place(tuple, acc) when is_tuple(tuple) do
    {list, acc} =
      tuple
      |> Tuple.to_list()
      |> Enum.map_reduce(acc, &do_place/2)

    {List.to_tuple(list), acc}
  end

  # Recurse into maps (values only — keys are typically strings/atoms)
  defp do_place(%{__struct__: _} = struct, acc), do: {struct, acc}

  defp do_place(map, acc) when is_map(map) do
    {pairs, acc} =
      Enum.map_reduce(map, acc, fn {k, v}, acc ->
        {v, acc} = do_place(v, acc)
        {{k, v}, acc}
      end)

    {Map.new(pairs), acc}
  end

  # Scalars — pass through
  defp do_place(other, acc), do: {other, acc}

  # Set up Holder on origin node + GC sentinel on local node
  defp setup_tracking(%Dux.TableRef{node: origin_node} = table_ref) do
    local_gc_pid = LocalGC.pid()

    if is_nil(local_gc_pid) do
      :error
    else
      case :erpc.call(origin_node, Holder, :start_child, [
             table_ref,
             local_gc_pid
           ]) do
        {:ok, holder_pid} ->
          # TODO(Phase 4): Implement GC sentinel for ADBC.
          # For now, the holder stays alive until the connection closes.
          # The sentinel ref is just the table_ref itself.
          {:ok, table_ref, holder_pid}

        _ ->
          :error
      end
    end
  end
end
