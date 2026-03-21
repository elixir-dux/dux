if Code.ensure_loaded?(Nx) do
  defimpl Nx.LazyContainer, for: Dux do
    @moduledoc """
    Implements `Nx.LazyContainer` for `%Dux{}`.

    Numeric columns become tensors on demand. Non-numeric columns are skipped.
    The result is a map of `%{column_name => tensor}`.
    """

    def traverse(%Dux{} = dux, acc, fun) do
      computed = Dux.compute(dux)
      {:table, ref} = computed.source
      columns = Dux.Native.table_to_columns(ref)
      dtypes = computed.dtypes

      {pairs, acc} =
        columns
        |> Enum.sort_by(fn {name, _} -> name end)
        |> Enum.flat_map_reduce(acc, &traverse_column(&1, &2, dtypes, fun))

      {Map.new(pairs), acc}
    end

    defp traverse_column({name, values}, acc, dtypes, fun) do
      case nx_type(Map.get(dtypes, name)) do
        nil ->
          {[], acc}

        nx_type ->
          template = Nx.template({length(values)}, nx_type)
          {result, acc} = fun.(template, fn -> Nx.tensor(values, type: nx_type) end, acc)
          {[{name, result}], acc}
      end
    end

    defp nx_type({:s, 8}), do: :s8
    defp nx_type({:s, 16}), do: :s16
    defp nx_type({:s, 32}), do: :s32
    defp nx_type({:s, 64}), do: :s64
    defp nx_type({:u, 8}), do: :u8
    defp nx_type({:u, 16}), do: :u16
    defp nx_type({:u, 32}), do: :u32
    defp nx_type({:u, 64}), do: :u64
    defp nx_type({:f, 32}), do: :f32
    defp nx_type({:f, 64}), do: :f64
    defp nx_type(:boolean), do: :u8
    defp nx_type({:decimal, _, _}), do: :f64
    defp nx_type(_), do: nil
  end
end
