if Code.ensure_loaded?(Nx) do
  defimpl Nx.LazyContainer, for: Dux do
    @moduledoc """
    Implements `Nx.LazyContainer` for `%Dux{}`.

    Numeric columns become tensors on demand via zero-copy from Arrow buffers.
    Non-numeric columns are skipped.
    The result is a map of `%{column_name => tensor}`.
    """

    def traverse(%Dux{} = dux, acc, fun) do
      computed = Dux.compute(dux)
      {:table, table_ref} = computed.source
      conn = Dux.Connection.get_conn()
      raw_columns = Dux.Backend.table_to_raw_columns(conn, table_ref)

      {pairs, acc} =
        raw_columns
        |> Enum.sort_by(fn {name, _} -> name end)
        |> Enum.flat_map_reduce(acc, &traverse_column(&1, &2, fun))

      {Map.new(pairs), acc}
    end

    defp traverse_column({name, %Adbc.Column{size: size} = col}, acc, fun) do
      case nx_type_for_adbc(col.field.type) do
        nil ->
          {[], acc}

        nx_type ->
          template = Nx.template({size}, nx_type)
          {result, acc} = fun.(template, fn -> Dux.column_to_tensor(col) end, acc)
          {[{name, result}], acc}
      end
    end

    defp nx_type_for_adbc(:s8), do: :s8
    defp nx_type_for_adbc(:s16), do: :s16
    defp nx_type_for_adbc(:s32), do: :s32
    defp nx_type_for_adbc(:s64), do: :s64
    defp nx_type_for_adbc(:u8), do: :u8
    defp nx_type_for_adbc(:u16), do: :u16
    defp nx_type_for_adbc(:u32), do: :u32
    defp nx_type_for_adbc(:u64), do: :u64
    defp nx_type_for_adbc(:f16), do: :f16
    defp nx_type_for_adbc(:f32), do: :f32
    defp nx_type_for_adbc(:f64), do: :f64
    defp nx_type_for_adbc(:date32), do: :s32
    defp nx_type_for_adbc(:date64), do: :s64
    defp nx_type_for_adbc({:time32, _}), do: :s32
    defp nx_type_for_adbc({:time64, _}), do: :s64
    defp nx_type_for_adbc({:timestamp, _, _}), do: :s64
    defp nx_type_for_adbc({:duration, _}), do: :s64
    defp nx_type_for_adbc({:interval, :month}), do: :s32

    defp nx_type_for_adbc({:dictionary, %Adbc.Field{type: key_type}, _}),
      do: nx_type_for_adbc(key_type)

    defp nx_type_for_adbc(_), do: nil
  end
end
