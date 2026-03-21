if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: Dux do
    def init(%Dux{} = dux) do
      computed = Dux.compute(dux)
      {:table, ref} = computed.source
      columns = Dux.Native.table_names(ref)
      n_rows = Dux.Native.table_n_rows(ref)
      col_data = Dux.Native.table_to_columns(ref)
      data = Enum.map(columns, fn col -> Map.fetch!(col_data, col) end)
      {:columns, %{columns: columns, count: n_rows}, data}
    end
  end
end
