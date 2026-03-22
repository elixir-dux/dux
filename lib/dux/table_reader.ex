if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: Dux do
    def init(%Dux{} = dux) do
      computed = Dux.compute(dux)
      {:table, table_ref} = computed.source
      conn = Dux.Connection.get_conn()
      columns = Dux.Backend.table_names(conn, table_ref)
      n_rows = Dux.Backend.table_n_rows(conn, table_ref)
      col_data = Dux.Backend.table_to_columns(conn, table_ref)
      data = Enum.map(columns, fn col -> Map.fetch!(col_data, col) end)
      {:columns, %{columns: columns, count: n_rows}, data}
    end
  end
end
