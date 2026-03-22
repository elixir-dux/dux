defimpl Inspect, for: Dux do
  import Inspect.Algebra

  @preview_limit 5

  def inspect(%Dux{source: {:table, table_ref}} = dux, opts) do
    conn = Dux.Connection.get_conn()
    names = Dux.Backend.table_names(conn, table_ref)
    n_rows = Dux.Backend.table_n_rows(conn, table_ref)
    n_cols = length(names)

    columns = Dux.Backend.table_to_columns(conn, table_ref)
    dtypes = dux.dtypes

    # Header: DuckDB[rows x cols]
    header =
      concat([
        color("DuckDB", :atom, opts),
        color("[", :list, opts),
        string("#{n_rows} x #{n_cols}"),
        color("]", :list, opts)
      ])

    # Distribution line (if distributed)
    dist = distribution_doc(dux.workers, opts)

    # Column lines: name dtype [values]
    col_docs = Enum.map(names, &format_column(&1, columns, dtypes, opts))

    inner =
      [line(), header] ++
        if(dist, do: [line(), dist], else: []) ++
        [line() | Enum.intersperse(col_docs, line())]

    concat([
      color("#Dux<", :map, opts),
      nest(concat(inner), 2),
      line(),
      color(">", :map, opts)
    ])
    |> group()
  end

  def inspect(%Dux{source: source, ops: ops, workers: workers}, opts) do
    source_desc = describe_source(source)

    parts =
      [color("#Dux<", :map, opts), color("lazy ", :atom, opts)] ++
        if(ops != [], do: [color("[#{length(ops)} ops] ", :list, opts)], else: []) ++
        [string(source_desc)] ++
        if(workers,
          do: [color(" distributed(#{length(workers)} workers)", :atom, opts)],
          else: []
        ) ++
        [color(">", :map, opts)]

    concat(parts)
  end

  defp format_column(name, columns, dtypes, opts) do
    dtype_str = format_dtype(Map.get(dtypes, name))
    values = Map.get(columns, name, [])

    concat([
      color(name, :map, opts),
      string(" "),
      color(dtype_str, :atom, opts),
      string(" "),
      format_values(values, opts)
    ])
  end

  defp format_values(values, opts) when length(values) <= @preview_limit do
    inner = Enum.map(values, &value_doc(&1, opts)) |> Enum.intersperse(string(", "))

    concat([
      color("[", :list, opts),
      concat(inner),
      color("]", :list, opts)
    ])
  end

  defp format_values(values, opts) do
    shown = Enum.take(values, @preview_limit)
    inner = Enum.map(shown, &value_doc(&1, opts)) |> Enum.intersperse(string(", "))

    concat([
      color("[", :list, opts),
      concat(inner),
      string(", "),
      color("...", :list, opts),
      color("]", :list, opts)
    ])
  end

  defp value_doc(nil, opts), do: color("nil", :atom, opts)

  defp value_doc(v, opts) when is_binary(v) do
    color(~s("#{truncate(v, 20)}"), :string, opts)
  end

  defp value_doc(v, _opts) when is_float(v), do: string(Float.to_string(v))
  defp value_doc(v, _opts), do: string(Kernel.inspect(v))

  defp truncate(s, max) do
    if String.length(s) > max do
      String.slice(s, 0, max - 1) <> "…"
    else
      s
    end
  end

  defp format_dtype({:s, n}), do: "s#{n}"
  defp format_dtype({:u, n}), do: "u#{n}"
  defp format_dtype({:f, n}), do: "f#{n}"
  defp format_dtype(:boolean), do: "bool"
  defp format_dtype(:string), do: "string"
  defp format_dtype(:date), do: "date"
  defp format_dtype(:time), do: "time"
  defp format_dtype(:binary), do: "binary"
  defp format_dtype({:naive_datetime, _}), do: "naive_datetime"
  defp format_dtype({:datetime, _, _}), do: "datetime"
  defp format_dtype({:duration, _}), do: "duration"
  defp format_dtype({:decimal, p, s}), do: "decimal(#{p},#{s})"
  defp format_dtype(nil), do: "unknown"
  defp format_dtype(other), do: Kernel.inspect(other)

  defp distribution_doc(nil, _opts), do: nil
  defp distribution_doc([], _opts), do: nil

  defp distribution_doc(workers, opts) when is_list(workers) do
    nodes =
      workers
      |> Enum.flat_map(fn
        pid when is_pid(pid) -> [node(pid)]
        _ -> []
      end)
      |> Enum.uniq()
      |> Enum.sort()

    node_desc =
      case nodes do
        [] -> "#{length(workers)} workers"
        [n] when n == node() -> "local"
        [n] -> "#{n}"
        ns -> "#{length(ns)} nodes"
      end

    concat([
      color("distributed:", :atom, opts),
      string(" #{length(workers)} workers on #{node_desc}")
    ])
  end

  defp describe_source({:sql, sql}) do
    "sql: #{truncate(sql, 40)}"
  end

  defp describe_source({:csv, path, _}), do: "csv: #{Path.basename(path)}"
  defp describe_source({:parquet, path, _}), do: "parquet: #{Path.basename(path)}"
  defp describe_source({:ndjson, path, _}), do: "ndjson: #{Path.basename(path)}"
  defp describe_source({:list, rows}), do: "list: #{length(rows)} rows"
  defp describe_source({:table, _}), do: "table"
  defp describe_source(nil), do: "empty"
  defp describe_source(other), do: Kernel.inspect(other)
end
