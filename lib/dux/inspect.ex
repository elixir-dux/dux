defimpl Inspect, for: Dux do
  import Inspect.Algebra

  @preview_limit 5

  def inspect(%Dux{source: {:table, ref}} = dux, opts) do
    # Computed — show shape, columns, dtypes, first few values
    names = Dux.Native.table_names(ref)
    n_rows = Dux.Native.table_n_rows(ref)
    n_cols = length(names)

    columns = Dux.Native.table_to_columns(ref)
    dtypes = dux.dtypes

    header = "DuckDB[#{n_rows} x #{n_cols}]"
    dist_line = distribution_line(dux.workers)
    col_lines = Enum.map(names, &format_column(&1, columns, dtypes))

    lines =
      [line(), string(header)] ++
        if(dist_line, do: [line(), string(dist_line)], else: []) ++
        [line() | Enum.intersperse(col_lines, line())]

    concat([
      "#Dux<",
      nest(concat(lines), 2),
      line(),
      ">"
    ])
    |> group()
    |> format(opts.width)
    |> IO.iodata_to_binary()
    |> color(:map, opts)
  end

  def inspect(%Dux{source: source, ops: ops, workers: workers}, opts) do
    # Lazy — show source type and op count without materializing
    source_desc = describe_source(source)
    dist_tag = if workers, do: " distributed(#{length(workers)} workers)", else: ""

    if ops == [] do
      color("#Dux<lazy #{source_desc}#{dist_tag}>", :map, opts)
    else
      color("#Dux<lazy [#{length(ops)} ops] #{source_desc}#{dist_tag}>", :map, opts)
    end
  end

  defp format_column(name, columns, dtypes) do
    dtype = format_dtype(Map.get(dtypes, name))
    values = Map.get(columns, name, [])
    preview = format_values(values)
    string("#{name} #{dtype} #{preview}")
  end

  defp format_values(values) when length(values) <= @preview_limit do
    "[#{Enum.map_join(values, ", ", &format_value/1)}]"
  end

  defp format_values(values) do
    shown = Enum.take(values, @preview_limit)
    "[#{Enum.map_join(shown, ", ", &format_value/1)}, ...]"
  end

  defp format_value(nil), do: "nil"
  defp format_value(v) when is_binary(v), do: ~s("#{truncate(v, 20)}")
  defp format_value(v) when is_float(v), do: Float.to_string(v)
  defp format_value(v), do: inspect(v)

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

  defp distribution_line(nil), do: nil
  defp distribution_line([]), do: nil

  defp distribution_line(workers) when is_list(workers) do
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

    "distributed: #{length(workers)} workers on #{node_desc}"
  end

  defp describe_source({:sql, sql}) do
    truncated = truncate(sql, 40)
    "sql: #{truncated}"
  end

  defp describe_source({:csv, path, _}), do: "csv: #{Path.basename(path)}"
  defp describe_source({:parquet, path, _}), do: "parquet: #{Path.basename(path)}"
  defp describe_source({:ndjson, path, _}), do: "ndjson: #{Path.basename(path)}"
  defp describe_source({:list, rows}), do: "list: #{length(rows)} rows"
  defp describe_source({:table, _}), do: "table"
  defp describe_source(nil), do: "empty"
  defp describe_source(other), do: Kernel.inspect(other)
end
