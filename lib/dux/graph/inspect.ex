defimpl Inspect, for: Dux.Graph do
  import Inspect.Algebra

  def inspect(%Dux.Graph{} = graph, opts) do
    n_vertices = safe_count(graph.vertices)
    n_edges = safe_count(graph.edges)

    density =
      if n_vertices > 1 do
        Float.round(n_edges / (n_vertices * (n_vertices - 1)), 4)
      else
        0.0
      end

    avg_degree =
      if n_vertices > 0 do
        Float.round(n_edges / n_vertices, 1)
      else
        0.0
      end

    lines = [
      "vertices: #{format_number(n_vertices)}",
      "edges: #{format_number(n_edges)}",
      "density: #{density}",
      "avg_degree: #{avg_degree}",
      ~s(vertex_id: "#{graph.vertex_id}"),
      ~s(edges: "#{graph.edge_src}" → "#{graph.edge_dst}")
    ]

    concat([
      "#Dux.Graph<",
      nest(
        concat([line() | Enum.intersperse(Enum.map(lines, &string/1), line())]),
        2
      ),
      line(),
      ">"
    ])
    |> group()
    |> format(opts.width)
    |> IO.iodata_to_binary()
    |> color(:map, opts)
  end

  defp safe_count(%Dux{source: {:table, ref}}) do
    Dux.Native.table_n_rows(ref)
  end

  defp safe_count(%Dux{} = dux) do
    Dux.n_rows(dux)
  rescue
    _ -> 0
  end

  defp format_number(n) when n >= 1_000_000 do
    "#{Float.round(n / 1_000_000, 1)}M"
  end

  defp format_number(n) when n >= 1_000 do
    "#{Float.round(n / 1_000, 1)}K"
  end

  defp format_number(n), do: "#{n}"
end
